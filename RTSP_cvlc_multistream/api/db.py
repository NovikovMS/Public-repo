__author__ = "Mikhail Novikov"
__email__ = "novikov.ms.in12@gmail.com"
__status__ = "Production"
__license__ = "MIT"

import os.path
import typing
from vlc_streamer.sockets import chk_port

from pydantic import BaseModel
from pydantic import Field
from vlc_streamer.options import run_options
from vlc_streamer.vlc_control import PlayerRC

import subprocess
import multiprocessing

import validators


def extract_host_port(address: str):
    _host, _port = address.split('\\')[0].split(':')
    return _host, _port


def replace_sout_sdp(sout: str, host: str, port: str):
    """
    в строке типа '--sout=#transcode{vcodec=h264, acodec=mpga, ab=192, channels=2, samplerate=44100}:rtp{proto=rtp,caching=5000, mux=ts, sdp=rtsp://0.0.0.0:8554}/live.stream}'
    Выделяет host, port, заменяем и собирает обратно
    :param sout:  str
    :param host:  str
    :param port:  str
    :return:
    """
    phrase = sout.split('dst=')
    dst = phrase[1].split("}")[0]
    protocol = 'http'
    path = dst.split('/', maxsplit=1)
    if len(path) > 1:
        path = f'/{path[1]}'
    else:
        path = ''
    stream_mrl = f"{protocol}://{host}:{port}" + f"{path}"
    phrase_out = f"{phrase[0]} dst={host}:{port}" + f"{path}" + "}"
    return phrase_out, stream_mrl


def sub_process(command):
    p = subprocess.Popen(command,
                         shell=False,
                         stdin=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         start_new_session=True)


class PhraseInput(BaseModel):
    """Шаблон фразы при получении новой записи"""
    IDEntity: str = Field(..., title="IDEntity", description="Сущность к которой принадлежат фалы", max_length=200)
    Video_run_options: typing.Optional[list] = run_options()
    Alias: typing.Optional[str] = None  # Описание добавляемой сущности, по умолчанию None
    Tag: typing.Optional[int] = 0  # Тэг для удобной группировки, по умолчанию 0


class PhraseOutput(PhraseInput):
    """Шаблон фразы для ответа по ID или добавления нового ID"""
    uid: typing.Optional[int] = None  # ID фразы в нашей временной базе данных.
    status: typing.Optional[int] = None  # начальный статус ID в нашей временной базе данных.
    vlc_pid: typing.Optional[int] = None # vlc pid процесса в контейнере
    ffmpeg_pid: typing.Optional[int] = None # ffmpeg pid процесса в контейнере
    stream_mrl: typing.Optional[str] = None


class ChangeInput(PhraseOutput):
    """Шаблон фразы для замены в ID параметра (статуса нпример)"""


class ListOutput(BaseModel):
    """Шаблон для вывода истории за сессию"""
    session: typing.Dict[int, PhraseOutput]  # список всех обработанных ID за сессию


class LinkOutput(BaseModel):
    """Шаблон для вывода истории за сессию"""
    uploaded_link: typing.Optional[str] = None  #


class ControlPort(BaseModel):
    control_port: typing.Optional[int] = 18554


class StreamingPort(BaseModel):
    streaming_port: typing.Optional[int] = 8554


class PortsPair(BaseModel):
    """Шаблон для вывода пары портов"""
    pair: typing.List[int]


class PortPool(BaseModel):
    """Шаблон для вывода списка портов плеера"""
    port_pool: typing.List[list]


class PortPoolStatus(BaseModel):
    """Шаблон для вывода словаря портов"""
    port_pool_status: typing.Dict[int, list] = {}


class DataBase:
    """
    Наша **fake** database на время работы сессии.
    Существует только для дебага...
    """

    def __init__(self):
        self.streams: typing.Dict[int, PhraseOutput] = {}  # пустой
        self.ports_pool = [[8554, 18554]]  # [(8554, 18554),]
        self.pool = []

    def set_port_pool(self, streaming_port_list: list, control_port_list: list):
        """
        Обновляем список пар портов.
        Длина списков должна быть одинаковой!
        :param streaming_port_list: по умолчанию [8554,]
        :param control_port_list: по умолчанию [18554,]
        :return:
        """
        if len(streaming_port_list) == len(control_port_list):
            self.ports_pool = list(zip(streaming_port_list, control_port_list))
            phrase_out = self.ports_pool
        else:
            phrase_out = self.ports_pool
        return phrase_out

    def get_port_pool(self) -> PortPool:
        """
        Озвучиваю список пар портов [(streaming, control),]
        """
        phrase_out = PortPool(port_pool=self.ports_pool)
        return phrase_out

    def get_busy_port_pool(self, host) -> PortPoolStatus:
        """
        Получение списка занятых портов, перебираю список пар портов
        :param host: 0.0.0.0 - порт локальной машины.
        :return:
        """
        i = 0
        port_pool_status = {}
        for pair in self.ports_pool:
            sp, cp = pair
            chk_sp = chk_port(host=host, port=sp)
            chk_cp = chk_port(host=host, port=sp)
            # проверяю свободен ли порт на host, если не свободен - то занят...
            if not chk_sp and not chk_cp:
                port_pool_status[i] = pair
                i += 1
        phrase_out = PortPoolStatus(port_pool_status=port_pool_status)
        return phrase_out

    def get_free_ports(self, host) -> typing.Optional[PortsPair]:
        """
        т.к. у меня нет callback на смерть плеера,
        я не могу вести статистику по портам,
        каждый раз перебеираю с начала пула потров и ищу пару свободных
        """
        sp_out, cp_out = (None, None)
        for pair in self.ports_pool:
            sp, cp = pair
            chk_sp = chk_port(host=host, port=sp)
            chk_cp = chk_port(host=host, port=cp)
            # проверяю свободен ли порт на host
            if chk_sp is True: # and chk_cp is True:  # cp порт остается висеть. Сложно отследить процесс.
                sp_out, cp_out = sp, cp
                break
        phrase_out = PortsPair(pair=(sp_out, cp_out))
        return phrase_out

    def list_streams(self) -> typing.Optional[ListOutput]:
        """
        Получение списка всех id и стримов за сессию
        """
        phrase_out = ListOutput(session=self.streams.items())
        return phrase_out

    def get(self, uid: int) -> typing.Optional[PhraseOutput]:
        """
        Получение записи по ID
        :param uid: номер записи int
        :return:
        """
        return self.streams.get(uid)

    def change_status(self, uid: int, status: int, reply: bool = True) -> PhraseOutput:
        """
        Меняем по ID статус записи
        :param uid: номер записи int
        :param status: кодовый int
        :param reply: bool, чек на возвращение ответа
        :return:
        """
        # Меняем по ID статус записи
        phrase = self.streams.get(uid)
        phrase.status = status
        self.streams[phrase.uid] = phrase
        if reply:
            return self.streams.get(uid)


    def change_pid(self, uid: int, pid: int, reply: bool = True) -> PhraseOutput:
        """
        Меняем по ID pid записи
        :param uid: номер записи int
        :param pid: int выданный системой
        :param reply: bool, чек на возвращение ответа
        :return:
        """
        # Меняем по ID статус записи
        phrase = self.streams.get(uid)
        phrase.vlc_pid = pid
        self.streams[phrase.uid] = phrase
        if reply:
            return self.streams.get(uid)

    def change_mrl(self, uid: int, mrl: str, reply: bool = True) -> PhraseOutput:
        """
        Меняем по ID mrl записи
        :param uid: номер записи int
        :param mrl: str ссылка на стрим
        :param reply: bool, чек на возвращение ответа
        :return:
        """
        # Меняем по ID статус записи
        phrase = self.streams.get(uid)
        phrase.stream_mrl = mrl
        self.streams[phrase.uid] = phrase
        if reply:
            return self.streams.get(uid)

    def change_connection_props(self, uid: int, host: str, streaming_port: str, control_port: str, reply: bool = True) -> PhraseOutput:
        """
        Меняем по ID порты в Video_run_options записи
        :param uid: номер записи int
        :param host: host замены str
        :param streaming_port: порт замены str
        :param control_port: порт замены str
        :param reply: bool, чек на возвращение ответа
        :return:
        """
        # Меняем по ID статус записи
        phrase = self.streams.get(uid)
        phrase.Video_run_options[6] = f"{host}:{control_port}"
        phrase.Video_run_options[11], phrase.stream_mrl = replace_sout_sdp(sout=phrase.Video_run_options[11],
                                                                            host=host,
                                                                            port=streaming_port)
        self.streams[phrase.uid] = phrase
        if reply:
            phrase_out = self.streams.get(uid)
            return phrase_out

    def add(self, phrase: PhraseInput) -> PhraseOutput:
        """
        Добавление записи
        :param phrase: Шаблон фразы для запуска стрима. {'host':, 'port':} При запуске выдадутся новые.
        :return:
        """

        status = 0
        # Статус uid.
        # 0 - не обработанно,
        # 1 - был запущен
        # 2 - ошибка чтения файла, стрим не запущен
        # 3 - ошибка прочее

        uid = len(self.streams) + 1
        pid = None
        phrase_out = PhraseOutput(uid=uid, status=status, pid=pid, **phrase.dict())
        self.streams[phrase_out.uid] = phrase_out
        phrase_out = self.get(uid=phrase_out.uid)
        return phrase_out

    def run(self, host: str, uid: int) -> PhraseOutput:
        """
        Зпускает наш стрим в своем пуле, и отпускает задачу.
        Плеер контролируем только через сокет
        :param host:
        :param uid:
        :return:
        """
        mrl_chk = False
        mrl = self.get(uid).Video_run_options[10]
        if mrl:
            if str(mrl).startswith('http'):
                mrl_chk = validators.url(mrl)
            if str(mrl).startswith('/') or str(mrl).startswith('./') or str(mrl).startswith('~/'):
                mrl_chk = os.path.isfile(mrl)

        if mrl_chk:
            try:
                # command = self.get(uid=uid).Video_run_options
                ports = self.get_free_ports(host)
                streaming_port, control_port = ports.pair
                phrase = self.change_connection_props(uid=uid,
                                                       host=host,
                                                       streaming_port=streaming_port,
                                                       control_port=control_port,
                                                       reply=True)
                command = phrase.Video_run_options

                proc = multiprocessing.Process(target=sub_process(command))
                self.pool.append(proc)
                proc.start()
                pid = proc.pid

                phrase = self.change_pid(uid=uid, pid=pid, reply=True)
                phrase = self.change_status(uid=uid, status=1, reply=True)
            except Exception as e:
                phrase = self.change_status(uid=uid, status=3, reply=True)
        else:
            phrase = self.change_status(uid=uid, status=2, reply=True)

        return phrase

    def delete(self, uid: int) -> typing.Union[typing.NoReturn, type(None)]:
        """
        Удаление записи по uid
        :param uid: int порядковый, номер, присвоенный при добавлении.
        :return:
        """
        if uid in self.streams:
            del self.streams[uid]
        else:
            raise ValueError("ID doesn't exist")

    def pause(self, uid: int) -> typing.Union[typing.NoReturn, type(None)]:
        """
        Зпускает передает на сокет в нашей базе команду паузы.
        :param uid:
        :return:
        """
        try:
            phrase = self.get(uid=uid).Video_run_options[6]
            host, port = extract_host_port(phrase)
            player = PlayerRC(address=host, port=port)
            player._pause()
            player._log_out()
        except Exception as e:
            raise ValueError("ID doesn't exist")

    def stop(self, uid: int) -> typing.Union[typing.NoReturn, type(None)]:
        """
        Зпускает передает на сокет в нашей базе команду stop.
        :param uid:
        :return:
        """
        try:
            phrase = self.get(uid=uid).Video_run_options[6]
            host, port = extract_host_port(phrase)
            player = PlayerRC(address=host, port=port)
            player._stop()
            player._log_out()
        except Exception as e:
            raise ValueError("ID doesn't exist")

    def play(self, uid: int) -> typing.Union[typing.NoReturn, type(None)]:
        """
        Зпускает передает на сокет в нашей базе команду play.
        :param uid:
        :return:
        """
        try:
            phrase = self.get(uid=uid).Video_run_options[6]
            host, port = extract_host_port(phrase)
            player = PlayerRC(address=host, port=port)
            player._play()
            player._log_out()
        except Exception as e:
            raise ValueError("ID doesn't exist")

    def quit(self, uid: int) -> typing.Union[typing.NoReturn, type(None)]:
        """
        Должен убиваеть плеер, убивает....
        :param uid:
        :return:
        """
        try:
            phrase = self.get(uid=uid).Video_run_options[6]
            host, port = extract_host_port(phrase)
            player = PlayerRC(address=host, port=port)
            player._quit()
        except Exception as e:
            raise ValueError("ID doesn't exist")

    def seek(self, uid: int = '0.0.0.0', time_sec: str = '1') -> typing.Union[typing.NoReturn, type(None)]:
        """
        Зпускает передает на сокет в нашей базе команду play.
        :param uid:
        :return:
        """
        try:
            phrase = self.get(uid=uid).Video_run_options[6]
            host, port = extract_host_port(phrase)
            player = PlayerRC(address=host, port=port)
            player._seek(time_sec)
        except Exception as e:
            raise ValueError("ID doesn't exist")

    def _kill(self, uid: int) -> typing.Union[typing.NoReturn, type(None)]:
        """
        Убивает дерево процессов с плеером.
        :param uid:
        :return:
        """
        try:
            gpid = self.get(uid=uid).vlc_pid
            subprocess.Popen(f'pkill -TERM -P {gpid}')
        except Exception as e:
            raise ValueError("ID doesn't exist")


#  Debug ===============

# db = DataBase()
# run_options_loc = run_options()
# run_options_loc[10]="/home/kit-kat/PycharmProjects/ProgressTerra/smartvideostreempython/vlc_streamer/media/8.mp4"
# chk_file = os.path.isfile(run_options_loc[9])
# phrase = PhraseInput(IDEntity='asd',
#     Video_run_options= run_options_loc,
#     Alias= None,  # Описание добавляемой сущности, по умолчанию None
#     Tag=0)
#
# host = '0.0.0.0'
# uid = 1
# db.add(phrase)
# db.add(phrase)
# db.add(phrase)
# db.set_port_pool(list(range(8554, 8602, 1)), list(range(18554, 18602, 1)))
# free_ports = db.get_free_ports(host)
# db.run(host, 1)
# db.run(host, 2)
# busy_ports = db.get_busy_port_pool(host)
# test_out=db.get(2)
# db.quit(1)
# db.quit(2)
# db._kill(1)
# print('=================')
