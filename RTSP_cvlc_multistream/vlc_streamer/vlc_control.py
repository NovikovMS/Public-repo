__author__ = "Mikhail Novikov"
__email__ = "novikov.ms.in12@gmail.com"
__status__ = "Production"
__license__ = "GPLv3"

import socket

from threading import Thread


class PlayerRC:
    def __init__(self, address: str, port: str):
        """
        Врапер по управлению cvlc через TCP сокет
        :param address: str
        :param port: str
        """
        self.is_initiated = False
        self.address = address
        try:
            self.port = int(port)
        except:
            self.port = port

        self.is_initiated = self._get_stream_status()
        print(f"stream is_playing: {self.is_initiated}")

    def _get_stream_status(self):
        """
        Проверяет ведется ли стрим через сокет
        :return: bool
        """
        try:
            status = self._threded_req_2way('is_playing')[1]
            if status == '1':
                return True
            else:
                return False
        except Exception as e:
            return None

    def _test_play(self, path: str):
        """
        запускает пробный стрим файлика
        :param path: str
        """
        self._threded_req_1way("loop on")
        self._threded_req_1way("random on")
        self._threded_req_1way(f"add {path}")  # adding the music folder
        # print("Init Playing")
        self._threded_req_1way("play")
        # print("Toggle play")
        pass

    def _play(self):
        """
        кнопка Play
        """
        if self.is_initiated:
            self._threded_req_1way("play")
            # print("Toggle play")
            pass

    def _pause(self):
        """
        кнопка Pause
        """
        if self.is_initiated:
            self._threded_req_1way("pause")
            # print("Toggle pause")
            pass

    def _stop(self):
        """
        кнопка Stop
        """
        if self.is_initiated:
            self._threded_req_1way("stop")
            # print("Toggle stop")
            pass

    def _next(self):
        """
        кнопка Next from playlist
        """
        if self.is_initiated:
            self._threded_req_1way("next")
            # print("Next")
            pass

    def _prev(self):
        """
        кнопка Previous from playlist
        """
        if self.is_initiated:
            self._threded_req_1way("prev")
            # print("Previous")
            pass

    def _log_out(self):
        """
        Отпускает RC сокет
        """
        if self.is_initiated:
            self._threded_req_1way("logout")
            # print("logout")
            pass

    def _quit(self):
        """
        Должен убивать, но не убивает плеер
        """
        if self.is_initiated:
            self._threded_req_1way("quit")
            # print("quit")
            pass

    def _volume(self, vol: int):
        """
        устанваливаем уровень звука стрима
        :param vol:
        """
        if self.is_initiated:
            self._threded_req_1way("volume " + str(vol))
            # print("Volume up")
            pass

    def _seek(self, time_sec: str):
        """
        устанваливаем позицию проигрывания стрима в целых секундах!!!
        милисекунды увы не принимает.
        дробные секунды принимает в виде строки "12.650", но это не точно...
        на 12 секунду перемотает, а вот дальше хз.
        :param time_sec: str
        :return:
        """
        if self.is_initiated:
            self._threded_req_1way("seek " + str(time_sec))
            # print(f"Seek: {time_sec} sec")
            pass

    def _get_length(self):
        """
        Возвращает длину проигрываемого файла в секундах
        :return: str
        """
        if self.is_initiated:
            reply = self._threded_req_2way("get_length ")[1]
            # print(f"get_length: {reply} sec")
            return reply

    def _get_time(self):
        """
        Возвращает позицию проигрываемого файла в секундах
        :return: str
        """
        if self.is_initiated:
            reply = self._threded_req_2way("get_time ")[1]
            # print(f"get_time: {reply} sec")
            return reply

    def _req(self, msg: str, full=False):
        """
        Подключается к открытому сокету плеера, отправляет команду
        :param msg: str команда отправляемая в vlc
        :param full: bool проверяет полный ответ
        :return: str ответа
        """
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                # Connect to server and send data
                sock.settimeout(0.7)
                sock.connect((self.address, self.port))
                response = ""
                received = ""
                sock.sendall(bytes(msg + '\n', "utf-8"))
                # if True:
                try:
                    while (True):
                        received = (sock.recv(1024)).decode()
                        response = response + received
                        if full:
                            b = response.count("\r\n")
                            if response.count("\r\n") > 1:
                                sock.close()
                                break
                        else:
                            if response.count("\r\n") > 0:
                                sock.close()
                                break
                except Exception as e:
                    response = response + received
                    pass
                sock.close()
                return response
        except Exception as e:
            return None
            pass

    def _threded_req_1way(self, msg):
        """
        отправляет команду, не возвращает ответ
        :param msg: str команда отправляемая в vlc
        """
        Thread(target=self._req, args=(msg,)).start()

    def _threded_req_2way(self, msg):
        """
        отправляет команду, возвращает ответ
        :param msg: str команда отправляемая в vlc
        :return: list ответа
        """
        response = str(self._req(msg, True)).split("\r\n")
        if len(response) < 2:
            return None
        response = response[1].split(" ")
        if len(response) < 2:
            return None
        return response

