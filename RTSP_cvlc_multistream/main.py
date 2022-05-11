__author__ = "Mikhail Novikov"
__email__ = "novikov.ms.in12@gmail.com"
__status__ = "Production"
__license__ = "GPLv3"

from fastapi import FastAPI
from fastapi import HTTPException
from api.db import *

from vlc_streamer.options import *

# fastapi и временная база
db = DataBase()
db.set_port_pool(default_streaming_port_pool, default_control_port_pool)

tags_metadata = [
    {
        "name": "prod",
        "description": "Работает со стримом",
    },
    {
        "name": "test",
        "description": "Тестируем элементы Апи",
    },
]

app = FastAPI(title="VLC player server API", openapi_tags=tags_metadata)


@app.get(
    path="/get_uid",
    response_description="Get id",
    description="Get text link by id from database",
    response_model=PhraseOutput,
    tags=["test"]
)
async def get(uid: int = 1):
    try:
        phrase = db.get(uid=uid)
    except IndexError:
        raise HTTPException(404, "No such ID in Phrase list")
    return phrase


@app.get(
    path="/get_port_pool",
    response_description="Get port pool",
    description="Get port pool from database",
    response_model=PortPool,
    tags=["test"]
)
async def get_port_pool():
    try:
        phrase = db.get_port_pool()
    except IndexError:
        raise HTTPException(404, "No such ID in Phrase list")
    return phrase


@app.get(
    path="/get_busy_ports",
    response_description="Get busy ports",
    description="Check busy ports on host from pool",
    response_model=PortPoolStatus,
    tags=["test"]
)
async def get_busy_ports(host: str = '0.0.0.0'):
    try:
        phrase = db.get_busy_port_pool(host=host)
    except IndexError:
        raise HTTPException(404, "All ports are busy")
    return phrase


@app.get(
    path="/get_free_ports",
    response_description="Get id",
    description="Get text link by id from database",
    response_model=PortsPair,
    tags=["test"]
)
async def get_free_ports(host: str = '0.0.0.0'):
    try:
        phrase = db.get_free_ports(host=host)
    except IndexError:
        raise HTTPException(404, "No such ID in Phrase list")
    return phrase


@app.get(
    path="/set_port_pool",
    response_description="List all id in current session",
    description="Get id list in current session",
    response_model=PortPool,
    tags=["test"],
)
async def set_port_pool(streaming_port_list: list = default_streaming_port_pool,
                        control_port_list: list = default_control_port_pool):
    try:
        phrase = db.set_port_pool(streaming_port_list=streaming_port_list,
                                  control_port_list=control_port_list)
    except Exception:
        raise HTTPException(404, "Can't access to Database")
    return phrase


@app.get(
    path="/list_streams",
    response_description="List all id in current session",
    description="Get id list in current session",
    response_model=ListOutput,
    tags=["test"],
)
async def list_streams():
    try:
        phrase = db.list_streams()
    except Exception:
        raise HTTPException(404, "Can't access to Database")
    return phrase


@app.post(
    path="/add",
    response_description="Added phrase with *id* parameter",
    response_model=PhraseOutput,
    tags=["prod"],
)
async def add(phrase: PhraseInput):
    phrase_out = db.add(phrase=phrase)
    return phrase_out


@app.post(
    path="/run_stream",
    response_description="run player with id on host",
    response_model=PhraseOutput,
    tags=["prod"],
)
async def run(host: str = '0.0.0.0', uid: int = 1):
    phrase = db.run(host=host, uid=uid)
    return phrase


@app.post(
    path="/play",
    response_description="play button on player with id on host",
    tags=["prod"],
)
async def play(uid: int = 1):
    try:
        db.play(uid)
    except ValueError as e:
        raise HTTPException(404, str(e))


@app.post(
    path="/pause",
    response_description="play button on player with id on host",
    tags=["prod"],
)
async def pause(uid: int = 1):
    try:
        db.pause(uid)
    except ValueError as e:
        raise HTTPException(404, str(e))


@app.post(
    path="/stop",
    response_description="sets a play position to player with id on host",
    tags=["prod"],
)
async def stop(uid: int = 1):
    try:
        db.stop(uid)
    except ValueError as e:
        raise HTTPException(404, str(e))


@app.post(
    path="/seek",
    response_description="stop button on player with id on host",
    tags=["prod"],
)
async def seek(uid: int = 1, time_sec: int = 1):
    try:
        db.seek(uid, str(time_sec))
    except ValueError as e:
        raise HTTPException(404, str(e))


@app.post(
    path="/kill",
    response_description="Kill python process tree with player",
    tags=["prod"],
)
async def _kill(uid: int = 1):
    try:
        db.quit(uid)
    except ValueError as e:
        raise HTTPException(404, str(e))


@app.post(
    path="/change_status",
    response_description="Change *id* status \n \
                        Статус id. \n  \
                        0 - не обработанно \n  \
                        1 - был запущен \n  \
                        2 - ошибка чтения файла \n \
                        3 - ошибка обработки",
    response_model=PhraseOutput,
    tags=['test'],
)
async def change_status(uid: int = 1, status: int = 1):
    try:
        phrase = db.change_status(uid, status, reply=True)
    except IndexError:
        raise HTTPException(404, "No such ID in Phrase list")
    return phrase


@app.post(
    path="/change_connection_props",
    response_description="Сhange host, streaming port, control port \n"
                         "in Video_run_options",
    response_model=PhraseOutput,
    tags=['test'],
)
async def change_connection_props(uid: int,
                                  host: str,
                                  streaming_port: str,
                                  control_port: str
                                  ):
    try:
        phrase = db.change_connection_props(uid=uid,
                                            host=host,
                                            streaming_port=streaming_port,
                                            control_port=control_port,
                                            reply=True)
    except IndexError:
        raise HTTPException(404, "No such ID in Phrase list")
    return phrase


@app.delete(
    path="/delete",
    response_description="Result of deleting",
    tags=['test'],
)
async def delete(uid: int = 1):
    try:
        db.delete(uid)
    except ValueError as e:
        raise HTTPException(404, str(e))


# DEBUG ========================
# phrase = PhraseInput(IDEntity='asd',
#     Video_run_options= run_options(),
#     Alias= None,  # Описание добавляемой сущности, по умолчанию None
#     Tag=0)
#
# phrase.Video_run_options[10]="/home/kit-kat/PycharmProjects/ProgressTerra/smartvideostreempython/vlc_streamer/media/8.mp4"
# phrase.Video_run_options[6]='0.0.0.0:18554'
# db.get_port_pool()
# db.add(phrase)
# db.add(phrase)
# db.add(phrase)
# db.set_port_pool(default_streaming_port_pool, default_control_port_pool)
#
# phrase = db.get(uid=2)
# db.run(host=default_host, uid=2)
# db.run(host=default_host, uid=1)
#
# db.pause(uid=2)
# phrase = db.get(uid=2)
# db.quit(uid=2)
# db.kill(uid=2)
# #
# print('finished')

