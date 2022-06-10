__author__ = "Mikhail Novikov"
__email__ = "novikov.ms.in12@gmail.com"
__status__ = "Production"
__license__ = "MIT"


# параметры соединения
default_source = "/home/api_user/SmartVideoStreemPython/vlc_streamer/media/8.mp4"
default_host = '0.0.0.0'
default_streaming_port_pool = list(range(8554, 8601, 1))
default_control_port_pool = list(range(18554, 18601, 1))

default_streaming_port = 8554
default_control_port = 18554

# параметры видео перекодирования
default_vcodec = 'mp4v'   # 'h264'
# default_vb = '80000'
default_acodec = 'flac'  # 'mpga'
default_ab = '192'
default_channels = 2
default_samplerate = 44100
default_mux = 'ogg'

# параметры кеширования
default_mux_caching = 5000
default_network_caching = 3000


# https://wiki.videolan.org/VLC_command-line_help/
# --start-paused Запустит с паузой стрим, но придется переделать индексы по коду...
def run_options(source: str = default_source,
                host: str = default_host,
                streaming_port: str = default_streaming_port,
                control_port: str = default_control_port):
    """
    возвращает список параметров для запуска cvlc
    !!! Важно, внутри не менять порядок элементов. На них индекс ссылаеются в db.py
    :param source: ссылка на файл /media/8.mp4 или url на файл
    :param host: str
    :param streaming_port: str
    :param control_port: str
    :return:
    """
    options = [
        "vlc",
        "-I",
        "dummy",
        "--extraintf", "rc",
        "--rc-host", f"{host}:{control_port}",
        "--no-video",
        "--no-audio",
        '--input-repeat=1000',  # "--noloop", # "--loop"
        f"{source}",
        # "--sout=#transcode{" + f"vcodec={default_vcodec}, acodec={default_acodec}, ab={default_ab}, channels={default_channels}, samplerate={default_samplerate}" + "}:rtp{proto=rtp,caching=5000, mux=ts, sdp=rtsp://" + f"{host}:{streaming_port}" + "/live.stream}",
        "--sout=#transcode{" + f"vcodec={default_vcodec}, acodec={default_acodec}, ab={default_ab}, channels={default_channels}, samplerate={default_samplerate}" + "}:http{"+f"mux={default_mux}, dst=" + f"{default_host}:{default_streaming_port}/stream.ts"+"}",
        '--sout-mux-caching', f'{default_mux_caching}',
        '--network-caching', f'{default_network_caching}',
    ]

    return options

