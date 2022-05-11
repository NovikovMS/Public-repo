FROM ubuntu:20.04
ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# создаем пользователя не рута
RUN useradd -ms /bin/bash  api_user
COPY . /home/api_user/SmartVideoStreemPython
RUN chown -R api_user /home/api_user/SmartVideoStreemPython
WORKDIR /home/api_user/SmartVideoStreemPython

# ставим пакеты убунты
RUN apt-get update --fix-missing
RUN apt-get install -y software-properties-common
RUN apt-get install -y --reinstall keyboard-configuration

RUN apt-get install -y python3.9
RUN apt-get install -y python3-pip
RUN apt-get install -y build-essential
RUN apt-get install -y git
RUN apt-get install -y curl
RUN apt-get install -y mc 

RUN apt-get install -y vlc
# Это заглушка для запуска vlc под рутом
RUN apt-get install -y vlc-plugin-access-extra

# X11
RUN apt-get install -y xorg

# Ставим кодеки
RUN echo ttf-mscorefonts-installer msttcorefonts/accepted-mscorefonts-eula select true | debconf-set-selections
RUN apt-get install -y ttf-mscorefonts-installer 
RUN apt-get install -y ubuntu-restricted-extras

# Ставим пакеты для питона
RUN pip3 install --no-cache-dir --upgrade -r requirements.txt


USER api_user
WORKDIR /home/api_user/rtsp_cvlc_multistream
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5089"]
