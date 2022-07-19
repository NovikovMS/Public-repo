# python runner, no dind
# Image based on ubuntu
FROM gitlab/gitlab-runner:latest

# Install dependencies
RUN apt-get --quiet --yes update
RUN apt-get --quiet --yes install  software-properties-common
RUN add-apt-repository ppa:deadsnakes/ppa --yes
RUN apt install apt-transport-https ca-certificates curl software-properties-common --yes

RUN apt-get --quiet --yes install \
        curl \
        wget \
        mc \
        tzdata\
        git \
        openssh-client \
        python3.9 \
        python3.9-dev \
        python3-pip \
        build-essential \
        libssl-dev \
        libffi-dev \
        python3-venv && \
    rm -rf /var/lib/apt/lists

# Make python 3.9 as default python
RUN rm -f /usr/bin/python && ln -s /usr/bin/python3.9 /usr/bin/python

# Install poetry
RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python3 -
