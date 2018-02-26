FROM registry.opensource.zalan.do/stups/python:latest

RUN apt-get update && apt-get install -y \
    python3-dev \
    libffi-dev \
    libssl-dev \
    libpq-dev \
    git

COPY . /agent

WORKDIR /agent

RUN pip3 install -U setuptools -e git+https://github.com/zalando-zmon/opentracing-utils.git#egg=opentracing_utils

RUN python setup.py install

CMD ["zmon-agent"]
