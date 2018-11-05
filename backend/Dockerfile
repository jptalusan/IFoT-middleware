#worker docker file
FROM arm32v7/debian:stretch

EXPOSE 5002

RUN mkdir -p /usr/scr/app
WORKDIR /usr/src/app

RUN apt-get -y update
RUN apt-get -y upgrade

RUN apt-get -y install build-essential libssl-dev libffi-dev

RUN apt-get -y install python3-dev
RUN apt-get -y install python3-pip

RUN apt-get -y install python3-lxml
RUN apt-get -y install python3-scipy
RUN apt-get -y install python3-bs4
RUN apt-get -y install python3-pil

ADD ./requirements.txt /usr/src/app/requirements.txt
RUN pip3 install -r requirements.txt

ADD ./requirements-np.txt /usr/src/app/requirements-np.txt
RUN pip3 install -r requirements-np.txt
