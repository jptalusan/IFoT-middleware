#worker docker file
#FROM raspberrypi3-05:5000/backend:latest
FROM linusmotu/rpi-jupyter:datascience1.3

EXPOSE 5002

RUN mkdir -p /usr/scr/app
WORKDIR /usr/src/app

ADD ./requirements.txt /usr/src/app/requirements.txt

RUN pip install -r requirements.txt
