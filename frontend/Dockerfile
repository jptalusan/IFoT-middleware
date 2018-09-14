#front end dockerfile
FROM resin/raspberrypi3-python:3.6-slim
#FROM raspberrypi3-05:5000/frontend:latest

EXPOSE 5001

# set working directory
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

# add requirements (to leverage Docker cache)
ADD ./requirements.txt /usr/src/app/requirements.txt

# install requirements
RUN pip install -r requirements.txt
