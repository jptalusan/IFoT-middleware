#front end dockerfile
FROM python:3.6
#FROM resin/raspberrypi3-python:3.6
#FROM raspberrypi3-05:5000/frontend:latest

EXPOSE 5001

# set working directory
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

# add requirements (to leverage Docker cache)
ADD ./requirements.txt /usr/src/app/requirements.txt
ADD ./requirements-np.txt /usr/src/app/requirements-np.txt

# install requirements
RUN pip install -r requirements.txt
RUN pip install -r requirements-np.txt
