FROM 593291632749.dkr.ecr.eu-west-1.amazonaws.com/python:3.7-slim

WORKDIR /etl

ARG ENVIRONMENT=prod

RUN apt-get -qq update &&\
    apt-get -y install git

COPY requirements_prod.txt requirements_prod.txt
COPY requirements_dev.txt requirements_dev.txt
RUN pip install -r requirements_${ENVIRONMENT}.txt

RUN apt-get -y remove --purge git

COPY etl/ etl/
COPY tests/ tests/
COPY run.py run.py

ENTRYPOINT python -u run.py
