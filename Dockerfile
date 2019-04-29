FROM python:3.7-alpine

WORKDIR /etl

ARG ENVIRONMENT=prod

COPY requirements_prod.txt requirements_prod.txt
COPY requirements_dev.txt requirements_dev.txt
RUN pip install -r requirements_${ENVIRONMENT}.txt

COPY etl /etl
COPY tests /tests

ENV PYTHONPATH "${PYTHONPATH}:/etl"
ENTRYPOINT python -u /etl/run.py
