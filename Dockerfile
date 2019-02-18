FROM python:3.7

WORKDIR /etl

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY etl /etl

ENTRYPOINT python -u /etl/run.py
