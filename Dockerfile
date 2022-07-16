FROM python:3-alpine

ADD requirements.txt .
RUN python -m pip install -r requirements.txt

COPY static static
COPY main.py main.py
