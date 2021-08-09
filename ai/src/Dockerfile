FROM python:3.8

WORKDIR /app

ENV HOME=/app/home
ENV SPICE_ENVIRONMENT=demo

COPY requirements.txt .
RUN python3 -m pip install -r requirements.txt

COPY *.py ./
COPY connectors/*.py ./connectors/

RUN mkdir -p /app/home/.spice/tmp/img

ENTRYPOINT ["python3", "-u", "train.py"]
