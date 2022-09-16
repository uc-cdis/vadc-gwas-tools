FROM quay.io/cdis/python:3.8-buster

COPY . /opt/
WORKDIR /opt
RUN python -m venv /env \
    && . /env/bin/activate \
    && pip install --upgrade pip poetry \
    && poetry install --no-dev --no-interaction

ENTRYPOINT ["/env/bin/vadc-gwas-tools"]
