FROM quay.io/cdis/python:3.8-buster

RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python

COPY . /opt/
WORKDIR /opt
RUN python -m venv /env && . /env/bin/activate && pip install --upgrade pip && $HOME/.poetry/bin/poetry install --no-dev --no-interaction

ENTRYPOINT ["/env/bin/vadc-gwas-tools"]
