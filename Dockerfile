FROM quay.io/cdis/python:3.8-bookworm

RUN pip install --upgrade pip poetry

WORKDIR /opt
COPY poetry.lock /opt/
COPY pyproject.toml /opt/
COPY vadc_gwas_tools/__main__.py /opt/vadc_gwas_tools/__main__.py
RUN python -m venv /env \
    && . /env/bin/activate \
    && poetry install --only main --no-interaction
COPY . /opt/

ENTRYPOINT ["/env/bin/vadc-gwas-tools"]
