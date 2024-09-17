FROM quay.io/cdis/amazonlinux:2023

RUN dnf install -y g++ python3-devel zlib-devel python3-pip python3-setuptools bzip2-devel xz-devel
RUN pip install --upgrade poetry

WORKDIR /opt
COPY poetry.lock /opt/
COPY pyproject.toml /opt/
COPY vadc_gwas_tools/__main__.py /opt/vadc_gwas_tools/__main__.py
RUN python3 -m venv /env \
    && . /env/bin/activate \
    && python3 -m pip install --no-binary :all: pysam \
    && poetry install --only main --no-interaction
COPY . /opt/

ENTRYPOINT ["/env/bin/vadc-gwas-tools"]
