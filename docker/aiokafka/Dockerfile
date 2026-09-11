FROM python:3.12

WORKDIR /opt/project

COPY setup.py pyproject.toml requirements-* /opt/project/

RUN pip install -r requirements-ci.txt

COPY aiokafka /opt/project/aiokafka
COPY tests /opt/project/tests
