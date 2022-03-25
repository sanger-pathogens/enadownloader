FROM python:3.10 AS compile-image

RUN python -m venv venv
# Make sure we use the virtualenv:
ENV PATH="$PWD/venv/bin:$PATH"

COPY requirements.txt ./
RUN pip install -r requirements.txt


FROM python:3.10-alpine AS runner

WORKDIR /opt

COPY --from=compile-image venv venv

# Make sure we use the virtualenv:
ENV PATH="$PWD/venv/bin:$PATH"

COPY setup.cfg pyproject.toml ./
COPY src src
RUN pip install .


FROM runner AS test

COPY tests tests
COPY requirements-test.txt requirements-test.txt
RUN pip install -r requirements-test.txt -e .

COPY sra_ids_test.txt .
