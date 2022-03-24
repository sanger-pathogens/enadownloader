FROM python:3.10 AS compile-image

RUN python -m venv /opt/venv
# Make sure we use the virtualenv:
ENV PATH="/opt/venv/bin:$PATH"

# TODO Starting to wonder if we should just clone the repo with git
COPY requirements.txt requirements.txt
COPY pyproject.toml pyproject.toml
COPY setup.cfg setup.cfg
COPY src src
RUN pip install -r requirements.txt .


FROM python:3.10-alpine AS runner

WORKDIR /opt

COPY --from=compile-image /opt/venv ./venv

# Make sure we use the virtualenv:
ENV PATH="/opt/venv/bin:$PATH"

COPY src .
COPY enadownloader.sh .
RUN chmod +x enadownloader.sh
COPY sra_ids_test.txt .

ENV PATH="/opt/:$PATH"

#FROM runner AS test
#COPY tests tests
#COPY requirements-test.txt requirements-test.txt
#RUN pip install -r requirements-test.txt
