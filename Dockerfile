FROM python:3.10 AS compile-image

RUN python -m venv /opt/venv
# Make sure we use the virtualenv:
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt


FROM python:3.10-alpine AS runner

WORKDIR /opt

COPY --from=compile-image /opt/venv /opt/venv

# Make sure we use the virtualenv:
ENV PATH="/opt/venv/bin:$PATH"

COPY ena_download.py ./ena_download.py
ENV PATH="$(pwd)/ena_download.py:$PATH"

FROM runner AS test
COPY tests tests
COPY requirements-test.txt requirements-test.txt
RUN pip install -r requirements-test.txt
