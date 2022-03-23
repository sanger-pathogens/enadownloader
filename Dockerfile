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

COPY *.py ./
RUN chmod +x ./enadownloader.py

ENV PATH="/opt:$PATH"

#FROM runner AS test
#COPY tests tests
#COPY requirements-test.txt requirements-test.txt
#RUN pip install -r requirements-test.txt
