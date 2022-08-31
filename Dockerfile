FROM python:3.10 AS compile-image

ARG WORK="/opt"
WORKDIR "$WORK"

RUN python -m venv venv
# Make sure we use the virtualenv:
ENV PATH="$WORK/venv/bin:$PATH"

COPY setup.cfg pyproject.toml ./
COPY src src

# Setting in-tree-build as pip started complaining about a potential new feature
RUN pip install .

FROM compile-image AS test

COPY tests tests

RUN pip install -e ".[test]"

# Loose default for quick testing
CMD ["pytest", "--cov", "src", "--cov-branch", "--cov-report", "term-missing", "--cov-fail-under", "80"]

FROM python:3.10-alpine AS runner

ARG WORK="/opt"
WORKDIR "$WORK"

COPY --from=compile-image $WORK/venv venv
# Make sure we use the virtualenv
ENV PATH="$WORK/venv/bin:$PATH"
