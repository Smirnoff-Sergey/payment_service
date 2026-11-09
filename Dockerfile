FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /srv

COPY pyproject.toml ./
COPY app ./app
RUN pip install ".[dev]"

COPY alembic.ini ./
COPY migrations ./migrations
COPY tests ./tests

RUN useradd --create-home --uid 1000 appuser
USER appuser

CMD ["uvicorn", "app.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
