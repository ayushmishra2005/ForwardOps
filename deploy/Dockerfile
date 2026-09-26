FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    FORWARDOPS_ROOT=/app

WORKDIR /app

RUN useradd --create-home --uid 10001 forwardops

COPY pyproject.toml README.md LICENSE ./
COPY src ./src
COPY migrations ./migrations
COPY examples ./examples

RUN pip install --no-cache-dir .

USER forwardops

EXPOSE 8000

CMD ["forwardops-api"]
