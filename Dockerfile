FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

COPY pyproject.toml README.md ./
COPY settle ./settle
COPY sim ./sim
COPY deploy ./deploy
RUN pip install --no-cache-dir .

# Every service in docker-compose.yml runs this image with its own command:
#   python -m settle.mockpsp | settle.shard | settle.gateway
CMD ["python", "-m", "settle.gateway"]
