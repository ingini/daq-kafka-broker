FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# confluent-kafka ships manylinux wheels with bundled librdkafka for x86_64;
# gcc/libc6-dev kept as fallback for arches without prebuilt wheels.
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc libc6-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY mapper.py ./

RUN useradd --uid 1000 --no-create-home --home-dir /app --shell /usr/sbin/nologin app \
    && chown -R app:app /app
USER app

EXPOSE 2123

ENTRYPOINT ["python", "-u", "mapper.py"]
