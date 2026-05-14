FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# ffmpeg needed for the FfmpegDecoder backend.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg gcc libc6-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py ./
COPY transcoder ./transcoder
COPY config.yml ./

RUN useradd --uid 1000 --no-create-home --home-dir /app --shell /usr/sbin/nologin app \
    && chown -R app:app /app
USER app

EXPOSE 2114
ENTRYPOINT ["python", "-u", "main.py"]
CMD ["--config", "/app/config.yml"]
