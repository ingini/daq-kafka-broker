FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends libgl1 libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# ultralytics pulls torch/onnx transitively; pin onnx for a stable opset.
RUN pip install --no-cache-dir \
        "ultralytics>=8.2.0" \
        "onnx>=1.16.0" \
        "onnxslim>=0.1.32"

COPY export.sh /usr/local/bin/export.sh
RUN chmod +x /usr/local/bin/export.sh

WORKDIR /work
ENTRYPOINT ["/usr/local/bin/export.sh"]
