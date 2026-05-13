#!/usr/bin/env bash
# Convert an Ultralytics YOLO .pt checkpoint to an ONNX file that Triton
# can serve. Idempotent: if the output already exists and is newer than
# the input, the export is skipped.
set -euo pipefail

SRC="${SRC:-/source/yolo26n.pt}"
DEST_DIR="${DEST_DIR:-/models/yolo26n/1}"
IMGSZ="${IMGSZ:-640}"
OPSET="${OPSET:-12}"
DEST="$DEST_DIR/model.onnx"

if [[ ! -f "$SRC" ]]; then
    echo "[exporter] source not found: $SRC" >&2
    exit 1
fi

mkdir -p "$DEST_DIR"

if [[ -f "$DEST" && "$DEST" -nt "$SRC" ]]; then
    echo "[exporter] $DEST is newer than $SRC — skipping export"
    exit 0
fi

WORK=$(mktemp -d)
cp "$SRC" "$WORK/"
SRC_NAME=$(basename "$SRC")

echo "[exporter] running: yolo export model=$SRC_NAME format=onnx imgsz=$IMGSZ opset=$OPSET dynamic=false"
(
    cd "$WORK"
    yolo export model="$SRC_NAME" format=onnx imgsz="$IMGSZ" opset="$OPSET" dynamic=false simplify=true
)

EXPORTED="${WORK}/$(basename "${SRC%.pt}").onnx"
if [[ ! -f "$EXPORTED" ]]; then
    echo "[exporter] expected ${EXPORTED} not produced" >&2
    ls -la "$WORK" >&2
    exit 2
fi

mv "$EXPORTED" "$DEST"
echo "[exporter] wrote $DEST ($(stat -c %s "$DEST") bytes)"

# Log the actual ONNX I/O shapes so the operator can sanity-check
# config.pbtxt against the trained number of classes.
python3 - <<PY
import onnx, json, sys
m = onnx.load("$DEST")
def shape(t):
    return [d.dim_value or d.dim_param or "?" for d in t.type.tensor_type.shape.dim]
inputs = {t.name: shape(t) for t in m.graph.input}
outputs = {t.name: shape(t) for t in m.graph.output}
print("[exporter] onnx inputs:", json.dumps(inputs))
print("[exporter] onnx outputs:", json.dumps(outputs))
PY
