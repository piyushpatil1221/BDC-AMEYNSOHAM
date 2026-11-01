"""Dynamic INT8 quantization of the FP32 ONNX encoder (runs at image build time).

Dynamic quantization stores weights as INT8 and quantizes activations on the fly per batch,
so no calibration dataset is needed. This is the CPU equivalent of the report's
"quantized BERT on ONNX Runtime" (report Section 4.3).

Only MatMul weights are quantized (per output channel). The word-embedding lookup (Gather)
stays FP32: quantizing it saves little time but costs noticeable embedding fidelity.
"""
import sys
from pathlib import Path

from onnxruntime.quantization import QuantType, quantize_dynamic

src, dst = Path(sys.argv[1]), Path(sys.argv[2])
quantize_dynamic(
    model_input=src,
    model_output=dst,
    weight_type=QuantType.QInt8,
    op_types_to_quantize=["MatMul"],
    per_channel=True,
)
print(f"FP32 {src.stat().st_size / 1e6:.1f} MB -> INT8 {dst.stat().st_size / 1e6:.1f} MB")
