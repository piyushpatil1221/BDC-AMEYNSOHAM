"""Download MIND-small (train + dev) TSV files.

The official Azure blob (mind201910small.blob.core.windows.net) now answers HTTP 409
(public access disabled), so the raw TSVs come from a Hugging Face mirror pinned to an exact
commit. Each file's size is checked against the mirror listing and its SHA-256 is printed so
the exact data used can be recorded in the report. MIND is released under the Microsoft
Research License Terms (non-commercial research use).
"""
import hashlib
import sys
import urllib.request
from pathlib import Path

from replay import config

FILES = {
    "train/news.tsv": 41_202_121,
    "train/behaviors.tsv": 92_019_716,
    "dev/news.tsv": 33_519_092,
    "dev/behaviors.tsv": 42_838_544,
}


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> None:
    cfg = config.load()["data"]
    out_dir = Path(cfg["raw_dir"])
    for rel, size in FILES.items():
        dest = out_dir / rel
        if dest.exists() and dest.stat().st_size == size:
            print(f"ok (cached)  {rel}")
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(".part")
        print(f"downloading  {rel} ({size / 1e6:.1f} MB)", flush=True)
        urllib.request.urlretrieve(f"{cfg['mirror']}/{rel}", tmp)
        if tmp.stat().st_size != size:
            sys.exit(f"size mismatch for {rel}: got {tmp.stat().st_size}, expected {size}")
        tmp.replace(dest)
    for rel in FILES:
        print(f"sha256 {sha256(out_dir / rel)}  {rel}")


if __name__ == "__main__":
    main()
