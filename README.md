# gz2zip

[![CI](https://github.com/45deg/gz2zip/actions/workflows/ci.yaml/badge.svg)](https://github.com/45deg/gz2zip/actions/workflows/ci.yaml)
[![PyPI version](https://img.shields.io/pypi/v/gz2zip.svg)](https://pypi.org/project/gz2zip/)
[![Python versions](https://img.shields.io/pypi/pyversions/gz2zip.svg)](https://pypi.org/project/gz2zip/)

`gz2zip` converts **GZIP to ZIP/ZIP64 without recompression** by reusing the original Deflate payload.

This is much faster than decompressing and re-zipping, and it supports both:
1. **seekable file conversion** (`gzip_to_zip`)
2. **streaming conversion** (`stream_gzip_to_zip`) for pipes and async workloads

## Benchmark

```
Benchmark: 100 MiB text, 5 iterations
Platform: macOS 26.0.1 (arm64)
CPU: Apple M1 Pro
Memory: 32.0 GiB
Python: 3.12.0
Input: 100.00 MiB text -> 30.49 MiB gzip

gz2zip.core.gzip_to_zip  mean 9.73 ms  median 9.69 ms  min 9.32 ms  max 10.48 ms
python gzip + zipfile    mean 3316.88 ms  median 3313.52 ms  min 3299.16 ms  max 3335.05 ms
shell gzip -dc | zip     mean 3390.16 ms  median 3385.02 ms  min 3361.35 ms  max 3435.71 ms
```

See `benchmarks/speed_experiment.py` for the benchmark script.

## Installation

```bash
pip install gz2zip
```

Install as an isolated CLI tool with `pipx`:

```bash
pipx install gz2zip
```

or with `uv`:

```bash
uv tool install gz2zip
```

Run without installing globally:

```bash
pipx run gz2zip --help
uvx gz2zip --help
```

## Supported Python versions

Based on the official Python version support table, this package targets:

- Python 3.10
- Python 3.11
- Python 3.12
- Python 3.13
- Python 3.14

## CLI Usage

The package installs the `gz2zip` command:

```bash
gz2zip INPUT_GZ [-o OUTPUT_ZIP] [-n NAME_IN_ZIP] [-s UNCOMPRESSED_SIZE] [-t ISO_TIMESTAMP] [-q]
```

### Common examples

Convert a file and auto-derive output name (`input.gz` -> `input.zip`):

```bash
gz2zip input.gz
```

Set output path and internal filename:

```bash
gz2zip input.gz -o output.zip -n data.csv
```

Pipe from stdin to stdout:

```bash
cat input.gz | gz2zip - -n data.csv > output.zip
```

Large-file case (>4 GiB original content): provide exact uncompressed size:

```bash
gz2zip huge.sql.gz -o huge.zip -n huge.sql -s 5368709120
```

Run as a module:

```bash
python -m gz2zip input.gz -o output.zip
```

### CLI options

| Option | Description |
|---|---|
| `input` | Input `.gz` file. Use `-` (or omit) to read from stdin. |
| `-o, --output` | Output ZIP file. Use `-` for stdout. |
| `-n, --name` | Filename to store inside ZIP. |
| `-s, --size` | Known uncompressed size (recommended for >4 GiB input). |
| `-t, --timestamp` | Override timestamp in ISO 8601, e.g. `2025-12-31T23:59:58`. |
| `-q, --quiet` | Suppress non-error logs. |

## API Usage

### Synchronous (seekable files)

```python
from gz2zip import gzip_to_zip

with open("input.gz", "rb") as f_in, open("output.zip", "wb") as f_out:
    gzip_to_zip(f_in, f_out, filename_in_zip="data.csv")
```

### Asynchronous streaming

```python
import asyncio
from gz2zip import stream_gzip_to_zip

async def gzip_chunks():
    with open("input.gz", "rb") as f:
        while chunk := f.read(64 * 1024):
            yield chunk

async def main():
    with open("output.zip", "wb") as out:
        async for chunk in stream_gzip_to_zip(gzip_chunks(), "data.csv"):
            out.write(chunk)

asyncio.run(main())
```

## ZIP64 note

GZIP stores `ISIZE` modulo `2^32`, so files larger than 4 GiB need an explicit
`known_uncompressed_size` (`-s` in CLI) if exact size metadata is required.

## Limitation: concatenated GZIP members

Concatenated GZIP streams are valid (for example: `cat a.gz b.gz > joined.gz`),
but this project currently does **not** support that multi-member format
correctly. Use single-member `.gz` input files.

## Development

Run tests:

```bash
uv run pytest
```
