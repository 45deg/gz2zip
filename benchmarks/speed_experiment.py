#!/usr/bin/env python3
"""
The benchmark generates a text corpus, compresses it to GZIP once, and then
compares three conversion paths:

- gz2zip.core.gzip_to_zip
- a normal Python gzip -> zipfile recompression path
- a shell pipeline: gzip -dc | zip
"""

from __future__ import annotations

import argparse
import gzip
import platform
import random
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
import zipfile
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from pathlib import Path

from gz2zip import gzip_to_zip


MIB = 1024 * 1024
COPY_BUFFER_SIZE = MIB
TEXT_WORDS = (
    "alpha",
    "bravo",
    "charlie",
    "delta",
    "echo",
    "foxtrot",
    "golf",
    "hotel",
    "india",
    "juliet",
    "kilo",
    "lima",
    "mike",
    "november",
    "oscar",
    "papa",
    "quebec",
    "romeo",
    "sierra",
    "tango",
    "uniform",
    "victor",
    "whiskey",
    "xray",
    "yankee",
    "zulu",
)

BenchRunner = Callable[[Path], None]


@dataclass(frozen=True)
class BenchmarkMethod:
    label: str
    output_tag: str
    run: BenchRunner


@dataclass(frozen=True)
class TimingSummary:
    mean: float
    median: float
    minimum: float
    maximum: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark gz2zip against Python and shell alternatives."
    )
    parser.add_argument("--size-mib", type=int, default=100, help="Size of the generated text corpus in MiB.")
    parser.add_argument("--iterations", type=int, default=5, help="Number of timed runs for each method.")
    return parser.parse_args()


def format_bytes(num_bytes: int) -> str:
    return f"{num_bytes / MIB:.2f} MiB"


def format_duration(value: float) -> str:
    return f"{value * 1000:.2f} ms"


def command_output(command: list[str]) -> str | None:
    try:
        return subprocess.check_output(command, text=True).strip()
    except (subprocess.SubprocessError, OSError):
        return None


def get_cpu_name() -> str:
    if sys.platform == "darwin":
        brand = command_output(["sysctl", "-n", "machdep.cpu.brand_string"])
        if brand:
            return brand
    return platform.processor() or platform.machine() or "unknown CPU"


def get_platform_name() -> str:
    if sys.platform == "darwin":
        version = command_output(["sw_vers", "-productVersion"])
        if version:
            return f"macOS {version}"
        return "macOS"
    return platform.system() or "unknown"


def get_memory_size() -> str:
    if sys.platform == "darwin":
        bytes_raw = command_output(["sysctl", "-n", "hw.memsize"])
        if not bytes_raw:
            return "unknown"
        try:
            bytes_value = int(bytes_raw)
            return f"{bytes_value / (1024 ** 3):.1f} GiB"
        except ValueError:
            return "unknown"
    return "unknown"


def write_text_corpus(path: Path, size_bytes: int) -> None:
    rng = random.Random(42)
    remaining = size_bytes
    index = 0

    with path.open("wb") as file_obj:
        while remaining > 0:
            line_words = rng.sample(TEXT_WORDS, 7)
            line = f"{index:09d} {' '.join(line_words)} {rng.getrandbits(64):016x}\n".encode("ascii")
            chunk = line if remaining >= len(line) else line[:remaining]
            file_obj.write(chunk)
            remaining -= len(chunk)
            index += 1


def gzip_corpus(source_path: Path, gzip_path: Path) -> None:
    with source_path.open("rb") as source, gzip.open(gzip_path, "wb", compresslevel=6) as target:
        shutil.copyfileobj(source, target, length=COPY_BUFFER_SIZE)


def bench_gz2zip(gzip_path: Path, arcname: str, output_path: Path) -> None:
    with gzip_path.open("rb") as source, output_path.open("wb") as target:
        gzip_to_zip(source, target, filename_in_zip=arcname)


def bench_python_zip(gzip_path: Path, arcname: str, output_path: Path) -> None:
    with gzip.open(gzip_path, "rb") as source, zipfile.ZipFile(
        output_path,
        "w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=6,
    ) as archive:
        with archive.open(arcname, "w") as target:
            shutil.copyfileobj(source, target, length=COPY_BUFFER_SIZE)


def bench_shell_zip(gzip_path: Path, output_path: Path) -> None:
    subprocess.run(
        [
            "/bin/sh",
            "-c",
            'gzip -dc "$1" | zip -q -6 "$2" -',
            "speed_experiment",
            str(gzip_path),
            str(output_path),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def to_output_tag(label: str) -> str:
    normalized = "".join(char if char.isalnum() else " " for char in label)
    return "-".join(normalized.lower().split())


def build_methods(gzip_path: Path, arcname: str) -> list[BenchmarkMethod]:
    specs: list[tuple[str, BenchRunner]] = [
        (
            "gz2zip.core.gzip_to_zip",
            partial(bench_gz2zip, gzip_path, arcname),
        ),
        (
            "python gzip + zipfile",
            partial(bench_python_zip, gzip_path, arcname),
        ),
        (
            "shell gzip -dc | zip",
            partial(bench_shell_zip, gzip_path),
        ),
    ]
    return [
        BenchmarkMethod(label=label, output_tag=to_output_tag(label), run=runner)
        for label, runner in specs
    ]


def measure(action: BenchRunner, output_path: Path) -> float:
    start = time.perf_counter()
    action(output_path)
    return time.perf_counter() - start


def summarize(samples: list[float]) -> TimingSummary:
    return TimingSummary(
        mean=statistics.mean(samples),
        median=statistics.median(samples),
        minimum=min(samples),
        maximum=max(samples),
    )


def run_method(method: BenchmarkMethod, iterations: int, temp_dir: Path) -> TimingSummary:
    samples: list[float] = []
    for iteration in range(iterations):
        output_path = temp_dir / f"{iteration:02d}-{method.output_tag}.zip"
        samples.append(measure(method.run, output_path))
    return summarize(samples)


def print_header(size_mib: int, iterations: int, input_size: int, gzip_size: int) -> None:
    print(f"Benchmark: {size_mib} MiB text, {iterations} iterations")
    print(f"Platform: {get_platform_name()} ({platform.machine()})")
    print(f"CPU: {get_cpu_name()}")
    print(f"Memory: {get_memory_size()}")
    print(f"Python: {platform.python_version()}")
    print(f"Input: {format_bytes(input_size)} text -> {format_bytes(gzip_size)} gzip")
    print()


def validate_args(args: argparse.Namespace) -> None:
    if args.size_mib <= 0:
        raise SystemExit("--size-mib must be greater than zero")
    if args.iterations <= 0:
        raise SystemExit("--iterations must be greater than zero")


def main() -> None:
    args = parse_args()
    validate_args(args)

    size_bytes = args.size_mib * MIB

    with tempfile.TemporaryDirectory(prefix="gz2zip-bench-") as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        text_path = temp_dir / "input.txt"
        gzip_path = temp_dir / "input.txt.gz"

        write_text_corpus(text_path, size_bytes)
        gzip_corpus(text_path, gzip_path)

        input_size = text_path.stat().st_size
        gzip_size = gzip_path.stat().st_size
        arcname = "benchmark.txt"

        methods = build_methods(gzip_path, arcname)
        print_header(args.size_mib, args.iterations, input_size, gzip_size)

        for method in methods:
            summary = run_method(method, args.iterations, temp_dir)
            print(
                f"{method.label:<24} mean {format_duration(summary.mean)}  "
                f"median {format_duration(summary.median)}  "
                f"min {format_duration(summary.minimum)}  max {format_duration(summary.maximum)}"
            )


if __name__ == "__main__":
    main()
