"""Check CLI guards, atomic output, special-file preservation, and stdout pipes."""

from __future__ import annotations

import gzip
import io
import os
import stat
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

from gz2zip import cli


def _write_gzip(path: Path, data: bytes) -> None:
    with gzip.open(path, "wb") as gz:
        gz.write(data)


def test_cli_rejects_negative_size(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    input_path = tmp_path / "input.gz"
    output_path = tmp_path / "output.zip"
    _write_gzip(input_path, b"payload")

    monkeypatch.setattr(
        sys,
        "argv",
        ["gz2zip", str(input_path), "-o", str(output_path), "--size", "-1"],
    )

    with pytest.raises(SystemExit) as excinfo:
        cli.main()

    assert excinfo.value.code == 1
    assert "non-negative integer" in capsys.readouterr().err
    assert not output_path.exists()


def test_cli_rejects_same_input_and_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    input_path = tmp_path / "same.gz"
    _write_gzip(input_path, b"same-file")
    original = input_path.read_bytes()

    monkeypatch.setattr(
        sys,
        "argv",
        ["gz2zip", str(input_path), "-o", str(input_path)],
    )

    with pytest.raises(SystemExit) as excinfo:
        cli.main()

    assert excinfo.value.code == 1
    assert "Input and output paths must be different." in capsys.readouterr().err
    assert input_path.read_bytes() == original


@pytest.mark.parametrize("failure", ["missing_input", "invalid_input", "partial_write", "create", "replace"])
def test_cli_preserves_existing_output_on_failure(tmp_path, monkeypatch, failure):
    input_path = tmp_path / "input.gz"
    output_path = tmp_path / "output.zip"
    original = b"existing output must survive"
    output_path.write_bytes(original)
    if failure != "missing_input":
        _write_gzip(input_path, b"payload")
    if failure == "invalid_input":
        input_path.write_bytes(b"invalid gzip")

    def fail_after_write(*, f_out, **kwargs):
        f_out.write(b"partial ZIP")
        raise OSError("conversion failed")

    def deny_operation(*args, **kwargs):
        raise PermissionError("permission denied")

    if failure == "partial_write":
        monkeypatch.setattr(cli.core, "gzip_to_zip", fail_after_write)
    elif failure == "create":
        monkeypatch.setattr(cli.tempfile, "NamedTemporaryFile", deny_operation)
    elif failure == "replace":
        monkeypatch.setattr(Path, "replace", deny_operation)

    monkeypatch.setattr(sys, "argv", ["gz2zip", str(input_path), "-o", str(output_path)])
    with pytest.raises(SystemExit) as excinfo:
        cli.main()

    assert excinfo.value.code == 1
    assert output_path.read_bytes() == original
    assert not list(tmp_path.glob(".gz2zip-*"))


@pytest.mark.parametrize("existing_output", [False, True])
def test_cli_publishes_complete_output(tmp_path, monkeypatch, existing_output):
    input_path = tmp_path / "input.gz"
    output_path = tmp_path / "output.zip"
    data = b"complete archive payload"
    _write_gzip(input_path, data)
    if existing_output:
        output_path.write_bytes(b"old archive")
    monkeypatch.setattr(sys, "argv", ["gz2zip", str(input_path), "-o", str(output_path)])

    cli.main()

    with zipfile.ZipFile(output_path) as archive:
        assert archive.read("input") == data
        assert archive.testzip() is None
    assert not list(tmp_path.glob(".gz2zip-*"))


@pytest.mark.parametrize("use_stdin", [False, True])
def test_cli_writes_valid_zip_to_pipe(tmp_path, use_stdin):
    input_path = tmp_path / "input.gz"
    data = b"pipe payload" * 1000
    _write_gzip(input_path, data)
    result = subprocess.run(
        [sys.executable, "-m", "gz2zip", "-" if use_stdin else str(input_path),
         "-o", "-", "-n", "data.txt"],
        input=input_path.read_bytes() if use_stdin else None,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr.decode()
    with zipfile.ZipFile(io.BytesIO(result.stdout)) as archive:
        assert archive.read("data.txt") == data
        assert archive.testzip() is None


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="Requires POSIX FIFOs")
@pytest.mark.parametrize("use_stdin", [False, True])
@pytest.mark.parametrize("use_symlink", [False, True])
def test_cli_rejects_fifo_without_replacing_it(tmp_path, use_stdin, use_symlink):
    input_path = tmp_path / "input.gz"
    _write_gzip(input_path, b"payload")
    fifo_path = tmp_path / "output.fifo"
    os.mkfifo(fifo_path)
    original = fifo_path.stat()
    output_path = fifo_path
    if use_symlink:
        output_path = tmp_path / "output.link"
        output_path.symlink_to(fifo_path)

    result = subprocess.run(
        [sys.executable, "-m", "gz2zip", "-" if use_stdin else str(input_path),
         "-o", str(output_path)],
        input=input_path.read_bytes() if use_stdin else None,
        capture_output=True,
        timeout=5,
    )

    assert result.returncode == 1
    assert b"Output path is not a regular file" in result.stderr
    assert stat.S_ISFIFO(fifo_path.stat().st_mode)
    assert fifo_path.stat().st_ino == original.st_ino
    if use_symlink:
        assert output_path.is_symlink()
    assert not list(tmp_path.glob(".gz2zip-*"))


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="Requires POSIX FIFOs")
def test_output_preserves_fifo_created_during_conversion(tmp_path):
    output_path = tmp_path / "output.zip"
    with pytest.raises(ValueError, match="not a regular file"):
        with cli._open_output(output_path) as output:
            output.write(b"pending archive")
            os.mkfifo(output_path)

    assert stat.S_ISFIFO(output_path.stat().st_mode)
    assert not list(tmp_path.glob(".gz2zip-*"))
