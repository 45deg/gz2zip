from __future__ import annotations

import gzip
import sys
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
