"""
APPNOTE-driven test suite for gz2zip.core.

This suite validates conversion integrity plus ZIP structure details against
APPNOTE.TXT sections around:
- 4.3.7 Local file header
- 4.3.9 Data descriptor
- 4.3.12 Central directory header
- 4.3.14/4.3.15 ZIP64 EOCD + locator
- 4.3.16 End of central directory record
- 4.4.4 bit 3 (data descriptor) and bit 11 (UTF-8 / EFS)

cf. https://pkwaredownloads.blob.core.windows.net/pem/APPNOTE.txt
"""

from __future__ import annotations

import datetime
import gzip
import io
import os
import struct
import zlib
import zipfile
from typing import AsyncIterable

import pytest

import gz2zip.core as core

LOCAL_HEADER_STRUCT = struct.Struct("<IHHHHHIIIHH")
CENTRAL_HEADER_STRUCT = struct.Struct("<IHHHHHHIIIHHHHHII")
EOCD_STRUCT = struct.Struct("<IHHHHIIH")
ZIP64_LOCATOR_STRUCT = struct.Struct("<IIQI")
DATA_DESCRIPTOR_ZIP32_STRUCT = struct.Struct("<IIII")
DATA_DESCRIPTOR_ZIP64_STRUCT = struct.Struct("<IIQQ")


# ==========================================
# Helpers
# ==========================================


def compress_data(data: bytes, filename: str = "", mtime: float = 0.0) -> bytes:
    """Generate standard GZIP bytes in-memory."""
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", filename=filename, mtime=mtime) as gz:
        gz.write(data)
    return buf.getvalue()


def build_gzip_with_optional_fields(
    data: bytes,
    *,
    extra: bytes = b"",
    filename: bytes = b"",
    comment: bytes = b"",
    add_header_crc: bool = False,
    mtime: int = 0,
) -> bytes:
    """Build a GZIP stream with selectable optional header fields."""
    flags = 0
    if extra:
        flags |= 0x04
    if filename:
        flags |= 0x08
    if comment:
        flags |= 0x10
    if add_header_crc:
        flags |= 0x02

    compressor = zlib.compressobj(wbits=-zlib.MAX_WBITS)
    deflate_payload = compressor.compress(data) + compressor.flush()

    header = bytearray(struct.pack("<BBBBIBB", 0x1F, 0x8B, 8, flags, mtime, 0, 255))
    if extra:
        header.extend(struct.pack("<H", len(extra)))
        header.extend(extra)
    if filename:
        header.extend(filename)
        header.extend(b"\x00")
    if comment:
        header.extend(comment)
        header.extend(b"\x00")
    if add_header_crc:
        header_crc16 = zlib.crc32(header) & 0xFFFF
        header.extend(struct.pack("<H", header_crc16))

    footer = struct.pack("<II", zlib.crc32(data) & 0xFFFFFFFF, len(data) & 0xFFFFFFFF)
    return bytes(header) + deflate_payload + footer


async def async_byte_stream(data: bytes, chunk_size: int = 8192) -> AsyncIterable[bytes]:
    """Mock an asynchronous stream of bytes."""
    for i in range(0, len(data), chunk_size):
        yield data[i : i + chunk_size]


async def consume_async_generator(gen) -> bytes:
    """Consume an AsyncGenerator entirely."""
    buf = bytearray()
    async for chunk in gen:
        buf.extend(chunk)
    return bytes(buf)


def convert_sync(
    gz_data: bytes,
    name: str,
    known_uncompressed_size: int | None = None,
    timestamp: datetime.datetime | None = None,
) -> bytes:
    """Run synchronous conversion and return ZIP bytes."""
    f_in = io.BytesIO(gz_data)
    f_out = io.BytesIO()
    core.gzip_to_zip(
        f_in=f_in,
        f_out=f_out,
        filename_in_zip=name,
        known_uncompressed_size=known_uncompressed_size,
        timestamp=timestamp,
    )
    return f_out.getvalue()


async def convert_async(
    gz_data: bytes,
    name: str,
    known_uncompressed_size: int | None = None,
    timestamp: datetime.datetime | None = None,
    *,
    chunk_size: int = 8192,
) -> bytes:
    """Run async conversion and return ZIP bytes."""
    stream = async_byte_stream(gz_data, chunk_size=chunk_size)
    zip_gen = core.stream_gzip_to_zip(
        stream,
        filename_in_zip=name,
        known_uncompressed_size=known_uncompressed_size,
        timestamp=timestamp,
    )
    return await consume_async_generator(zip_gen)


def parse_local_file_header(zip_data: bytes, offset: int = 0) -> dict[str, int | bytes]:
    """Parse local file header per APPNOTE 4.3.7."""
    (
        signature,
        version_needed,
        gp_flag,
        compression_method,
        mod_time,
        mod_date,
        crc32,
        compressed_size,
        uncompressed_size,
        filename_len,
        extra_len,
    ) = LOCAL_HEADER_STRUCT.unpack_from(zip_data, offset)

    name_start = offset + LOCAL_HEADER_STRUCT.size
    name_end = name_start + filename_len
    extra_end = name_end + extra_len

    return {
        "signature": signature,
        "version_needed": version_needed,
        "gp_flag": gp_flag,
        "compression_method": compression_method,
        "mod_time": mod_time,
        "mod_date": mod_date,
        "crc32": crc32,
        "compressed_size": compressed_size,
        "uncompressed_size": uncompressed_size,
        "filename_len": filename_len,
        "extra_len": extra_len,
        "filename": zip_data[name_start:name_end],
        "extra": zip_data[name_end:extra_end],
        "data_offset": extra_end,
        "header_size": LOCAL_HEADER_STRUCT.size + filename_len + extra_len,
    }


def parse_central_directory_header(
    zip_data: bytes, offset: int
) -> dict[str, int | bytes]:
    """Parse central directory header per APPNOTE 4.3.12."""
    (
        signature,
        version_made_by,
        version_needed,
        gp_flag,
        compression_method,
        mod_time,
        mod_date,
        crc32,
        compressed_size,
        uncompressed_size,
        filename_len,
        extra_len,
        comment_len,
        disk_start,
        internal_attr,
        external_attr,
        relative_offset_local_header,
    ) = CENTRAL_HEADER_STRUCT.unpack_from(zip_data, offset)

    name_start = offset + CENTRAL_HEADER_STRUCT.size
    name_end = name_start + filename_len
    extra_end = name_end + extra_len
    comment_end = extra_end + comment_len

    return {
        "signature": signature,
        "version_made_by": version_made_by,
        "version_needed": version_needed,
        "gp_flag": gp_flag,
        "compression_method": compression_method,
        "mod_time": mod_time,
        "mod_date": mod_date,
        "crc32": crc32,
        "compressed_size": compressed_size,
        "uncompressed_size": uncompressed_size,
        "filename_len": filename_len,
        "extra_len": extra_len,
        "comment_len": comment_len,
        "disk_start": disk_start,
        "internal_attr": internal_attr,
        "external_attr": external_attr,
        "relative_offset_local_header": relative_offset_local_header,
        "filename": zip_data[name_start:name_end],
        "extra": zip_data[name_end:extra_end],
        "comment": zip_data[extra_end:comment_end],
        "header_size": CENTRAL_HEADER_STRUCT.size + filename_len + extra_len + comment_len,
    }


def parse_eocd(zip_data: bytes) -> dict[str, int | bytes]:
    """Locate and parse EOCD per APPNOTE 4.3.16."""
    eocd_signature = b"PK\x05\x06"
    search_start = max(0, len(zip_data) - (0xFFFF + EOCD_STRUCT.size))
    eocd_offset = zip_data.rfind(eocd_signature, search_start)
    if eocd_offset < 0:
        raise AssertionError("EOCD signature not found")

    (
        signature,
        disk_no,
        cd_disk_no,
        entries_this_disk,
        entries_total,
        cd_size,
        cd_offset,
        comment_len,
    ) = EOCD_STRUCT.unpack_from(zip_data, eocd_offset)

    comment_start = eocd_offset + EOCD_STRUCT.size
    comment_end = comment_start + comment_len

    return {
        "signature": signature,
        "disk_no": disk_no,
        "cd_disk_no": cd_disk_no,
        "entries_this_disk": entries_this_disk,
        "entries_total": entries_total,
        "cd_size": cd_size,
        "cd_offset": cd_offset,
        "comment_len": comment_len,
        "comment": zip_data[comment_start:comment_end],
        "offset": eocd_offset,
    }


def decode_dos_time(mod_time: int, mod_date: int) -> tuple[int, int, int, int, int, int]:
    """Decode packed DOS date/time fields to components."""
    second = (mod_time & 0x1F) * 2
    minute = (mod_time >> 5) & 0x3F
    hour = (mod_time >> 11) & 0x1F
    day = mod_date & 0x1F
    month = (mod_date >> 5) & 0x0F
    year = ((mod_date >> 9) & 0x7F) + 1980
    return year, month, day, hour, minute, second


def generate_giant_gzip_zeros(target_size_gb: float) -> tuple[bytes, int]:
    """
    Generate a highly compressible GZIP stream above a target size.

    Uses 16MB zero chunks to quickly create ZIP64-threshold payloads.
    """
    target_size = int(target_size_gb * 1024 * 1024 * 1024)
    chunk_size = 16 * 1024 * 1024

    out = io.BytesIO()
    with gzip.GzipFile(fileobj=out, mode="wb") as gz:
        chunk = b"\x00" * chunk_size
        bytes_written = 0
        while bytes_written < target_size:
            gz.write(chunk)
            bytes_written += chunk_size

    return out.getvalue(), bytes_written


# ==========================================
# Tests: Basic Conversion & Integrity
# ==========================================


def test_sync_basic_conversion():
    data = b"Hello, World! " * 5000
    gz_data = compress_data(data)
    zip_data = convert_sync(gz_data, "test_sync.txt")

    with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
        assert zf.namelist() == ["test_sync.txt"]
        assert zf.read("test_sync.txt") == data
        assert zf.testzip() is None


@pytest.mark.asyncio
async def test_async_basic_conversion():
    data = b"Hello, Async World! " * 5000
    gz_data = compress_data(data)
    zip_data = await convert_async(gz_data, "test_async.txt", chunk_size=128)

    with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
        assert zf.namelist() == ["test_async.txt"]
        assert zf.read("test_async.txt") == data
        assert zf.testzip() is None


def test_sync_with_all_optional_gzip_header_fields():
    data = b"optional header fields " * 1000
    gz_data = build_gzip_with_optional_fields(
        data,
        extra=b"\xAA\xBB\xCC\xDD",
        filename=b"original.bin",
        comment=b"created by tests",
        add_header_crc=True,
        mtime=1700000000,
    )
    zip_data = convert_sync(gz_data, "renamed.bin")

    with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
        assert zf.namelist() == ["renamed.bin"]
        assert zf.read("renamed.bin") == data


@pytest.mark.asyncio
async def test_async_with_all_optional_gzip_header_fields():
    data = b"optional async header fields " * 1000
    gz_data = build_gzip_with_optional_fields(
        data,
        extra=b"\x00\x01",
        filename=b"origin_async.bin",
        comment=b"async-comment",
        add_header_crc=True,
        mtime=1700000010,
    )
    zip_data = await convert_async(gz_data, "renamed_async.bin", chunk_size=17)

    with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
        assert zf.namelist() == ["renamed_async.bin"]
        assert zf.read("renamed_async.bin") == data


# ==========================================
# Tests: APPNOTE Structure Compliance
# ==========================================


def test_sync_local_header_matches_appnote_4_3_7():
    data = b"local-header-check" * 512
    name = "spec.bin"
    zip_data = convert_sync(compress_data(data), name)

    local = parse_local_file_header(zip_data, 0)
    with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
        info = zf.getinfo(name)

    assert local["signature"] == 0x04034B50
    assert local["version_needed"] == 20
    assert local["compression_method"] == 8
    assert local["gp_flag"] & 0x0800  # APPNOTE 4.4.4 bit 11 (UTF-8 / EFS)
    assert not (local["gp_flag"] & 0x0008)  # bit 3 unset in sync mode
    assert local["filename"] == name.encode("utf-8")
    assert local["crc32"] == (zlib.crc32(data) & 0xFFFFFFFF)
    assert local["compressed_size"] == info.compress_size
    assert local["uncompressed_size"] == info.file_size


def test_sync_central_directory_and_eocd_match_appnote_4_3_12_4_3_16():
    data = b"central-directory-check" * 400
    name = "central.txt"
    zip_data = convert_sync(compress_data(data), name)

    eocd = parse_eocd(zip_data)
    central = parse_central_directory_header(zip_data, eocd["cd_offset"])

    assert eocd["signature"] == 0x06054B50
    assert eocd["disk_no"] == 0
    assert eocd["cd_disk_no"] == 0
    assert eocd["entries_this_disk"] == 1
    assert eocd["entries_total"] == 1
    assert eocd["comment_len"] == 0

    assert central["signature"] == 0x02014B50
    assert central["version_needed"] == 20
    assert central["compression_method"] == 8
    assert central["relative_offset_local_header"] == 0
    assert central["filename"] == name.encode("utf-8")
    assert central["comment_len"] == 0


def test_sync_utf8_flag_and_filename_encoding_match_appnote_4_4_4_bit11():
    data = b"utf8 name check"
    name = "cafe_\u00f1i\u00f1o_\u6f22\u5b57.txt"
    zip_data = convert_sync(compress_data(data), name)

    local = parse_local_file_header(zip_data, 0)
    eocd = parse_eocd(zip_data)
    central = parse_central_directory_header(zip_data, eocd["cd_offset"])

    assert local["gp_flag"] & 0x0800
    assert central["gp_flag"] & 0x0800
    assert local["filename"].decode("utf-8") == name
    assert central["filename"].decode("utf-8") == name


def test_sync_small_file_avoids_zip64_records():
    zip_data = convert_sync(compress_data(b"small"), "small.txt")
    eocd = parse_eocd(zip_data)
    local = parse_local_file_header(zip_data, 0)

    assert b"PK\x06\x06" not in zip_data  # zip64 EOCD
    assert b"PK\x06\x07" not in zip_data  # zip64 locator
    assert local["version_needed"] == 20
    assert eocd["entries_total"] == 1
    assert eocd["entries_this_disk"] == 1


@pytest.mark.asyncio
async def test_async_data_descriptor_matches_appnote_4_3_9():
    data = b"descriptor-check" * 2000
    name = "streamed.txt"
    zip_data = await convert_async(compress_data(data), name, chunk_size=101)

    local = parse_local_file_header(zip_data, 0)
    with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
        info = zf.getinfo(name)

    assert local["signature"] == 0x04034B50
    assert local["gp_flag"] & 0x0008  # bit 3 => data descriptor required
    assert local["crc32"] == 0
    assert local["compressed_size"] == 0
    assert local["uncompressed_size"] == 0

    dd_offset = local["data_offset"] + info.compress_size
    dd_signature, dd_crc32, dd_comp_size, dd_uncomp_size = (
        DATA_DESCRIPTOR_ZIP64_STRUCT.unpack_from(zip_data, dd_offset)
    )

    assert dd_signature == 0x08074B50
    assert dd_crc32 == (zlib.crc32(data) & 0xFFFFFFFF)
    assert dd_comp_size == info.compress_size
    assert dd_uncomp_size == len(data)

    eocd = parse_eocd(zip_data)
    assert eocd["cd_offset"] == dd_offset + DATA_DESCRIPTOR_ZIP64_STRUCT.size


@pytest.mark.asyncio
async def test_async_uses_zip64_eocd_and_locator():
    zip_data = await convert_async(compress_data(b"zip64-footer-check"), "zip64.txt")
    eocd = parse_eocd(zip_data)

    locator_offset = eocd["offset"] - ZIP64_LOCATOR_STRUCT.size
    locator_sig, disk_with_zip64, zip64_eocd_offset, total_disks = (
        ZIP64_LOCATOR_STRUCT.unpack_from(zip_data, locator_offset)
    )
    (
        zip64_sig,
        zip64_record_size,
        version_made_by,
        version_needed,
        _,
        _,
        entries_this_disk,
        entries_total,
        _,
        zip64_cd_offset,
    ) = struct.unpack_from("<IQHHIIQQQQ", zip_data, zip64_eocd_offset)

    assert eocd["entries_this_disk"] == 0xFFFF
    assert eocd["entries_total"] == 0xFFFF
    assert locator_sig == 0x07064B50
    assert disk_with_zip64 == 0
    assert total_disks == 1
    assert zip64_sig == 0x06064B50
    assert zip64_record_size == 44
    assert version_made_by == 45
    assert version_needed == 45
    assert entries_this_disk == 1
    assert entries_total == 1
    assert zip64_cd_offset == eocd["cd_offset"]


def test_sync_deflate_payload_excludes_gzip_footer():
    data = b"payload footer check " * 2048
    zip_data = convert_sync(compress_data(data), "payload.bin")

    with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
        info = zf.getinfo("payload.bin")

    local = parse_local_file_header(zip_data, info.header_offset)
    payload_start = local["data_offset"]
    payload_end = payload_start + info.compress_size
    compressed_payload = zip_data[payload_start:payload_end]

    inflater = zlib.decompressobj(-zlib.MAX_WBITS)
    restored = inflater.decompress(compressed_payload) + inflater.flush()

    assert inflater.eof
    assert inflater.unused_data == b""
    assert restored == data


# ==========================================
# Tests: Time, Header Building and Errors
# ==========================================


def test_explicit_timestamp():
    data = b"timestamp testing payload"
    dt = datetime.datetime(2025, 12, 31, 23, 59, 58)
    zip_data = convert_sync(compress_data(data), "time.txt", timestamp=dt)

    with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
        info = zf.getinfo("time.txt")
        assert info.date_time == (2025, 12, 31, 23, 59, 58)


def test_datetime_to_dos_time_clamps_pre_1980():
    mod_time, mod_date = core.datetime_to_dos_time(
        datetime.datetime(1979, 12, 31, 23, 59, 59)
    )
    assert decode_dos_time(mod_time, mod_date) == (1980, 1, 1, 0, 0, 0)


def test_datetime_to_dos_time_uses_two_second_precision():
    mod_time, mod_date = core.datetime_to_dos_time(
        datetime.datetime(2026, 1, 2, 3, 4, 59)
    )
    year, month, day, hour, minute, second = decode_dos_time(mod_time, mod_date)
    assert (year, month, day, hour, minute, second) == (2026, 1, 2, 3, 4, 58)


def test_data_descriptor_sizes_for_zip32_and_zip64():
    crc32 = 0x12345678
    comp_size = 0x9ABCDEF0
    uncomp_size = 0x11121314

    dd32 = core._create_data_descriptor(crc32, comp_size, uncomp_size, is_zip64=False)
    dd64 = core._create_data_descriptor(crc32, comp_size, uncomp_size, is_zip64=True)

    assert len(dd32) == DATA_DESCRIPTOR_ZIP32_STRUCT.size
    assert DATA_DESCRIPTOR_ZIP32_STRUCT.unpack(dd32) == (
        0x08074B50,
        crc32,
        comp_size,
        uncomp_size,
    )
    assert len(dd64) == DATA_DESCRIPTOR_ZIP64_STRUCT.size
    assert DATA_DESCRIPTOR_ZIP64_STRUCT.unpack(dd64) == (
        0x08074B50,
        crc32,
        comp_size,
        uncomp_size,
    )


def test_zip64_local_header_extra_field_layout():
    large_uncomp_size = 0x1_0000_0001
    large_comp_size = 0x1_0000_0002

    local_bytes = core._create_local_file_header(
        filename_bytes=b"large.bin",
        mod_time=0,
        mod_date=0,
        crc32=0xAABBCCDD,
        compress_size=large_comp_size,
        uncompressed_size=large_uncomp_size,
        use_data_descriptor=False,
    )
    local = parse_local_file_header(local_bytes)

    assert local["version_needed"] == 45
    assert local["compressed_size"] == 0xFFFFFFFF
    assert local["uncompressed_size"] == 0xFFFFFFFF

    extra_id, extra_size, original_size, compressed_size = struct.unpack(
        "<HHQQ", local["extra"]
    )
    assert extra_id == 0x0001
    assert extra_size == 16
    assert original_size == large_uncomp_size
    assert compressed_size == large_comp_size


def test_zip64_central_directory_extra_and_footer_layout():
    large_uncomp_size = 0x1_0000_0001
    large_comp_size = 0x1_0000_0002
    footer = core._create_central_directory_and_eocd(
        filename_bytes=b"large.bin",
        mod_time=0,
        mod_date=0,
        crc32=0x11223344,
        compress_size=large_comp_size,
        uncompressed_size=large_uncomp_size,
        local_header_offset=0,
        cd_offset=0,
        use_data_descriptor=False,
    )

    central = parse_central_directory_header(footer, 0)
    assert central["signature"] == 0x02014B50
    assert central["version_needed"] == 45
    assert central["compressed_size"] == 0xFFFFFFFF
    assert central["uncompressed_size"] == 0xFFFFFFFF

    extra_id, extra_size = struct.unpack_from("<HH", central["extra"], 0)
    original_size, compressed_size = struct.unpack_from("<QQ", central["extra"], 4)
    assert extra_id == 0x0001
    assert extra_size == 16
    assert original_size == large_uncomp_size
    assert compressed_size == large_comp_size

    zip64_eocd_offset = central["header_size"]
    assert struct.unpack_from("<I", footer, zip64_eocd_offset)[0] == 0x06064B50

    eocd = parse_eocd(footer)
    assert eocd["entries_this_disk"] == 0xFFFF
    assert eocd["entries_total"] == 0xFFFF

    locator_offset = eocd["offset"] - ZIP64_LOCATOR_STRUCT.size
    locator_sig, _, locator_zip64_offset, _ = ZIP64_LOCATOR_STRUCT.unpack_from(
        footer, locator_offset
    )
    assert locator_sig == 0x07064B50
    assert locator_zip64_offset == zip64_eocd_offset


@pytest.mark.asyncio
async def test_invalid_gzip_magic_handling():
    with pytest.raises(ValueError, match="Not a valid GZIP file"):
        core.gzip_to_zip(io.BytesIO(b"PK\x03\x04..."), io.BytesIO(), "fail.txt")

    stream = async_byte_stream(b"random garbage data")
    with pytest.raises(ValueError, match="Not a valid GZIP stream"):
        await consume_async_generator(core.stream_gzip_to_zip(stream, "fail.txt"))


def test_sync_rejects_non_deflate_method():
    broken = bytearray(compress_data(b"method-check-sync"))
    broken[2] = 0  # GZIP CM field

    with pytest.raises(ValueError, match="Compression method is not Deflate"):
        core.gzip_to_zip(io.BytesIO(bytes(broken)), io.BytesIO(), "bad-sync.txt")


@pytest.mark.asyncio
async def test_async_rejects_non_deflate_method():
    broken = bytearray(compress_data(b"method-check-async"))
    broken[2] = 0  # GZIP CM field

    with pytest.raises(ValueError, match="Compression method is not Deflate"):
        await consume_async_generator(
            core.stream_gzip_to_zip(async_byte_stream(bytes(broken)), "bad-async.txt")
        )


@pytest.mark.asyncio
async def test_async_rejects_missing_footer():
    # Valid fixed GZIP header + only 7 trailing bytes (footer must be 8 bytes).
    truncated = struct.pack("<BBBBIBB", 0x1F, 0x8B, 8, 0, 0, 0, 255) + (b"\x00" * 7)
    with pytest.raises(ValueError, match="missing CRC/ISIZE footer"):
        await consume_async_generator(
            core.stream_gzip_to_zip(async_byte_stream(truncated), "truncated.txt")
        )


# ==========================================
# Tests: Heavy ZIP64 Validation (> 4GB)
# ==========================================


def test_heavy_random_data_50mb():
    # 50MB of pseudo-random, mostly uncompressible data.
    block = os.urandom(1024 * 1024)
    data = block * 50
    zip_data = convert_sync(compress_data(data), "heavy.dat")

    with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
        assert zf.testzip() is None
        assert zf.read("heavy.dat") == data


@pytest.mark.memory_heavy
def test_zip64_giant_file_sync():
    gz_data, bytes_written = generate_giant_gzip_zeros(4.3)
    zip_data = convert_sync(
        gz_data,
        "giant_sync.bin",
        known_uncompressed_size=bytes_written,
    )

    with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
        info = zf.getinfo("giant_sync.bin")
        assert info.file_size == bytes_written
        assert zf.testzip() is None


@pytest.mark.asyncio
@pytest.mark.memory_heavy
async def test_zip64_giant_file_async():
    gz_data, bytes_written = generate_giant_gzip_zeros(4.3)
    zip_data = await convert_async(
        gz_data,
        "giant_async.bin",
        known_uncompressed_size=bytes_written,
        chunk_size=5 * 1024 * 1024,
    )

    with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
        info = zf.getinfo("giant_async.bin")
        assert info.file_size == bytes_written
        assert zf.testzip() is None
