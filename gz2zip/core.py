import struct
import datetime
from typing import IO, AsyncIterable, AsyncGenerator, Optional, Tuple


def datetime_to_dos_time(dt: datetime.datetime) -> Tuple[int, int]:
    """Converts a Python datetime object to MS-DOS time format.

    MS-DOS uses year values relative to 1980 and 2-second precision.

    Args:
        dt (datetime.datetime): The datetime to convert.

    Returns:
        Tuple[int, int]: A tuple containing the MS-DOS formatted
            modification time and date.
    """
    if dt.year < 1980:
        dt = datetime.datetime(1980, 1, 1, 0, 0, 0)
    mod_time = (dt.hour << 11) | (dt.minute << 5) | (dt.second // 2)
    mod_date = ((dt.year - 1980) << 9) | (dt.month << 5) | dt.day
    return mod_time, mod_date


def _resolve_dos_time(
    mtime: int, timestamp: Optional[datetime.datetime]
) -> Tuple[int, int]:
    """Resolves and formats the MS-DOS modification time and date.

    Args:
        mtime (int): The modification time parsed from the GZIP header.
        timestamp (Optional[datetime.datetime]): An explicit timestamp to override.

    Returns:
        Tuple[int, int]: A tuple containing the MS-DOS formatted time and date.
    """
    if timestamp:
        return datetime_to_dos_time(timestamp)
    if mtime != 0:
        dt = datetime.datetime.fromtimestamp(mtime)
        return datetime_to_dos_time(dt)
    return datetime_to_dos_time(datetime.datetime.now())


def _create_local_file_header(
    filename_bytes: bytes,
    mod_time: int,
    mod_date: int,
    crc32: int,
    compress_size: int,
    uncompressed_size: int,
    use_data_descriptor: bool,
) -> bytes:
    """Creates the ZIP Local File Header."""
    needs_zip64 = (
        uncompressed_size >= 0xFFFFFFFF
        or compress_size >= 0xFFFFFFFF
        or use_data_descriptor
    )

    version_needed = 45 if needs_zip64 else 20
    gp_flag = 0x0808 if use_data_descriptor else 0x0800

    if use_data_descriptor:
        lh_comp_size = 0
        lh_uncomp_size = 0
        lh_extra_field = b""
    elif needs_zip64:
        lh_comp_size = 0xFFFFFFFF
        lh_uncomp_size = 0xFFFFFFFF
        lh_extra_field = struct.pack(
            "<HHQQ", 0x0001, 16, uncompressed_size, compress_size
        )
    else:
        lh_comp_size = compress_size
        lh_uncomp_size = uncompressed_size
        lh_extra_field = b""

    lh = struct.pack(
        "<IHHHHHIIIHH",
        0x04034B50,
        version_needed,
        gp_flag,
        8,
        mod_time,
        mod_date,
        0 if use_data_descriptor else crc32,
        lh_comp_size,
        lh_uncomp_size,
        len(filename_bytes),
        len(lh_extra_field),
    )
    return lh + filename_bytes + lh_extra_field


def _create_data_descriptor(
    crc32: int, compress_size: int, uncompressed_size: int, is_zip64: bool
) -> bytes:
    """Creates the ZIP Data Descriptor."""
    if is_zip64:
        return struct.pack("<IIQQ", 0x08074B50, crc32, compress_size, uncompressed_size)
    return struct.pack("<IIII", 0x08074B50, crc32, compress_size, uncompressed_size)


def _create_central_directory_and_eocd(
    filename_bytes: bytes,
    mod_time: int,
    mod_date: int,
    crc32: int,
    compress_size: int,
    uncompressed_size: int,
    local_header_offset: int,
    cd_offset: int,
    use_data_descriptor: bool,
) -> bytes:
    """Creates the Central Directory Header and End of Central Directory records."""
    needs_zip64 = (
        uncompressed_size >= 0xFFFFFFFF
        or compress_size >= 0xFFFFFFFF
        or local_header_offset >= 0xFFFFFFFF
        or cd_offset >= 0xFFFFFFFF
        or use_data_descriptor
    )

    version_made_by = 45 if needs_zip64 else 20
    version_needed = 45 if needs_zip64 else 20
    gp_flag = 0x0808 if use_data_descriptor else 0x0800

    cd_comp_size = 0xFFFFFFFF if compress_size >= 0xFFFFFFFF else compress_size
    cd_uncomp_size = 0xFFFFFFFF if uncompressed_size >= 0xFFFFFFFF else uncompressed_size
    cd_local_offset = (
        0xFFFFFFFF if local_header_offset >= 0xFFFFFFFF else local_header_offset
    )

    cd_extra_payload = b""
    if needs_zip64:
        if cd_uncomp_size == 0xFFFFFFFF:
            cd_extra_payload += struct.pack("<Q", uncompressed_size)
        if cd_comp_size == 0xFFFFFFFF:
            cd_extra_payload += struct.pack("<Q", compress_size)
        if cd_local_offset == 0xFFFFFFFF:
            cd_extra_payload += struct.pack("<Q", local_header_offset)

    cd_extra_field = b""
    if cd_extra_payload:
        cd_extra_field = (
            struct.pack("<HH", 0x0001, len(cd_extra_payload)) + cd_extra_payload
        )

    cdh = struct.pack(
        "<IHHHHHHIIIHHHHHII",
        0x02014B50,
        version_made_by,
        version_needed,
        gp_flag,
        8,
        mod_time,
        mod_date,
        crc32,
        cd_comp_size,
        cd_uncomp_size,
        len(filename_bytes),
        len(cd_extra_field),
        0,  # file comment length
        0,  # disk number start
        0,  # internal file attr
        0,  # external file attr
        cd_local_offset,
    )

    cd_data = cdh + filename_bytes + cd_extra_field
    cd_size = len(cd_data)

    footer = bytearray(cd_data)

    if needs_zip64:
        zip64_eocd_offset = cd_offset + cd_size
        zip64_eocd = struct.pack(
            "<IQHHIIQQQQ",
            0x06064B50,
            44,
            45,
            45,
            0,
            0,
            1,
            1,
            cd_size,
            cd_offset,
        )
        zip64_locator = struct.pack("<IIQI", 0x07064B50, 0, zip64_eocd_offset, 1)
        footer.extend(zip64_eocd)
        footer.extend(zip64_locator)

    eocd_cd_size = 0xFFFFFFFF if cd_size >= 0xFFFFFFFF else cd_size
    eocd_cd_offset = 0xFFFFFFFF if cd_offset >= 0xFFFFFFFF else cd_offset
    eocd_entries = 0xFFFF if needs_zip64 else 1

    eocd = struct.pack(
        "<IHHHHIIH",
        0x06054B50,
        0,
        0,
        eocd_entries,
        eocd_entries,
        eocd_cd_size,
        eocd_cd_offset,
        0,
    )
    footer.extend(eocd)

    return bytes(footer)


def gzip_to_zip(
    f_in: IO[bytes],
    f_out: IO[bytes],
    filename_in_zip: str,
    known_uncompressed_size: Optional[int] = None,
    timestamp: Optional[datetime.datetime] = None,
) -> None:
    """Converts a GZIP stream to a ZIP stream at high speed without recompression.

    This synchronous function reads a GZIP compressed stream, extracts the raw
    Deflate payload, and encapsulates it within a ZIP format structure. It fully
    supports ZIP64 extensions for files larger than 4GB.

    Args:
        f_in (IO[bytes]): The input GZIP binary stream.
        f_out (IO[bytes]): The output ZIP binary stream.
        filename_in_zip (str): The filename to use inside the ZIP archive.
        known_uncompressed_size (Optional[int], optional): The uncompressed
            size of the data. If not provided, it will be read from the GZIP
            footer. Providing this can avoid reading the end of the file.
        timestamp (Optional[datetime.datetime], optional): The modification
            time to set in the ZIP header. If None, it uses the timestamp
            from the GZIP header, or the current time as a fallback.

    Raises:
        ValueError: If the input stream is not a valid GZIP file or if
            the compression method is not Deflate.
    """
    # 1. Parse GZIP Header
    magic = f_in.read(2)
    if magic != b"\x1f\x8b":
        raise ValueError("Not a valid GZIP file.")

    method, flag, mtime, xfl, os_type = struct.unpack("<BBIBB", f_in.read(8))
    if method != 8:
        raise ValueError("Compression method is not Deflate.")

    if flag & 0x04:  # FEXTRA
        extra_len = struct.unpack("<H", f_in.read(2))[0]
        f_in.seek(extra_len, 1)
    if flag & 0x08:  # FNAME
        while f_in.read(1) != b"\x00":
            pass
    if flag & 0x10:  # FCOMMENT
        while f_in.read(1) != b"\x00":
            pass
    if flag & 0x02:  # FHCRC
        f_in.seek(2, 1)

    deflate_start = f_in.tell()

    # Read footer for CRC32 and Uncompressed Size
    f_in.seek(-8, 2)
    deflate_end = f_in.tell()
    crc32, gz_isize = struct.unpack("<II", f_in.read(8))
    compress_size = deflate_end - deflate_start

    uncompressed_size = (
        known_uncompressed_size if known_uncompressed_size is not None else gz_isize
    )
    mod_time, mod_date = _resolve_dos_time(mtime, timestamp)
    filename_bytes = filename_in_zip.encode("utf-8")

    # 2. Write Local File Header
    lh_bytes = _create_local_file_header(
        filename_bytes=filename_bytes,
        mod_time=mod_time,
        mod_date=mod_date,
        crc32=crc32,
        compress_size=compress_size,
        uncompressed_size=uncompressed_size,
        use_data_descriptor=False,
    )
    f_out.write(lh_bytes)

    # 3. Copy Deflate Data
    f_in.seek(deflate_start)
    bytes_left = compress_size
    chunk_size = 8 * 1024 * 1024

    while bytes_left > 0:
        chunk = f_in.read(min(bytes_left, chunk_size))
        f_out.write(chunk)
        bytes_left -= len(chunk)

    cd_offset = f_out.tell()

    # 4. Write Central Directory and Footer
    footer_bytes = _create_central_directory_and_eocd(
        filename_bytes=filename_bytes,
        mod_time=mod_time,
        mod_date=mod_date,
        crc32=crc32,
        compress_size=compress_size,
        uncompressed_size=uncompressed_size,
        local_header_offset=0,
        cd_offset=cd_offset,
        use_data_descriptor=False,
    )
    f_out.write(footer_bytes)


async def stream_gzip_to_zip(
    gzip_stream: AsyncIterable[bytes],
    filename_in_zip: str,
    known_uncompressed_size: Optional[int] = None,
    timestamp: Optional[datetime.datetime] = None,
) -> AsyncGenerator[bytes, None]:
    """Asynchronously streams a GZIP AsyncIterable into a ZIP stream.

    Ideal for streaming responses in async web frameworks (e.g., FastAPI).
    It uses ZIP Data Descriptors to enable single-pass, on-the-fly conversion
    without seeking backwards.

    Args:
        gzip_stream (AsyncIterable[bytes]): The input asynchronous GZIP stream.
        filename_in_zip (str): The filename to use inside the ZIP archive.
        known_uncompressed_size (Optional[int], optional): The uncompressed
            size of the data. If not provided, it will be extracted from the
            GZIP footer at the end of the stream.
        timestamp (Optional[datetime.datetime], optional): The modification
            time to set in the ZIP header. If None, it uses the timestamp
            from the GZIP header, or the current time as a fallback.

    Yields:
        bytes: Chunks of the resulting ZIP archive.

    Raises:
        ValueError: If the input stream is not a valid GZIP stream or if
            the compression method is not Deflate.
        EOFError: If the GZIP stream ends unexpectedly.
    """
    iterator = aiter(gzip_stream)
    buffer = bytearray()

    async def read_exact(n: int) -> bytes:
        while len(buffer) < n:
            try:
                chunk = await anext(iterator)
                buffer.extend(chunk)
            except StopAsyncIteration:
                if len(buffer) < n:
                    raise EOFError("Unexpected end of GZIP stream")
                break
        data = buffer[:n]
        del buffer[:n]
        return bytes(data)

    async def read_until_zero() -> bytes:
        while b"\x00" not in buffer:
            try:
                chunk = await anext(iterator)
                buffer.extend(chunk)
            except StopAsyncIteration:
                raise EOFError("Unexpected end of GZIP stream")
        idx = buffer.index(b"\x00")
        data = buffer[: idx + 1]
        del buffer[: idx + 1]
        return bytes(data)

    # 1. Parse GZIP Header asynchronously
    magic = await read_exact(2)
    if magic != b"\x1f\x8b":
        raise ValueError("Not a valid GZIP stream.")

    header_base = await read_exact(8)
    method, flag, mtime, xfl, os_type = struct.unpack("<BBIBB", header_base)
    if method != 8:
        raise ValueError("Compression method is not Deflate.")

    if flag & 0x04:  # FEXTRA
        xlen_b = await read_exact(2)
        xlen = struct.unpack("<H", xlen_b)[0]
        await read_exact(xlen)
    if flag & 0x08:  # FNAME
        await read_until_zero()
    if flag & 0x10:  # FCOMMENT
        await read_until_zero()
    if flag & 0x02:  # FHCRC
        await read_exact(2)

    mod_time, mod_date = _resolve_dos_time(mtime, timestamp)
    filename_bytes = filename_in_zip.encode("utf-8")
    current_offset = 0

    # 2. Yield Local File Header (Data Descriptor Mode)
    lh_bytes = _create_local_file_header(
        filename_bytes=filename_bytes,
        mod_time=mod_time,
        mod_date=mod_date,
        crc32=0,
        compress_size=0,
        uncompressed_size=0,
        use_data_descriptor=True,
    )
    yield lh_bytes
    current_offset += len(lh_bytes)

    # 3. Stream Deflate Data & Maintain 8-byte Footer Window
    tail = bytearray(buffer)  # Leftover data from header parsing
    buffer.clear()
    compress_size = 0

    while True:
        try:
            chunk = await anext(iterator)
            tail.extend(chunk)
            # Yield everything except the last 8 bytes (reserved for CRC/ISIZE)
            if len(tail) > 8:
                yield_data = tail[:-8]
                yield yield_data
                compress_size += len(yield_data)
                current_offset += len(yield_data)
                del tail[:-8]
        except StopAsyncIteration:
            if len(tail) > 8:
                yield_data = tail[:-8]
                yield yield_data
                compress_size += len(yield_data)
                current_offset += len(yield_data)
                del tail[:-8]
            break

    if len(tail) != 8:
        raise ValueError("Invalid GZIP stream: missing CRC/ISIZE footer.")

    crc32, gz_isize = struct.unpack("<II", tail)
    uncompressed_size = (
        known_uncompressed_size if known_uncompressed_size is not None else gz_isize
    )

    # 4. Yield Data Descriptor
    dd_bytes = _create_data_descriptor(
        crc32=crc32,
        compress_size=compress_size,
        uncompressed_size=uncompressed_size,
        is_zip64=True,  # Data Descriptor mode defaults to ZIP64 for safety
    )
    yield dd_bytes
    current_offset += len(dd_bytes)

    # 5. Yield Central Directory and Footer
    footer_bytes = _create_central_directory_and_eocd(
        filename_bytes=filename_bytes,
        mod_time=mod_time,
        mod_date=mod_date,
        crc32=crc32,
        compress_size=compress_size,
        uncompressed_size=uncompressed_size,
        local_header_offset=0,
        cd_offset=current_offset,
        use_data_descriptor=True,
    )
    yield footer_bytes
