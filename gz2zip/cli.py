#!/usr/bin/env python3
"""
Unix-style Command Line Interface for high-speed GZIP to ZIP conversion.

Supports stdin/stdout piping, robust error handling, and quiet execution.
It behaves like a standard Unix filter:
  $ cat data.gz | python cli.py > data.zip
"""

import argparse
import asyncio
import datetime
import sys
from pathlib import Path
from typing import Optional

# Import the core logic
from . import core

def log(msg: str, quiet: bool = False) -> None:
    """Prints informational messages to stderr to keep stdout clean for binary pipelines."""
    if not quiet:
        print(msg, file=sys.stderr)


async def process_stream(
    f_out, 
    name: str, 
    size: Optional[int], 
    timestamp: Optional[datetime.datetime]
) -> None:
    """Consumes stdin asynchronously and streams the ZIP conversion to the output."""
    async def stdin_generator():
        # Read in 8MB chunks from standard input
        while True:
            chunk = sys.stdin.buffer.read(8 * 1024 * 1024)
            if not chunk:
                break
            yield chunk

    async for chunk in core.stream_gzip_to_zip(
        stdin_generator(),
        filename_in_zip=name,
        known_uncompressed_size=size,
        timestamp=timestamp
    ):
        f_out.write(chunk)
        f_out.flush()


def parse_args() -> argparse.Namespace:
    """Parses command line arguments."""
    parser = argparse.ArgumentParser(
        description="Convert GZIP to ZIP quickly. Supports Unix pipelines (stdin/stdout)."
    )
    
    parser.add_argument(
        "input", 
        type=str, 
        nargs="?",
        default="-",
        help="Input GZIP file (.gz). Use '-' or omit to read from stdin."
    )
    
    parser.add_argument(
        "-o", "--output", 
        type=str, 
        help="Output ZIP file. Use '-' for stdout. Defaults to stdout if input is stdin, "
             "otherwise replaces .gz with .zip."
    )
    
    parser.add_argument(
        "-n", "--name", 
        type=str, 
        help="Filename inside the ZIP archive. Defaults to the input filename, or 'stdin_data' for pipes."
    )
    
    parser.add_argument(
        "-s", "--size", 
        type=int, 
        help="Known uncompressed size in bytes. Providing this avoids a seek to the end of large files."
    )
    
    parser.add_argument(
        "-t", "--timestamp", 
        type=str, 
        help="Explicit modification timestamp in ISO 8601 format (e.g., '2025-12-31T23:59:58')."
    )
    
    parser.add_argument(
        "-q", "--quiet", 
        action="store_true", 
        help="Suppress all informational output (fatal errors are still printed to stderr)."
    )

    return parser.parse_args()


def _paths_refer_to_same_file(input_path: Path, output_path: Path) -> bool:
    """Best-effort check to avoid in-place overwrite of the input GZIP."""
    try:
        input_resolved = input_path.resolve(strict=False)
        output_resolved = output_path.resolve(strict=False)
    except OSError:
        return False
    if input_resolved == output_resolved:
        return True
    if input_path.exists() and output_path.exists():
        try:
            return input_path.samefile(output_path)
        except OSError:
            return False
    return False


def main() -> None:
    """Main CLI execution logic."""
    args = parse_args()

    if args.size is not None and args.size < 0:
        print("Error: --size must be a non-negative integer.", file=sys.stderr)
        sys.exit(1)

    # 1. Resolve Input target
    is_stdin = args.input == "-"
    
    # Unix safety: Prevent reading binary from a keyboard
    if is_stdin and sys.stdin.isatty():
        print("Error: Input is a terminal. Please pipe GZIP data or specify a file.", file=sys.stderr)
        sys.exit(1)
        
    # 2. Resolve Output target
    output_target = args.output
    if not output_target:
        output_target = "-" if is_stdin else None
        
    is_stdout = output_target == "-"

    # Unix safety: Prevent writing binary garbage to the screen
    if is_stdout and sys.stdout.isatty():
        print("Error: Output is a terminal. Refusing to write binary data to stdout.", file=sys.stderr)
        sys.exit(1)

    # 3. Resolve Internal ZIP Filename
    name_in_zip = args.name
    if not name_in_zip:
        if is_stdin:
            name_in_zip = "stdin_data"
        else:
            in_path = Path(args.input)
            name_in_zip = in_path.stem if in_path.suffix.lower() == '.gz' else in_path.name

    # 4. Resolve Output File Path (if not stdout)
    out_path = None
    if not is_stdout:
        if output_target:
            out_path = Path(output_target)
        else:
            in_path = Path(args.input)
            out_path = in_path.with_suffix('.zip') if in_path.suffix.lower() == '.gz' else in_path.with_suffix(in_path.suffix + '.zip')

    if not is_stdin and out_path and _paths_refer_to_same_file(Path(args.input), out_path):
        print("Error: Input and output paths must be different.", file=sys.stderr)
        sys.exit(1)

    # 5. Parse Timestamp
    timestamp = None
    if args.timestamp:
        try:
            timestamp = datetime.datetime.fromisoformat(args.timestamp)
        except ValueError:
            print(f"Error: Invalid timestamp format '{args.timestamp}'. Use 'YYYY-MM-DDTHH:MM:SS'.", file=sys.stderr)
            sys.exit(1)

    # 6. Logging
    log("Starting Conversion...", args.quiet)
    log(f"  Input:      {'<stdin>' if is_stdin else args.input}", args.quiet)
    log(f"  Output:     {'<stdout>' if is_stdout else out_path}", args.quiet)
    log(f"  Internal:   {name_in_zip}", args.quiet)

    # 7. Execute Conversion
    try:
        f_out = sys.stdout.buffer if is_stdout else open(out_path, 'wb')
        
        try:
            if is_stdin:
                # Use the asynchronous streaming generator designed for unseekable pipes
                asyncio.run(process_stream(f_out, name_in_zip, args.size, timestamp))
            else:
                # Use the ultra-fast synchronous approach designed for static files
                with open(args.input, 'rb') as f_in:
                    core.gzip_to_zip(
                        f_in=f_in,
                        f_out=f_out,
                        filename_in_zip=name_in_zip,
                        known_uncompressed_size=args.size,
                        timestamp=timestamp
                    )
        finally:
            if not is_stdout:
                f_out.close()

        log("Conversion completed successfully!", args.quiet)
        
    except Exception as e:
        print(f"\nError during conversion: {e}", file=sys.stderr)
        # Clean up partial output file if an error occurred and we weren't piping to stdout
        if not is_stdout and out_path and out_path.exists():
            out_path.unlink(missing_ok=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
