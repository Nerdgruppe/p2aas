#!/usr/bin/env python

from pathlib import Path
import sys
import urllib.parse
import argparse
import base64


def main():

    parser = argparse.ArgumentParser(
        description="Encodes the data on stdin in a url safe encoding.",
    )

    parser.add_argument(
        "--safe",
        type=str,
        default="/",
        help="Selects the characters that are safe to be used in the URL. Defaults to '/'",
    )

    parser.add_argument(
        "--file",
        type=Path,
        help="If given, will encode the given file instead of data from stdin.",
    )

    parser.add_argument(
        "--base64",
        action="store_true",
        help="If given, the data will also be encoded with base64",
    )

    parser.add_argument(
        "--base64url",
        action="store_true",
        help="If given, the data will also be encoded with URL safe base64",
    )

    args = parser.parse_args()

    if args.base64 and args.base64url:
        sys.stderr.write("--base64 and --base64url are mutually exclusive\n")
        sys.exit(1)

    data: bytes
    if args.file:
        data = args.file.read_bytes()
    else:
        data = sys.stdin.buffer.read()

    if args.base64:
        data = base64.standard_b64encode(data)
    elif args.base64url:
        data = base64.urlsafe_b64encode(data)

    encoded = urllib.parse.quote_from_bytes(data, safe=args.safe)

    sys.stdout.write(encoded)
    sys.stdout.write("\n")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
