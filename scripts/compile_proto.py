"""Proto compilation script for rag-eval-governance-worker."""

import sys
from pathlib import Path

from grpc_tools import protoc


def compile_proto() -> int:
    root = Path(__file__).resolve().parent.parent
    proto_dir = root / "api" / "proto" / "v1"
    proto_file = proto_dir / "eval_worker.proto"

    if not proto_file.exists():
        print(f"Error: {proto_file} does not exist", file=sys.stderr)
        return 1

    args = [
        "protoc",
        f"-I{proto_dir}",
        f"--python_out={proto_dir}",
        f"--grpc_python_out={proto_dir}",
        f"--pyi_out={proto_dir}",
        str(proto_file),
    ]

    print(f"Compiling {proto_file}...")
    result = protoc.main(args)
    if result == 0:
        print("Protobuf compilation succeeded.")
    else:
        print(f"Protobuf compilation failed with exit code {result}.", file=sys.stderr)
    return result


if __name__ == "__main__":
    sys.exit(compile_proto())
