import hashlib
import platform
import re
import subprocess
import sys
import tempfile
from contextlib import suppress
from pathlib import Path

VERSION = "25.8.33.6"
SOURCE_REVISION = "65eaf18b4a939f3e02aad50b7fbb2711b09495a0"
IMAGE = (
    "clickhouse/clickhouse-server:25.8.33.6@"
    "sha256:0152dd511befe6a2c2ef53e930726179669b08116da78500b37c51c96ff5ee77"
)
BINARY_SHA256 = {
    "amd64": "6a96266c0f00897225f9bb2ab0f965111298da75d7ec1b67465c6c9d5cf5ae81",
    "arm64": "6a23992b3035628dd1fc0fc981453db82b2f35fcf0f5c588293ce7c4918841fd",
}


def architecture() -> str:
    machine = platform.machine().lower()
    architectures = {"x86_64": "amd64", "amd64": "amd64", "aarch64": "arm64", "arm64": "arm64"}
    if machine not in architectures:
        raise RuntimeError("Unsupported native ClickHouse architecture")
    return architectures[machine]


def destination() -> Path:
    if sys.prefix == sys.base_prefix:
        raise RuntimeError("Native ClickHouse staging requires the project virtual environment")
    return Path(sys.executable).with_name("clickhouse")


def verify_binary(path: Path, arch: str) -> str:
    with path.open("rb") as binary:
        actual = hashlib.file_digest(binary, "sha256").hexdigest()
    if actual != BINARY_SHA256[arch]:
        raise RuntimeError("Native ClickHouse executable checksum mismatch")
    version = subprocess.check_output([str(path), "--version"], text=True).strip()
    if re.search(rf"\bversion {re.escape(VERSION)}(?:\s|$)", version) is None:
        raise RuntimeError("Native ClickHouse executable version mismatch")
    print(f"Native ClickHouse {VERSION}: arch={arch} sha256={actual}")
    return version


def main(container: str) -> None:
    if re.fullmatch(r"[0-9a-f]{64}", container) is None:
        raise RuntimeError("Native ClickHouse staging requires the exact CI service container ID")
    target = destination()
    arch = architecture()
    image = subprocess.check_output(
        ["docker", "inspect", "--format", "{{.Config.Image}}", container], text=True
    ).rstrip("\n")
    if image != IMAGE:
        raise RuntimeError("Native ClickHouse CI service image mismatch")
    temporary = tempfile.NamedTemporaryFile(prefix="native-clickhouse-", dir=target.parent, delete=False)
    try:
        candidate = Path(temporary.name)
        temporary.close()
        subprocess.run(["docker", "cp", f"{container}:/usr/bin/clickhouse", str(candidate)], check=True)
        candidate.chmod(0o755)
        verify_binary(candidate, arch)
        candidate.replace(target)
    except BaseException:
        with suppress(BaseException):
            temporary.close()
        with suppress(BaseException):
            Path(temporary.name).unlink(missing_ok=True)
        raise


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise RuntimeError("Native ClickHouse staging requires one CI service container ID")
    main(sys.argv[1])
