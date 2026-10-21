"""Real static Capture binary, embedded schema, stamped startup and negative health."""

import json
import struct


def test_static_binary_embeds_canonical_schema(docker, image, image_config, repository_root, tmp_path):
    assert image_config["User"] == "1000:1000"
    assert image_config["Entrypoint"] == ["/tank-capture"]
    container = docker("create", "--network", "none", image).strip()
    try:
        binary = tmp_path / "tank-capture"
        docker("cp", container + ":/tank-capture", binary)
        data = binary.read_bytes()
    finally:
        docker("rm", container)
    assert data[:6] == b"\x7fELF\x02\x01", "Expected little-endian ELF64"
    start = struct.unpack_from("<Q", data, 32)[0]
    size, count = struct.unpack_from("<HH", data, 54)
    assert size >= 4 and count > 0 and start + size * count <= len(data)
    for index in range(count):
        assert struct.unpack_from("<I", data, start + index * size)[0] != 3, "Dynamic interpreter required"
    assert (repository_root / "capture/internal/schema/market_schema.json").read_bytes() in data


def test_startup_stamps_revision_before_closed_database_failure(docker, image, revision, repository_root):
    database = "postgresql://synthetic:synthetic@127.0.0.1:1/tank_test_docker?connect_timeout=2"
    output = docker(
        "run",
        "--rm",
        "--network",
        "none",
        "--mount",
        "type=bind,src=" + str(repository_root / "config") + ",dst=/config,readonly",
        "-e",
        "TANK_DATABASE_URL=" + database,
        image,
        expected=1,
    )
    rows = [json.loads(line) for line in output.splitlines() if line.strip()]
    starts = [row for row in rows if row.get("msg") == "starting"]
    assert len(starts) == 1
    assert starts[0]["code_version"] == "tank-capture/" + revision
    assert starts[0]["schema"] == "mkt" and starts[0]["instruments"] > 0
    errors = [row for row in rows if row.get("level") == "ERROR"]
    assert any("connect" in row.get("error", "").lower() for row in errors)


def test_healthcheck_fails_without_database(docker, image):
    docker("run", "--rm", "--network", "none", image, "healthcheck", expected=1)
