"""Exercise the real sandbox worker and its existing image contract."""


def test_analysis_libraries_browser_font_cache_and_worker_protocol(docker, image, repository_root):
    contract = repository_root / "deploy/openma_browser/image-contract.py"
    docker(
        "run",
        "--rm",
        "--network",
        "none",
        "--mount",
        "type=bind,src=" + str(contract) + ",dst=/tmp/tank-image-contract.py,readonly",
        "--entrypoint",
        "python3",
        image,
        "/tmp/tank-image-contract.py",
    )
