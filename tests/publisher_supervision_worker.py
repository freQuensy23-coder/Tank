"""Observable local-only worker for the explicitly instrumented supervision image."""

import os
import time
from pathlib import Path


def main() -> None:
    state = Path("/host/apify-state")
    (state / "actual-worker-started").write_text(str(os.getpid()))
    while True:
        with (state / "actual-worker-effects").open("a") as output:
            output.write(str(time.monotonic_ns()) + "\n")
        time.sleep(0.02)


if __name__ == "__main__":
    main()
