import hashlib
import importlib
import json
import pickle
import signal
import sys
import time
from decimal import localcontext
from importlib.machinery import ExtensionFileLoader
from pathlib import Path


def main():
    signal.alarm(15)
    root, inputs, output, begin = (Path(value).resolve() for value in sys.argv[1:5])
    sys.path.insert(0, str(root / "src"))
    from market_truth_request_worker import encode, failure_receipt, provenance

    try:
        artifacts = {}
        for name in ("tank.analysis.truth", "tank.clickhouse_market", "tank.util"):
            module = importlib.import_module(name)
            assert isinstance(module.__loader__, ExtensionFileLoader)
            filename = Path(module.__file__).resolve()
            artifacts[name] = {
                "path": str(filename.relative_to(root)),
                "sha256": hashlib.sha256(filename.read_bytes()).hexdigest(),
                "loader": type(module.__loader__).__name__,
            }
        provenance(root)
        snapshot, config, context = pickle.loads(inputs.read_bytes())
        from tank.analysis.truth import compute_market_truth

        while not begin.exists():
            time.sleep(0.02)
        assert sys.getprofile() is None
        with localcontext(context):
            result = compute_market_truth(snapshot, config=config, baseline_cache={})
        full = {key: value for key, value in result.items() if key not in {"mta_id", "code_version"}}
        digest = hashlib.sha256(json.dumps(full, sort_keys=True, default=encode).encode()).hexdigest()
        output.write_text(json.dumps({"result_sha256": digest, "artifacts": artifacts}, sort_keys=True))
    except BaseException as error:
        output.write_text(json.dumps(failure_receipt(error), sort_keys=True))
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
