"""Discover the offline unit while preserving its frozen historical evidence path.

The original test bytes are pinned by openma_lifecycle/verification.v6.json.
Re-export the same test function so normal pytest discovery applies tests/conftest.py;
no assertions or active test cases are copied.
"""

import importlib.util
from pathlib import Path

_original = (
    Path(__file__).resolve().parents[1]
    / "deploy"
    / "openma_lifecycle"
    / "prepare_manifest_test.py"
)
_spec = importlib.util.spec_from_file_location("tank_retained_manifest_unit", _original)
_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_module)

test_composite_preserves_base_and_hashes_only_final_files = (
    _module.test_composite_preserves_base_and_hashes_only_final_files
)
