import copy

import market_truth_request_worker as worker
import pytest


def native_profile():
    return {
        "shared": {
            "frames": [
                {"name": "compute_market_truth", "file": "/owned/truth.cpython-313-x86_64-linux-gnu.so"},
                {
                    "name": "__pyx_f_4tank_8analysis_5truth__oi_stats",
                    "file": "/owned/truth.cpython-313-x86_64-linux-gnu.so",
                },
                {"name": "unrelated", "file": "/private/not-exported.py"},
            ]
        },
        "profiles": [{"type": "sampled", "samples": [[0, 1], [0, 1, 1], [2]], "weights": [1, 1, 1]}],
    }


def test_native_samples_count_only_real_compute_stacks_without_exporting_paths():
    source = native_profile()
    before = copy.deepcopy(source)
    result = worker.native_profile_rows(source)
    assert source == before
    assert result["kind"] == "native_computation_stack_samples"
    assert result["samples"] == 2
    assert result["rows"] == [
        {
            "module": "truth.cpython-313-x86_64-linux-gnu.so",
            "function": "__pyx_f_4tank_8analysis_5truth__oi_stats",
            "self_samples": 2,
            "inclusive_samples": 2,
        },
        {
            "module": "truth.cpython-313-x86_64-linux-gnu.so",
            "function": "compute_market_truth",
            "self_samples": 0,
            "inclusive_samples": 2,
        },
    ]


@pytest.mark.parametrize("index", [-1, 3, True, "0"])
def test_native_samples_refuse_invalid_frame_references(index):
    source = native_profile()
    source["profiles"][0]["samples"].append([index])
    source["profiles"][0]["weights"].append(1)
    with pytest.raises(ValueError):
        worker.native_profile_rows(source)


@pytest.mark.parametrize("samples", [[], [[2]], [[0]]])
def test_native_samples_refuse_absent_compiled_computation(samples):
    source = native_profile()
    source["profiles"][0]["samples"] = samples
    source["profiles"][0]["weights"] = [1] * len(samples)
    with pytest.raises(ValueError):
        worker.native_profile_rows(source)
