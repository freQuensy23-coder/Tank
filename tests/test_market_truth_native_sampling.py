import copy
import hashlib
import json
import signal
from contextlib import asynccontextmanager
from contextvars import copy_context

import market_truth_request_worker as worker
import pytest


def native_profile():
    return {
        "shared": {
            "frames": [
                {"name": "compute_market_truth", "file": "/owned/truth.cpython-313-x86_64-linux-gnu.so"},
                {
                    "name": "_oi_stats",
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
            "function": "_oi_stats",
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


@pytest.mark.parametrize("samples", [[], [[2]], [[0]], [[1]]])
def test_native_samples_refuse_absent_compiled_computation(samples):
    source = native_profile()
    source["profiles"][0]["samples"] = samples
    source["profiles"][0]["weights"] = [1] * len(samples)
    with pytest.raises(ValueError):
        worker.native_profile_rows(source)


@pytest.mark.parametrize(
    "name", ["tank.analysis.truth.compute_market_truth", "tank.analysis.truth._compute_market_truth"]
)
def test_qualified_native_entry_points_do_not_count_as_inner_helpers(name):
    source = native_profile()
    source["shared"]["frames"][0]["name"] = name
    source["profiles"][0]["samples"] = [[0]]
    source["profiles"][0]["weights"] = [1]
    with pytest.raises(ValueError):
        worker.native_profile_rows(source)


def test_native_samples_refuse_interpreted_frames_even_with_matching_function_names():
    source = native_profile()
    for frame in source["shared"]["frames"]:
        frame["file"] = "/owned/truth.py"
    with pytest.raises(ValueError):
        worker.native_profile_rows(source)


async def test_native_sampler_failure_preserves_only_safe_evidence_and_removes_owned_files(
    monkeypatch, tmp_path
):
    before = set(tmp_path.iterdir())
    stderr = b"Permission denied: private sampler detail"
    killed = []

    class Process:
        pid, returncode = 61741, 1

        async def communicate(self):
            return b"", stderr

    async def create(*args, **kwargs):
        assert kwargs["start_new_session"] is True
        assert "--native" in args and "--pid" not in args
        return Process()

    monkeypatch.setattr(worker.asyncio, "create_subprocess_exec", create)
    monkeypatch.setattr(worker.os, "killpg", lambda *args: killed.append(args))
    call = (copy_context(), worker.truth.compute_market_truth, ({},), {"config": {}})
    with pytest.raises(worker.NativeSamplingError) as caught:
        await worker.sample_retained_native(call, tmp_path, tmp_path)
    receipt = worker.failure_receipt(caught.value)
    assert receipt["native_sampling_failure"] == {
        "exit_code": 1,
        "permission_denied": True,
        "stderr_sha256": hashlib.sha256(stderr).hexdigest(),
        "child_failure": None,
    }
    assert "private sampler detail" not in json.dumps(receipt)
    assert str(tmp_path) not in json.dumps(receipt)
    assert killed == [(Process.pid, signal.SIGKILL)]
    assert set(tmp_path.iterdir()) == before


@pytest.mark.parametrize("initial_returncode", [None, 0])
async def test_native_sampler_deadline_kills_and_reaps_only_its_owned_group(
    monkeypatch, tmp_path, initial_returncode
):
    before = set(tmp_path.iterdir())
    killed, waited = [], []
    original_timeout = worker.asyncio.timeout

    class Process:
        pid, returncode = 61742, initial_returncode

        async def communicate(self):
            waited.append(self.pid)
            self.returncode = -9
            return b"", b""

    async def create(*args, **kwargs):
        assert kwargs["start_new_session"] is True
        return Process()

    @asynccontextmanager
    async def timeout(seconds):
        if seconds == 20:
            raise TimeoutError
        async with original_timeout(seconds):
            yield

    monkeypatch.setattr(worker.asyncio, "create_subprocess_exec", create)
    monkeypatch.setattr(worker.asyncio, "timeout", timeout)
    monkeypatch.setattr(worker.os, "killpg", lambda *args: killed.append(args))
    call = (copy_context(), worker.truth.compute_market_truth, ({},), {"config": {}})
    with pytest.raises(TimeoutError):
        await worker.sample_retained_native(call, tmp_path, tmp_path)
    assert killed == [(Process.pid, signal.SIGKILL)] and waited == [Process.pid]
    assert set(tmp_path.iterdir()) == before
