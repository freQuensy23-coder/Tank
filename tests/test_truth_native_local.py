import asyncio
import errno
import fcntl
import hashlib
import os
import re
import signal
import sys
from copy import deepcopy
from pathlib import Path
from threading import Event
from types import SimpleNamespace

import pytest
from test_truth_sql_conditions import original_truth as original_truth
from truth_native_test_safety import require_loopback_native_url

from tank.analysis import truth_native_local as local
from tank.clickhouse_market import _external_parameters, _parameter_plan, _wire_parameters, parameter_string


def _http_guard(error, message):
    return (
        re.match(
            r"\AClickHouse 500: Code: 395\. DB::Exception: " + re.escape(message) + r"(?=[:.] )",
            str(error),
        )
        is not None
    )


@pytest.mark.parametrize(
    "text",
    [
        "native local execution failed",
        "ClickHouse 500: Code: 62. DB::Exception: private-secret anchor must precede cutoff. ",
        "ClickHouse 500: Code: 395. DB::Exception: private-secret anchor must precede cutoff. ",
        "ClickHouse 500: Code: 395. DB::Exception: anchor must precede cutoff-extra. ",
        "ClickHouse 500: private-secret Code: 395. DB::Exception: anchor must precede cutoff. ",
    ],
)
def test_http_guard_rejects_generic_wrong_code_and_injected_message(text):
    assert not _http_guard(RuntimeError(text), "anchor must precede cutoff")


def test_local_plan_binds_small_strings_once_and_ignores_lexical_literals():
    value = "שלום😀\\\n\t\r\0'\""
    wire = parameter_string(value)
    query = (
        "SELECT {p:String}, '{p:String}', \"{p:String}\", `{p:String}`, "
        "$tag${p:String}$tag$, $$ {p:String} $$ /* {p:String} */ -- {p:String}\n"
        "# {p:String}\n, {p:String}"
    )
    plan = _parameter_plan(query, {"p": (wire, "String")}, bind_all=True)
    replacement = "(SELECT tuple(c0) FROM _tank_http_parameters).1"
    assert plan.query == query.replace("SELECT {p:String}", "SELECT " + replacement).replace(
        "\n, {p:String}", "\n, " + replacement
    )
    assert plan.structure == "c0 String"
    assert plan.data == (wire + "\n").encode()
    assert plan.fields == ()
    assert value not in plan.query


@pytest.mark.parametrize("kind", ["Identifier", "Bool", "Int64", "Nullable(String)", "String) ENGINE=URL"])
def test_local_plan_refuses_non_string_types(kind):
    with pytest.raises(ValueError, match="native local parameter"):
        _parameter_plan("SELECT {p:" + kind + "}", {"p": ("x", kind)}, bind_all=True)


@pytest.mark.parametrize(
    "query,params",
    [
        ("SELECT {absent:String}", {}),
        ("SELECT {p:Identifier}", {"p": ("x", "String")}),
        ("SELECT {p:String}", {"p": (True, "String")}),
        ("SELECT {p:String}", {"bad-name": ("x", "String")}),
    ],
)
def test_local_plan_rejects_unbound_or_invalid_parameters(query, params):
    with pytest.raises(ValueError, match="native local parameter"):
        _parameter_plan(query, params, bind_all=True)


def test_shared_http_plan_preserves_multipart_projection_exactly():
    params = {"big": ("\\nα" * 3000, "String"), "small": (True, "Bool"), "id": ("mkt", "Identifier")}
    query = "SELECT {big:String}, {small:Bool} FROM {id:Identifier}"
    wire = _wire_parameters(params)
    actual_query, files = _external_parameters(query, params, wire)
    assert (
        actual_query
        == "SELECT (SELECT tuple(c0) FROM _tank_http_parameters).1, {small:Bool} FROM {id:Identifier}"
    )
    assert files == [
        ("param_small", (None, b"true")),
        ("param_id", (None, b"mkt")),
        ("_tank_http_parameters_structure", (None, b"c0 String")),
        ("_tank_http_parameters_format", (None, b"TabSeparated")),
        ("_tank_http_parameters", ("parameters.tsv", (params["big"][0] + "\n").encode())),
    ]


def test_local_plan_avoids_existing_table_name_and_preserves_order():
    plan = _parameter_plan(
        "WITH _tank_http_parameters AS (SELECT 1) SELECT {b:String}, {a:String}",
        {"b": ("second", "String"), "a": ("first", "String")},
        bind_all=True,
    )
    assert plan.table == "_tank_http_parameters_"
    assert plan.structure == "c0 String, c1 String"
    assert plan.data == b"second\tfirst\n"
    assert "SELECT tuple(c0) FROM _tank_http_parameters_" in plan.query


@pytest.mark.parametrize("missing_bindings", [False, True])
def test_sealed_seekable_inputs_are_owned_and_readable(monkeypatch, missing_bindings):
    if missing_bindings:
        bindings = SimpleNamespace(**vars(os))
        for name in ("memfd_create", "MFD_CLOEXEC", "MFD_ALLOW_SEALING"):
            vars(bindings).pop(name, None)
        monkeypatch.setattr(local, "os", bindings)
    prepared = local._prepare("SELECT {p:String}", {"p": ("already\\n", "String")})
    descriptors = prepared.fds
    try:
        query_fd, data_fd = descriptors
        assert query_fd != data_fd
        assert os.read(query_fd, 4096).endswith(b" FORMAT JSONEachRow")
        assert os.read(data_fd, 4096) == b"already\\n\n"
        os.lseek(data_fd, 0, os.SEEK_SET)
        assert os.read(data_fd, 4096) == b"already\\n\n"
        for descriptor in descriptors:
            assert not os.get_inheritable(descriptor)
            assert fcntl.fcntl(descriptor, getattr(fcntl, "F_GET_SEALS", 1034)) & getattr(
                fcntl, "F_SEAL_WRITE", 0x0008
            )
            assert fcntl.fcntl(descriptor, 1034) == 0x000F
            with pytest.raises(OSError):
                os.write(descriptor, b"private")
    finally:
        prepared.close()
    for descriptor in descriptors:
        with pytest.raises(OSError):
            os.fstat(descriptor)


def test_pidfd_allocation_without_python_binding_is_owned_and_cloexec(monkeypatch):
    bindings = SimpleNamespace(**vars(os))
    vars(bindings).pop("pidfd_open", None)
    monkeypatch.setattr(local, "os", bindings)
    descriptor = local._pidfd_open(os.getpid())
    try:
        assert not os.get_inheritable(descriptor)
        os.fstat(descriptor)
        assert local._P_PIDFD == 3
    finally:
        os.close(descriptor)
    with pytest.raises(OSError):
        os.fstat(descriptor)


@pytest.mark.parametrize("primitive", ["memfd_create", "pidfd_open"])
@pytest.mark.parametrize("failure", ["loader", "symbol", "kernel"])
def test_missing_native_capabilities_fail_closed_and_private(monkeypatch, primitive, failure):
    bindings = SimpleNamespace(**vars(os))
    vars(bindings).pop(primitive, None)
    monkeypatch.setattr(local, "os", bindings)
    calls = []

    def unsupported(*args):
        calls.append(args)
        return -1

    def library(name, *, use_errno):
        assert name is None and use_errno is True
        if failure == "loader":
            raise OSError("private-loader-details")
        return SimpleNamespace(**({primitive: unsupported} if failure == "kernel" else {}))

    monkeypatch.setattr(
        local,
        "ctypes",
        SimpleNamespace(
            CDLL=library,
            c_int=local.ctypes.c_int,
            c_uint=local.ctypes.c_uint,
            c_char_p=local.ctypes.c_char_p,
            get_errno=lambda: errno.ENOSYS,
        ),
    )
    with pytest.raises((local._LocalFailure, OSError)) as caught:
        if primitive == "memfd_create":
            local._memfd_create()
        else:
            local._pidfd_open(os.getpid())
    assert "unavailable" in str(caught.value)
    assert "private" not in str(caught.value)
    if failure == "kernel":
        assert isinstance(caught.value, OSError)
        assert caught.value.errno == errno.ENOSYS
        assert vars(unsupported)["restype"] is local.ctypes.c_int
        assert vars(unsupported)["argtypes"] == (
            local.ctypes.c_char_p if primitive == "memfd_create" else local.ctypes.c_int,
            local.ctypes.c_uint,
        )
        assert calls == ([(b"tank-native-input", 3)] if primitive == "memfd_create" else [(os.getpid(), 0)])
    else:
        assert not calls


@pytest.mark.parametrize("primitive", ["memfd", "pidfd"])
def test_non_linux_native_primitives_are_refused(monkeypatch, primitive):
    monkeypatch.setattr(local, "sys", SimpleNamespace(platform="darwin"))
    with pytest.raises(local._LocalFailure, match="unavailable"):
        if primitive == "memfd":
            local._memfd_create()
        else:
            local._pidfd_open(os.getpid())


def test_sealing_failure_closes_real_owned_descriptor(monkeypatch):
    descriptors = []
    allocate = local._memfd_create

    def observed():
        descriptor = allocate()
        descriptors.append(descriptor)
        return descriptor

    def refuse(*args):
        raise OSError("private-sealing-error")

    monkeypatch.setattr(local, "_memfd_create", observed)
    monkeypatch.setattr(local, "fcntl", SimpleNamespace(fcntl=refuse))
    with pytest.raises(OSError):
        local._memfd(b"private-input")
    assert len(descriptors) == 1
    with pytest.raises(OSError):
        os.fstat(descriptors[0])


def test_fd_preparation_failure_closes_prior_descriptor(monkeypatch):
    actual = local._memfd
    descriptors = []

    def fault(data):
        if descriptors:
            raise OSError("private-input")
        descriptor = actual(data)
        descriptors.append(descriptor)
        return descriptor

    monkeypatch.setattr(local, "_memfd", fault)
    with pytest.raises(OSError):
        local._prepare("SELECT {p:String}", {"p": ("x", "String")})
    for descriptor in descriptors:
        with pytest.raises(OSError):
            os.fstat(descriptor)


class SyntheticSubprocessFaultTransport:
    def __init__(self, stdout=b"", stderr=b"", code=0):
        self.pid = 987654321
        self.returncode = None
        self.stdout = asyncio.StreamReader()
        self.stderr = asyncio.StreamReader()
        self.stdout.feed_data(stdout)
        self.stdout.feed_eof()
        self.stderr.feed_data(stderr)
        self.stderr.feed_eof()
        self.code = code
        self.reaped = False
        self.owns_group = True

    async def wait(self):
        self.returncode = self.code
        return self.code

    def reap(self):
        self.owns_group = False
        self.reaped = True

    def close_readers(self):
        for reader in (self.stdout, self.stderr):
            if not reader.at_eof():
                reader.feed_eof()


@pytest.fixture
def synthetic_spawn(monkeypatch):
    calls = []
    processes = []

    async def spawn(*args, **kwargs):
        calls.append((args, kwargs))
        return processes.pop(0)

    monkeypatch.setattr(local, "_spawn", spawn)
    monkeypatch.setattr(local, "_binary", lambda: Path(sys.executable).absolute().with_name("clickhouse"))
    monkeypatch.setattr(local.os, "killpg", lambda pid, sig: None)
    return calls, processes


@pytest.mark.parametrize("failure", ["exit", "json", "shape", "stdout", "stderr"])
async def test_synthetic_subprocess_faults_are_bounded_private_and_reaped(
    synthetic_spawn, monkeypatch, failure
):
    calls, processes = synthetic_spawn
    secret = b"private-token-secret"
    process = SyntheticSubprocessFaultTransport(
        stdout=secret
        if failure == "json"
        else b"[]\n"
        if failure == "shape"
        else b"x" * 65
        if failure == "stdout"
        else b"",
        stderr=secret * 4 if failure == "stderr" else secret,
        code=43 if failure == "exit" else 0,
    )
    processes.append(process)
    monkeypatch.setattr(local, "_MAX_STDOUT_BYTES", 64)
    monkeypatch.setattr(local, "_MAX_STDERR_BYTES", 64)
    monkeypatch.setenv("PRIVATE_CLOUD_SECRET", secret.decode())
    provider = local.ReplayNativeRows()
    try:
        with pytest.raises(RuntimeError) as raised:
            await provider.rows("SELECT {p:String}", {"p": (secret.decode(), "String")})
        assert secret.decode() not in str(raised.value)
        assert "SELECT" not in str(raised.value)
        assert process.reaped
        assert not provider._requests
        ((args, kwargs),) = calls
        assert args[0] == str(Path(sys.executable).absolute().with_name("clickhouse"))
        assert args[1] == "local"
        assert "--queries-file" in args and "--query-file" not in args
        assert secret.decode() not in repr(args)
        assert set(kwargs["env"]) == {"HOME", "TMPDIR", "TZ", "LANG", "LC_ALL"}
        assert kwargs["start_new_session"] is True
        assert not Path(kwargs["cwd"]).exists()
        for descriptor in kwargs["pass_fds"]:
            with pytest.raises(OSError):
                os.fstat(descriptor)
    finally:
        await provider.close()


async def test_missing_binary_fails_without_spawn(monkeypatch):
    monkeypatch.setattr(local.Path, "is_file", lambda path: False)
    provider = local.ReplayNativeRows()
    with pytest.raises(RuntimeError, match="binary unavailable"):
        await provider.rows("SELECT 1")
    await provider.close()
    with pytest.raises(RuntimeError, match="closed"):
        await provider.rows("SELECT 1")


@pytest.mark.parametrize(
    "stderr,code,expected",
    [
        (
            b"Code: 395. DB::Exception: anchor must precede cutoff. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO)\n",
            139,
            (395, "anchor_must_precede_cutoff"),
        ),
        (
            b"Code: 395. DB::Exception: Market Truth decimal input outside native financial range: while executing private-secret. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO) (version 25.8.22.40 (official build))\n",
            139,
            (395, "decimal_input_outside_financial_range"),
        ),
        (b"Code: 62. DB::Exception: Syntax error private-secret\n", 62, (62, None)),
        (
            b"Code: 651. DB::Exception: anchor must precede cutoff. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO)\n",
            139,
            (651, None),
        ),
        (
            b"Code: 395. DB::Exception: private-secret anchor must precede cutoff. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO)\n",
            139,
            (395, None),
        ),
        (
            b"Code: 395. DB::Exception: anchor must precede cutoff-extra. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO)\n",
            139,
            (395, None),
        ),
        (b"Code: 395. DB::Exception: anchor must precede cutoff. private-secret\n", 139, (395, None)),
        (
            b"Code: 395. DB::Exception: anchor must precede cutoff. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO)\n",
            -11,
            (None, None),
        ),
        (
            b"Code: 395. DB::Exception: anchor must precede cutoff. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO)\n",
            62,
            (None, None),
        ),
        (
            b"private-secret log\nCode: 395. DB::Exception: anchor must precede cutoff. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO)\n",
            139,
            (None, None),
        ),
        (
            b"Code: 62. DB::Exception: BadValueData private-secret\nCode: 395. DB::Exception: Market Truth decimal input outside native financial range. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO)\n",
            62,
            (None, None),
        ),
        (
            b"Code: 395. DB::Exception: anchor must precede cutoff. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO)\nCode: 395. private-secret\n",
            139,
            (None, None),
        ),
        (
            b"Code: 0395. DB::Exception: anchor must precede cutoff. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO)\n",
            139,
            (None, None),
        ),
        (
            b"Code: 395. NotException: anchor must precede cutoff. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO)\n",
            139,
            (None, None),
        ),
        (b"Bad arguments: private-secret Code: 395 anchor must precede cutoff\n", 139, (None, None)),
        (b"private-secret", 139, (None, None)),
    ],
)
def test_failure_identity_uses_only_unambiguous_native_header_and_static_guard(stderr, code, expected):
    assert local._failure_identity(stderr, code) == expected


def test_failure_identity_refuses_oversized_diagnostics(monkeypatch):
    monkeypatch.setattr(local, "_MAX_STDERR_BYTES", 16)
    assert local._failure_identity(b"Code: 62. DB::Exception: private-secret", 62) == (None, None)


async def test_synthetic_native_failure_publishes_only_validated_metadata(synthetic_spawn):
    _, processes = synthetic_spawn
    secret = "private-secret"
    process = SyntheticSubprocessFaultTransport(
        stderr=b"Code: 395. DB::Exception: Market Truth decimal input outside native financial range: while executing private-secret. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO)\n",
        code=139,
    )
    processes.append(process)
    provider = local.ReplayNativeRows()
    try:
        with pytest.raises(local._LocalFailure) as caught:
            await provider.rows("SELECT {p:String}", {"p": (secret, "String")})
        error = caught.value
        assert str(error) == "native local execution failed"
        assert vars(error) == {"native_code": 395, "source_guard": "decimal_input_outside_financial_range"}
        assert secret not in repr(error) + repr(vars(error))
        assert not hasattr(error, "stderr")
        assert process.reaped
    finally:
        await provider.close()


@pytest.mark.parametrize(
    "native_code,source_guard",
    [
        (True, "anchor_must_precede_cutoff"),
        (395, "private-secret"),
        (62, "anchor_must_precede_cutoff"),
        (395, []),
    ],
)
def test_failure_metadata_rejects_unvalidated_categories(native_code, source_guard):
    error = local._LocalFailure(
        "native local execution failed", native_code=native_code, source_guard=source_guard
    )
    assert error.source_guard is None
    assert error.native_code == (native_code if type(native_code) is int else None)


async def test_cancellation_during_thread_preparation_closes_late_fds(monkeypatch, synthetic_spawn):
    entered, release = Event(), Event()
    actual = local._prepare
    prepared = []

    def delayed(query, params):
        entered.set()
        assert release.wait(2)
        inputs = actual(query, params)
        prepared.append(inputs)
        return inputs

    monkeypatch.setattr(local, "_prepare", delayed)
    provider = local.ReplayNativeRows()
    request = asyncio.create_task(provider.rows("SELECT {p:String}", {"p": ("x", "String")}))
    assert await asyncio.to_thread(entered.wait, 2)
    request.cancel()
    closing = asyncio.create_task(provider.close())
    await asyncio.sleep(0)
    request.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await request
    await closing
    assert prepared and prepared[0].closed
    assert not synthetic_spawn[0]
    for descriptor in prepared[0].fds:
        with pytest.raises(OSError):
            os.fstat(descriptor)


@pytest.mark.parametrize("case", ["query", "values", "count"])
def test_local_input_limits_refuse_before_allocating_fds(monkeypatch, case):
    monkeypatch.setattr(local, "_MAX_INPUT_BYTES", 64)
    monkeypatch.setattr(local, "_MAX_PARAMETERS", 2)
    query = "SELECT {p:String}"
    params = {"p": ("x", "String")}
    if case == "query":
        query += " " * 100
    elif case == "values":
        params["p"] = ("α" * 64, "String")
    else:
        params.update(a=("a", "String"), b=("b", "String"))
    with pytest.raises(ValueError, match="input limit"):
        local._prepare(query, params)


async def test_synthetic_spawn_failure_does_not_expose_exception_or_inputs(monkeypatch, synthetic_spawn):
    calls, _ = synthetic_spawn

    async def failed(*args, **kwargs):
        calls.append((args, kwargs))
        raise OSError("private-credential SELECT hidden")

    monkeypatch.setattr(local, "_spawn", failed)
    provider = local.ReplayNativeRows()
    with pytest.raises(RuntimeError, match="^native local request failed$"):
        await provider.rows("SELECT {p:String}", {"p": ("private-credential", "String")})
    await provider.close()
    assert not Path(calls[0][1]["cwd"]).exists()
    for descriptor in calls[0][1]["pass_fds"]:
        with pytest.raises(OSError):
            os.fstat(descriptor)


async def test_cancellation_during_spawn_reaps_late_child_and_repeated_close(monkeypatch, synthetic_spawn):
    calls, _ = synthetic_spawn
    entered, release = asyncio.Event(), asyncio.Event()
    process = SyntheticSubprocessFaultTransport()
    signalled = []

    async def delayed(*args, **kwargs):
        calls.append((args, kwargs))
        entered.set()
        await release.wait()
        return process

    monkeypatch.setattr(local, "_spawn", delayed)
    monkeypatch.setattr(local.os, "killpg", lambda pid, signal: signalled.append((pid, signal)))
    provider = local.ReplayNativeRows()
    request = asyncio.create_task(provider.rows("SELECT {p:String}", {"p": ("private", "String")}))
    await entered.wait()
    request.cancel()
    closing = asyncio.create_task(provider.close())
    await asyncio.sleep(0)
    closing.cancel()
    request.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await request
    with pytest.raises(asyncio.CancelledError):
        await closing
    await provider.close()
    assert process.reaped and signalled
    assert not provider._requests
    assert not Path(calls[0][1]["cwd"]).exists()


async def test_close_cancels_two_owned_children_during_pipe_draining(monkeypatch, synthetic_spawn):
    calls, processes = synthetic_spawn
    entered = asyncio.Event()
    stopped = {}
    children = []
    actual_drain = local._drain

    class WaitingFaultTransport(SyntheticSubprocessFaultTransport):
        def __init__(self, pid):
            super().__init__()
            self.pid = pid
            self.stdout = asyncio.StreamReader()
            self.stderr = asyncio.StreamReader()
            stopped[pid] = asyncio.Event()

        async def wait(self):
            await stopped[self.pid].wait()
            return await super().wait()

    async def draining(*args, **kwargs):
        if len(calls) == 2:
            entered.set()
        return await actual_drain(*args, **kwargs)

    def terminate(pid, sig):
        child = next(child for child in children if child.pid == pid)
        if not stopped[pid].is_set():
            stopped[pid].set()
            child.stdout.feed_eof()
            child.stderr.feed_eof()

    children.extend(WaitingFaultTransport(pid) for pid in (987654321, 987654322))
    processes.extend(children)
    monkeypatch.setattr(local, "_drain", draining)
    monkeypatch.setattr(local.os, "killpg", terminate)
    provider = local.ReplayNativeRows()
    requests = [
        asyncio.create_task(provider.rows("SELECT {p:String}", {"p": ("x", "String")})) for _ in children
    ]
    await entered.wait()
    await provider.close()
    for request in requests:
        with pytest.raises(asyncio.CancelledError):
            await request
    assert all(child.reaped for child in children)
    assert all(not Path(kwargs["cwd"]).exists() for _, kwargs in calls)
    assert not provider._requests


@pytest.mark.parametrize("phase", ["already_reaped", "term_exit", "term_reaped"])
async def test_cleanup_never_signals_recycled_group_after_reap(monkeypatch, synthetic_spawn, phase):
    _, processes = synthetic_spawn
    draining = asyncio.Event()
    exited = asyncio.Event()
    signals = []
    actual_drain = local._drain
    process = SyntheticSubprocessFaultTransport(stdout=b"retained-private-buffer")
    process.stdout = asyncio.StreamReader()
    process.stderr = asyncio.StreamReader()
    process.stdout.feed_data(b"retained-private-buffer")
    if phase == "already_reaped":
        process.returncode = 0
        process.reap()
        exited.set()

    async def wait():
        await exited.wait()
        process.returncode = -signal.SIGTERM if phase != "already_reaped" else 0
        if phase == "term_reaped":
            process.reap()
        return process.returncode

    async def drain(*args, **kwargs):
        draining.set()
        return await actual_drain(*args, **kwargs)

    def killpg(pid, sig):
        assert process.owns_group and not process.reaped, "recycled unrelated group was targeted"
        assert pid == process.pid
        signals.append(sig)
        exited.set()

    process.wait = wait
    processes.append(process)
    monkeypatch.setattr(local, "_drain", drain)
    monkeypatch.setattr(local.os, "killpg", killpg)
    provider = local.ReplayNativeRows()
    request = asyncio.create_task(provider.rows("SELECT 1"))
    await draining.wait()
    request.cancel()
    closing = asyncio.create_task(provider.close())
    request.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request
    await closing
    await provider.close()
    assert process.reaped and not process.owns_group
    assert process.stdout.at_eof() and process.stderr.at_eof()
    assert signals == (
        []
        if phase == "already_reaped"
        else [signal.SIGTERM, signal.SIGKILL]
        if phase == "term_exit"
        else [signal.SIGTERM]
    )
    local._signal_group(process, signal.SIGKILL)
    assert signals == (
        []
        if phase == "already_reaped"
        else [signal.SIGTERM, signal.SIGKILL]
        if phase == "term_exit"
        else [signal.SIGTERM]
    )


async def test_owned_exit_notification_does_not_reap_before_final_group_signal(monkeypatch):
    events = []
    popen = SimpleNamespace(pid=987654321, returncode=None)

    def reap():
        events.append("reap")
        popen.returncode = 0
        return 0

    popen.wait = reap
    child = local._OwnedProcess(popen, 123456789)
    child._loop = asyncio.get_running_loop()
    monkeypatch.setattr(child._loop, "remove_reader", lambda fd: events.append("remove"))
    monkeypatch.setattr(local.os, "waitid", lambda *args: SimpleNamespace(si_code=os.CLD_EXITED, si_status=0))
    monkeypatch.setattr(local.os, "close", lambda fd: events.append("close"))
    monkeypatch.setattr(local.os, "killpg", lambda pid, sig: events.append(sig))
    child._exited()
    assert await child.wait() == 0
    assert popen.returncode is None and child.owns_group
    overflow = asyncio.Event()
    child.stdout.feed_data(b"retained-output")
    child.stderr.feed_data(b"retained-diagnostic")
    child.stdout.feed_eof()
    child.stderr.feed_eof()
    buffers = await asyncio.gather(
        local._drain(child.stdout, 64, overflow, keep=True),
        local._drain(child.stderr, 64, overflow, keep=True),
    )
    assert buffers == [b"retained-output", b"retained-diagnostic"]
    assert not overflow.is_set()
    local._signal_group(child, signal.SIGKILL)
    child.reap()
    child.reap()
    local._signal_group(child, signal.SIGKILL)
    assert events.index(signal.SIGKILL) < events.index("reap")
    assert events.count(signal.SIGKILL) == 1
    assert not child.owns_group


@pytest.mark.parametrize("failure", ["pidfd_unsupported", "waitid_unsupported", "already_reaped"])
def test_process_handle_setup_fails_closed_before_reap(monkeypatch, failure):
    events = []
    pipes = [SimpleNamespace(close=lambda: events.append("pipe_close")) for _ in range(2)]
    process = SimpleNamespace(pid=987654321, stdout=pipes[0], stderr=pipes[1])
    process.wait = lambda: events.append("reap")
    monkeypatch.setattr(local.signal, "getsignal", lambda sig: signal.SIG_DFL)
    monkeypatch.setattr(local.subprocess, "Popen", lambda *args, **kwargs: process)

    def pidfd_open(pid):
        if failure == "pidfd_unsupported":
            raise OSError("private-kernel-error")
        return 123456789

    def waitid(*args):
        if failure == "already_reaped":
            raise ChildProcessError("private-owner-lost")
        raise OSError("private-kernel-error")

    def killpg(pid, sig):
        assert "reap" not in events
        assert failure != "already_reaped", "recycled unrelated group was targeted"
        events.append("kill")

    monkeypatch.setattr(local, "_pidfd_open", pidfd_open)
    monkeypatch.setattr(local.os, "waitid", waitid)
    monkeypatch.setattr(local.os, "killpg", killpg)
    monkeypatch.setattr(local.os, "close", lambda fd: events.append("pidfd_close"))
    with pytest.raises(OSError):
        local._start_process(("native",), {})
    assert events.count("reap") == 1
    assert events.count("pipe_close") == 2
    assert events.count("kill") == (0 if failure == "already_reaped" else 1)
    if failure != "already_reaped":
        assert events.index("kill") < events.index("reap")


@pytest.fixture
async def native_local():
    binary = Path(sys.executable).with_name("clickhouse")
    assert binary.is_absolute() and binary.is_file(), "pinned interpreter-sibling native binary is required"
    provider = local.ReplayNativeRows()
    try:
        yield provider
    finally:
        await provider.close()


@pytest.mark.integration
@pytest.mark.process
@pytest.mark.parametrize("large", [False, True])
async def test_real_local_scalar_controls_repeated_cte_reads_and_large_unicode(native_local, large):
    value = "α😀שלום漢字\\literal\n\t\r\0'\"" * (150000 if large else 1)
    literal = "Ω😀漢字" * (20000 if large else 1)
    params = {
        "p": (parameter_string(value), "String"),
        "q": (parameter_string(value[::-1]), "String"),
        "empty": ("", "String"),
        "nulltext": (parameter_string("\\N"), "String"),
    }
    params.update({f"small{i}": (f"small{i}", "String") for i in range(32)})
    query = (
        "WITH a AS (SELECT {p:String} AS v), b AS (SELECT {q:String} AS v) "
        "SELECT (SELECT v FROM a) AS value, length((SELECT v FROM b)) AS bytes, "
        "(SELECT v FROM b) AS second_value, getSetting('use_query_cache') AS query_cache, "
        "lengthUTF8({p:String}) AS chars, lower(hex(SHA256({p:String}))) AS hash, "
        "substringUTF8({p:String}, 1, 12) AS prefix, toTypeName({p:String}) AS kind, "
        f"'{literal}' AS literal, '{{p:String}}' AS quoted, $q${{p:String}}$q$ AS heredoc, "
        "1 AS `quoted{p:String}`, {empty:String} AS empty_value, {nulltext:String} AS null_text, "
        + ", ".join("{" + f"small{i}:String" + "} AS s" + str(i) for i in range(32))
        + " /* {p:String} */ -- {p:String}\n# {p:String}\n"
    )
    rows = await native_local.rows(query, params)
    (row,) = rows
    assert row["value"] == value
    assert row["second_value"] == value[::-1]
    assert not row["query_cache"]
    assert row["bytes"] == len(value.encode())
    assert row["chars"] == len(value)
    assert row["hash"] == hashlib.sha256(value.encode()).hexdigest()
    assert row["prefix"] == value[:12]
    assert row["kind"] == "String"
    assert row["literal"] == literal
    assert row["quoted"] == row["heredoc"] == "{p:String}"
    assert row["empty_value"] == "" and row["null_text"] == "\\N"
    assert all(row[f"s{i}"] == f"small{i}" for i in range(32))


@pytest.mark.integration
@pytest.mark.process
@pytest.mark.parametrize("case", ["supply", "fresh", "physical", "no_anchor", "future", "incomplete"])
async def test_real_local_assessment_matches_http_and_independent_original(
    native_local, original_truth, monkeypatch, case
):
    from datetime import timedelta

    from test_truth_sql_conditions import _physical_snapshot
    from test_truth_sql_engine import assert_complete, forbid_python_business
    from test_truth_sql_support import _fresh_snapshot, _supply_snapshot

    from tank.analysis import truth_sql_engine as engine
    from tank.clickhouse_market import ClickHouse
    from tank.util import iso, parse_time

    packet = (
        _supply_snapshot()
        if case == "supply"
        else _fresh_snapshot()[0]
        if case == "fresh"
        else _physical_snapshot()
    )
    packet.pop("_primary", None)
    control_packet = deepcopy(packet)
    if case == "no_anchor":
        packet.pop("anchor", None)
    elif case == "future":
        packet["anchor"]["ts"] = iso(parse_time(packet["cutoff"]) + timedelta(seconds=60))
    elif case == "incomplete":
        packet["flow_bars"][0]["complete"] = False
    config = {"config_version": "local-parity", "code_version": "local-parity"}
    url = require_loopback_native_url()
    http = ClickHouse(url)
    try:
        if case == "future":
            control_http = await engine.evaluate(http, deepcopy(control_packet), config)
            control_local = await engine.evaluate(native_local, deepcopy(control_packet), config)
            control_expected = original_truth.compute_market_truth(deepcopy(control_packet), config=config)
            control_expected["measurement_context"]["numeric_policy"] = engine.NUMERIC_POLICY
            assert control_local == control_http
            assert_complete(control_local, control_expected)
            with pytest.raises(RuntimeError) as http_refusal:
                await engine.evaluate(http, deepcopy(packet), config)
            assert _http_guard(http_refusal.value, "anchor must precede cutoff")
            with pytest.raises(local._LocalFailure) as local_refusal:
                await engine.evaluate(native_local, deepcopy(packet), config)
            assert local_refusal.value.native_code == 395
            assert local_refusal.value.source_guard == "anchor_must_precede_cutoff"
            return
        expected = original_truth.compute_market_truth(deepcopy(packet), config=config)
        expected["measurement_context"]["numeric_policy"] = engine.NUMERIC_POLICY
        forbid_python_business(monkeypatch)
        via_http = await engine.evaluate(http, deepcopy(packet), config)
        via_local = await engine.evaluate(native_local, deepcopy(packet), config)
        assert via_local == via_http
        assert_complete(via_local, expected)
    finally:
        await http.close()


@pytest.mark.integration
@pytest.mark.process
async def test_real_local_conditions_full_fields_cache_preimages_and_reuse(native_local, original_truth):
    from test_truth_sql_conditions import METRICS, _cache_snapshot, _cached_evaluate, _requirements
    from test_truth_sql_engine import assert_complete

    from tank.analysis import truth_sql_engine as engine
    from tank.clickhouse_market import ClickHouse

    packet = _cache_snapshot()
    marker = "δ\\quoted\\n\n\t\r\0'\"終"
    packet["domain_dependency_fingerprints"] = {"spot": {"head": marker, "order": ["B", "A"]}}
    packet["dependency_fingerprint"] = {"frontier": marker}
    config = {"config_version": marker, "code_version": "local-controls"}
    requirements = _requirements(METRICS)
    local_cache, oracle_cache, baselines, http_cache = {}, {}, {}, {}
    url = require_loopback_native_url()
    http = ClickHouse(url)
    try:
        for _ in range(2):
            actual = await _cached_evaluate(
                native_local,
                original_truth,
                packet,
                requirements,
                local_cache,
                oracle_cache,
                baselines,
                config=config,
            )
            expected = await engine.evaluate_conditions(
                http, deepcopy(packet), requirements, config, cache=http_cache
            )
            assert_complete(actual, expected)
            assert local_cache == http_cache
    finally:
        await http.close()


@pytest.mark.integration
@pytest.mark.process
@pytest.mark.parametrize("amount", ["1e25", "0.0000000000000000000000001"])
async def test_real_local_preserves_financial_range_refusal(native_local, original_truth, amount):
    from decimal import Decimal

    from test_truth_sql_engine import assert_complete
    from test_truth_sql_support import _supply_snapshot

    from tank.analysis import truth_sql_engine as engine
    from tank.clickhouse_market import ClickHouse

    packet = _supply_snapshot()
    wallet = next(row for row in packet["supply"] if row["kind"] == "WALLET_FLOWS")
    wallet["flows"][0]["qty_native"] = "1.25"
    expected = original_truth.compute_market_truth(deepcopy(packet))
    expected["measurement_context"]["numeric_policy"] = engine.NUMERIC_POLICY
    http = ClickHouse(require_loopback_native_url())
    try:
        control_http = await engine.evaluate(http, deepcopy(packet))
        control_local = await engine.evaluate(native_local, deepcopy(packet))
        assert control_local == control_http
        assert_complete(control_http, expected)
        assert_complete(control_local, expected)
        for body in (expected, control_http, control_local):
            quantities = [
                flow["qty"]
                for flow in body["supply"]["flows"]
                if flow.get("wallet_list_commit") == "captured"
            ]
            assert quantities == [Decimal("1.25")]
        wallet["flows"][0]["qty_native"] = amount
        with pytest.raises(RuntimeError) as http_refusal:
            await engine.evaluate(http, deepcopy(packet))
        assert _http_guard(http_refusal.value, "Market Truth decimal input outside native financial range")
        with pytest.raises(local._LocalFailure) as local_refusal:
            await engine.evaluate(native_local, deepcopy(packet))
        assert local_refusal.value.native_code == 395
        assert local_refusal.value.source_guard == "decimal_input_outside_financial_range"
    finally:
        await http.close()


@pytest.mark.integration
@pytest.mark.process
@pytest.mark.parametrize("failure", ["raw_empty", "public_domain", "observations", "nonfinite", "duplicate"])
async def test_real_local_preserves_whole_envelope_refusal_and_cache(native_local, original_truth, failure):
    from test_truth_sql_engine import test_real_native_whole_envelope_refusal_preserves_ordered_cache

    await test_real_native_whole_envelope_refusal_preserves_ordered_cache(
        native_local, original_truth, "spot.classification", ("spot", "W1H"), failure
    )


@pytest.mark.integration
@pytest.mark.process
async def test_real_local_late_spawn_cancellation_reaps_child_and_removes_owned_scratch(
    native_local, monkeypatch
):
    entered, release = asyncio.Event(), asyncio.Event()
    spawn = local._spawn
    children = []
    launches = []

    async def observed(*args, **kwargs):
        child = await spawn(*args, **kwargs)
        children.append(child)
        launches.append(kwargs)
        entered.set()
        await release.wait()
        return child

    monkeypatch.setattr(local, "_spawn", observed)
    request = asyncio.create_task(native_local.rows("SELECT count() FROM numbers(1000000000000)"))
    await entered.wait()
    request.cancel()
    closing = asyncio.create_task(native_local.close())
    await asyncio.sleep(0)
    request.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await request
    await closing
    assert children[0].returncode is not None
    assert not children[0].owns_group
    assert not Path(launches[0]["cwd"]).exists()
    for descriptor in launches[0]["pass_fds"]:
        with pytest.raises(OSError):
            os.fstat(descriptor)
    assert not native_local._requests


@pytest.mark.integration
@pytest.mark.process
async def test_real_local_cancellation_terminates_acknowledged_active_workload(native_local, monkeypatch):
    acknowledged = asyncio.Event()
    spawn = local._spawn
    children = []
    launches = []

    class ObservedOutput:
        def __init__(self, reader):
            self.reader = reader
            self.prefix = bytearray()

        async def read(self, size):
            chunk = await self.reader.read(size)
            if not acknowledged.is_set():
                self.prefix.extend(chunk[: 64 - len(self.prefix)])
                if b"\n" in self.prefix:
                    assert self.prefix.split(b"\n", 1)[0] == b'{"started":0}'
                    acknowledged.set()
            return chunk

    async def observed(*args, **kwargs):
        child = await spawn(*args, **kwargs)
        children.append(child)
        launches.append(kwargs)
        child.stdout = ObservedOutput(child.stdout)
        return child

    monkeypatch.setattr(local, "_spawn", observed)
    request = asyncio.create_task(native_local.rows("SELECT number AS started FROM numbers(1000000000000)"))
    started = asyncio.create_task(acknowledged.wait())
    try:
        await asyncio.wait((request, started), return_when=asyncio.FIRST_COMPLETED)
        assert not request.done(), "native workload exited or failed before active acknowledgement"
        assert acknowledged.is_set()
        (child,) = children
        assert child.owns_group and child.returncode is None
        assert os.waitid(local._P_PIDFD, child._pidfd, os.WEXITED | os.WNOWAIT | os.WNOHANG) is None
        request.cancel()
        closing = asyncio.create_task(native_local.close())
        await asyncio.sleep(0)
        request.cancel()
        with pytest.raises(asyncio.CancelledError):
            await request
        await closing
        assert child.returncode in (-signal.SIGTERM, -signal.SIGKILL)
        assert child._process.returncode == child.returncode
        assert not child.owns_group
        assert child._pidfd is None
        assert child._process.stdout.closed and child._process.stderr.closed
        assert not Path(launches[0]["cwd"]).exists()
        for descriptor in launches[0]["pass_fds"]:
            with pytest.raises(OSError):
                os.fstat(descriptor)
        assert not native_local._requests
    finally:
        started.cancel()
        await asyncio.gather(started, return_exceptions=True)
        await native_local.close()
        await asyncio.gather(request, return_exceptions=True)


@pytest.mark.integration
@pytest.mark.process
async def test_real_local_clean_config_and_environment_do_not_inherit_caller(
    native_local, monkeypatch, tmp_path
):
    (tmp_path / "config.xml").write_text("private-invalid-config")
    (tmp_path / ".clickhouse-local").mkdir()
    (tmp_path / ".clickhouse-local" / "config.xml").write_text("private-invalid-user-config")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("HTTP_PROXY", "http://private-secret.invalid")
    spawn = local._spawn
    launches = []

    async def observed(*args, **kwargs):
        launches.append((args, kwargs))
        return await spawn(*args, **kwargs)

    monkeypatch.setattr(local, "_spawn", observed)
    assert await native_local.rows("SELECT toUInt64(7) AS value") == [{"value": 7}]
    args, kwargs = launches[0]
    assert "--no-system-tables" not in args
    config = Path(args[args.index("--config-file") + 1])
    assert config.parent == Path(kwargs["cwd"])
    assert not config.exists() and not config.parent.exists()
    assert "HTTP_PROXY" not in kwargs["env"]
    assert kwargs["env"]["HOME"] != str(tmp_path)
    assert (tmp_path / "config.xml").read_text() == "private-invalid-config"


@pytest.mark.integration
@pytest.mark.process
async def test_real_local_effective_types_and_request_settings_match_http(native_local):
    from tank.clickhouse_market import ClickHouse

    url = require_loopback_native_url()
    query = (
        "SELECT toTypeName(toDecimal256('1.25', 24)) AS decimal_type, "
        "toTypeName(toDateTime64('2026-01-01 00:00:00', 6, 'UTC')) AS date_type, "
        "toDateTime64('2026-01-01 00:00:00', 6, 'UTC') AS stamp, "
        "toInt64(-123) AS number, toDecimal256('1.25', 24) AS amount, "
        "timezone() AS zone, getSetting('date_time_output_format') AS time_format, "
        "getSetting('output_format_json_quote_64bit_integers') AS quote_int, "
        "getSetting('output_format_json_quote_decimals') AS quote_decimal, "
        "getSetting('short_circuit_function_evaluation') AS short_circuit, "
        "getSetting('enable_analyzer') AS analyzer, "
        "getSetting('date_time_input_format') AS date_input, "
        "getSetting('do_not_merge_across_partitions_select_final') AS final_partitions, "
        "getSetting('use_skip_indexes_if_final') AS final_indexes, "
        "getSetting('max_query_size') AS max_query"
    )
    http = ClickHouse(url)
    try:
        via_local = await native_local.rows(query)
        assert via_local == await http.rows(query)
        assert via_local[0]["decimal_type"] == "Decimal(76, 24)"
        assert via_local[0]["date_type"] == "DateTime64(6, 'UTC')"
        assert via_local[0]["number"] == -123
        assert via_local[0]["amount"] == 1.25
    finally:
        await http.close()


@pytest.mark.integration
@pytest.mark.process
async def test_real_local_invalid_tsv_failure_is_private_and_does_not_return_partial_rows(native_local):
    with pytest.raises(RuntimeError, match="^native local execution failed$"):
        await native_local.rows("SELECT {p:String}", {"p": ("\tprivate-secret\t", "String")})
