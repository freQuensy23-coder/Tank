import asyncio
import ctypes
import fcntl
import os
import re
import signal
import subprocess
import sys
import tempfile
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path

from tank.clickhouse_market import _REQUEST_SETTINGS, _json_lines, _parameter_plan

_MAX_INPUT_BYTES = 256 << 20
_MAX_PARAMETERS = 4096
_MAX_STDOUT_BYTES = 256 << 20
_MAX_STDERR_BYTES = 1 << 20
_TERMINATE_GRACE_SECONDS = 0.25
_MFD_CLOEXEC = 0x0001
_MFD_ALLOW_SEALING = 0x0002
_F_ADD_SEALS = 1033
_INPUT_SEALS = 0x0008 | 0x0004 | 0x0002 | 0x0001
_P_PIDFD = getattr(os, "P_PIDFD", 3) if sys.platform == "linux" else None
_SOURCE_GUARDS = (
    (b"anchor must precede cutoff", "anchor_must_precede_cutoff"),
    (b"Market Truth decimal input outside native financial range", "decimal_input_outside_financial_range"),
)


def _failure_identity(stderr, returncode):
    if len(stderr) > _MAX_STDERR_BYTES or not 0 < returncode <= 255 or stderr.count(b"Code:") != 1:
        return None, None
    header = re.match(rb"\ACode: ([1-9][0-9]{0,8})\. DB::Exception: ", stderr)
    if header is None:
        return None, None
    code = int(header[1])
    if returncode != (code % 256 or 1):
        return None, None
    guard = None
    if code == 395 and re.search(
        rb" \(FUNCTION_THROW_IF_VALUE_IS_NON_ZERO\)(?: \(version [0-9.]+ \(official build\)\))?\n?\Z",
        stderr,
    ):
        message = stderr[header.end():]
        for prefix, label in _SOURCE_GUARDS:
            if message.startswith((prefix + b": ", prefix + b". ")):
                guard = label
                break
    return code, guard


class _LocalFailure(RuntimeError):
    def __init__(self, message, *, native_code=None, source_guard=None):
        super().__init__(message)
        self.native_code = native_code if type(native_code) is int and 0 < native_code < 1_000_000_000 else None
        self.source_guard = source_guard if self.native_code == 395 and type(source_guard) is str and source_guard in {
            label for _, label in _SOURCE_GUARDS
        } else None


def _binary():
    binary = Path(sys.executable).with_name("clickhouse")
    if not binary.is_absolute() or not binary.is_file() or not os.access(binary, os.X_OK):
        raise _LocalFailure("native local binary unavailable")
    return binary


def _libc_function(name, argtypes):
    if sys.platform != "linux":
        raise _LocalFailure("native local primitives unavailable")
    try:
        function = getattr(ctypes.CDLL(None, use_errno=True), name)
    except (AttributeError, OSError):
        raise _LocalFailure("native local primitives unavailable") from None
    function.argtypes = argtypes
    function.restype = ctypes.c_int
    return function


def _memfd_create():
    if sys.platform != "linux":
        raise _LocalFailure("native local primitives unavailable")
    create = getattr(os, "memfd_create", None)
    if create is not None:
        return create("tank-native-input", _MFD_CLOEXEC | _MFD_ALLOW_SEALING)
    create = _libc_function("memfd_create", (ctypes.c_char_p, ctypes.c_uint))
    descriptor = create(b"tank-native-input", _MFD_CLOEXEC | _MFD_ALLOW_SEALING)
    if descriptor < 0:
        raise OSError(ctypes.get_errno(), "native local primitives unavailable")
    return descriptor


def _pidfd_open(pid):
    if sys.platform != "linux":
        raise _LocalFailure("native local process ownership unavailable")
    create = getattr(os, "pidfd_open", None)
    if create is not None:
        return create(pid)
    create = _libc_function("pidfd_open", (ctypes.c_int, ctypes.c_uint))
    descriptor = create(pid, 0)
    if descriptor < 0:
        raise OSError(ctypes.get_errno(), "native local process ownership unavailable")
    return descriptor


def _memfd(data):
    descriptor = _memfd_create()
    try:
        view = memoryview(data)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("native local input write failed")
            view = view[written:]
        os.lseek(descriptor, 0, os.SEEK_SET)
        fcntl.fcntl(descriptor, _F_ADD_SEALS, _INPUT_SEALS)
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


@dataclass
class _Inputs:
    fds: tuple[int, int]
    table: str
    structure: str
    closed: bool = False

    def close(self):
        if not self.closed:
            self.closed = True
            for descriptor in self.fds:
                os.close(descriptor)


def _prepare(query, params):
    if len(query) > _MAX_INPUT_BYTES or len(query.encode()) > _MAX_INPUT_BYTES:
        raise ValueError("native local input limit exceeded")
    if len(params or {}) > _MAX_PARAMETERS:
        raise ValueError("native local input limit exceeded")
    total = 0
    for value, _ in (params or {}).values():
        if not isinstance(value, str):
            raise ValueError("native local parameter unsupported")
        if len(value) > _MAX_INPUT_BYTES:
            raise ValueError("native local input limit exceeded")
        total += len(value.encode()) + 1
        if total > _MAX_INPUT_BYTES:
            raise ValueError("native local input limit exceeded")
    plan = _parameter_plan(query, params, bind_all=True)
    statement = (plan.query + " FORMAT JSONEachRow").encode()
    if len(statement) > _MAX_INPUT_BYTES or len(plan.data) > _MAX_INPUT_BYTES:
        raise ValueError("native local input limit exceeded")
    query_fd = _memfd(statement)
    try:
        data_fd = _memfd(plan.data)
    except BaseException:
        os.close(query_fd)
        raise
    return _Inputs((query_fd, data_fd), plan.table, plan.structure)


async def _settle(task):
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
    return task.result(), cancelled


async def _drain(reader, limit, overflow, *, keep):
    body = bytearray()
    count = 0
    while chunk := await reader.read(65536):
        count += len(chunk)
        if count > limit:
            overflow.set()
        elif keep:
            body.extend(chunk)
    return bytes(body)


class _OwnedProcess:
    def __init__(self, process, pidfd):
        self._process = process
        self.pid = process.pid
        self._pidfd = pidfd
        self._loop = asyncio.get_running_loop()
        self._exit = self._loop.create_future()
        self._transports = []
        self.stdout = asyncio.StreamReader()
        self.stderr = asyncio.StreamReader()
        self.owns_group = True
        self.returncode = None

    def _exited(self):
        if self._pidfd is None:
            return
        self._loop.remove_reader(self._pidfd)
        try:
            status = os.waitid(_P_PIDFD, self._pidfd, os.WEXITED | os.WNOWAIT)
            if status is None:
                raise _LocalFailure("native local process ownership unavailable")
            self.returncode = status.si_status if status.si_code == os.CLD_EXITED else -status.si_status
            self._exit.set_result(self.returncode)
        except Exception as error:
            self.owns_group = False
            self._exit.set_exception(error)

    async def wait(self):
        return await asyncio.shield(self._exit)

    def reap(self):
        self.owns_group = False
        if self._pidfd is None:
            return
        try:
            self.returncode = self._process.wait()
        finally:
            self._loop.remove_reader(self._pidfd)
            os.close(self._pidfd)
            self._pidfd = None

    def close_readers(self):
        for transport in self._transports:
            transport.close()
        for pipe in (self._process.stdout, self._process.stderr):
            pipe.close()


def _start_process(arguments, kwargs):
    if signal.getsignal(signal.SIGCHLD) != signal.SIG_DFL or _P_PIDFD is None:
        raise _LocalFailure("native local process ownership unavailable")
    process = subprocess.Popen(arguments, **kwargs, bufsize=0)
    pidfd = None
    try:
        pidfd = _pidfd_open(process.pid)
        os.waitid(_P_PIDFD, pidfd, os.WEXITED | os.WNOWAIT | os.WNOHANG)
    except BaseException as error:
        if not isinstance(error, (ChildProcessError, ProcessLookupError)):
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
        process.wait()
        if pidfd is not None:
            os.close(pidfd)
        for pipe in (process.stdout, process.stderr):
            if pipe is not None:
                pipe.close()
        raise
    return process, pidfd


async def _spawn(*arguments, **kwargs):
    process, pidfd = await asyncio.to_thread(_start_process, arguments, kwargs)
    child = _OwnedProcess(process, pidfd)
    try:
        child._loop.add_reader(pidfd, child._exited)
        for name in ("stdout", "stderr"):
            reader = getattr(child, name)
            transport, _ = await child._loop.connect_read_pipe(
                lambda reader=reader: asyncio.StreamReaderProtocol(reader), getattr(process, name)
            )
            child._transports.append(transport)
        return child
    except BaseException:
        _signal_group(child, signal.SIGKILL)
        try:
            await asyncio.to_thread(os.waitid, _P_PIDFD, pidfd, os.WEXITED | os.WNOWAIT)
        finally:
            child.reap()
            child.close_readers()
        raise


def _signal_group(process, sig):
    if process.owns_group:
        with suppress(ProcessLookupError):
            os.killpg(process.pid, sig)


class ReplayNativeRows:
    def __init__(self):
        self._closed = False
        self._requests = set()

    async def rows(self, query, params=None):
        if self._closed:
            raise RuntimeError("native local provider closed")
        task = asyncio.create_task(self._run(query, params))
        self._requests.add(task)
        try:
            return await task
        finally:
            self._requests.discard(task)

    async def close(self):
        self._closed = True
        requests = tuple(self._requests)
        for task in requests:
            task.cancel()
        if requests:
            _, cancelled = await _settle(asyncio.ensure_future(asyncio.gather(*requests, return_exceptions=True)))
            self._requests.difference_update(requests)
            if cancelled:
                raise asyncio.CancelledError

    async def _run(self, query, params):
        preparation = None
        inputs = None
        directory = None
        spawn = None
        process = None
        waiter = None
        drains = []
        completion = None
        overflow_wait = None
        overflow = asyncio.Event()

        def readers(child):
            return [
                asyncio.create_task(_drain(child.stdout, _MAX_STDOUT_BYTES, overflow, keep=True)),
                asyncio.create_task(_drain(child.stderr, _MAX_STDERR_BYTES, overflow, keep=True)),
            ]

        async def cleanup():
            nonlocal inputs, process, drains, waiter
            try:
                if preparation is not None and inputs is None:
                    with suppress(Exception):
                        inputs = await preparation
                if spawn is not None and process is None:
                    with suppress(Exception):
                        process = await spawn
                if process is not None:
                    if not drains:
                        drains = readers(process)
                    if waiter is None:
                        waiter = asyncio.create_task(process.wait())
                    try:
                        if process.returncode is None:
                            _signal_group(process, signal.SIGTERM)
                            try:
                                async with asyncio.timeout(_TERMINATE_GRACE_SECONDS):
                                    await asyncio.shield(waiter)
                            except TimeoutError:
                                _signal_group(process, signal.SIGKILL)
                        if any(not task.done() for task in drains):
                            _signal_group(process, signal.SIGKILL)
                        await waiter
                    finally:
                        process.reap()
                        process.close_readers()
                        for task in drains:
                            if not task.done():
                                task.cancel()
                        await asyncio.gather(*drains, return_exceptions=True)
                if completion is not None:
                    await asyncio.gather(completion, return_exceptions=True)
            finally:
                if overflow_wait is not None:
                    overflow_wait.cancel()
                    await asyncio.gather(overflow_wait, return_exceptions=True)
                try:
                    if inputs is not None:
                        inputs.close()
                finally:
                    if directory is not None:
                        directory.cleanup()

        try:
            binary = _binary()
            preparation = asyncio.create_task(asyncio.to_thread(_prepare, query, params))
            inputs = await asyncio.shield(preparation)
            directory = tempfile.TemporaryDirectory(prefix="tank-native-")
            root = str(Path(directory.name).absolute())
            query_fd, data_fd = inputs.fds
            arguments = [
                str(binary),
                "local",
                "--config-file",
                str(Path(root) / "no-inherited-config.xml"),
                "--queries-file",
                f"/proc/self/fd/{query_fd}",
                *(f"--{name}={value}" for name, value in _REQUEST_SETTINGS.items()),
            ]
            if inputs.structure:
                arguments.extend([
                    "--table", inputs.table,
                    "--structure", inputs.structure,
                    "--input-format", "TabSeparated",
                    "--file", f"/proc/self/fd/{data_fd}",
                ])
            spawn = asyncio.create_task(_spawn(
                *arguments,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                pass_fds=inputs.fds,
                cwd=root,
                env={"HOME": root, "TMPDIR": root, "TZ": "UTC", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"},
                start_new_session=True,
            ))
            process = await asyncio.shield(spawn)
            drains = readers(process)
            waiter = asyncio.create_task(process.wait())
            completion = asyncio.gather(waiter, *drains)
            overflow_wait = asyncio.create_task(overflow.wait())
            await asyncio.wait((completion, overflow_wait), return_when=asyncio.FIRST_COMPLETED)
            if overflow.is_set():
                raise _LocalFailure("native local output limit exceeded")
            await asyncio.shield(completion)
            code = process.returncode
            body = drains[0].result()
            if code:
                native_code, source_guard = _failure_identity(drains[1].result(), code)
                raise _LocalFailure(
                    "native local execution failed", native_code=native_code, source_guard=source_guard
                )
            if len(body) > 1_000_000:
                decoded = await asyncio.to_thread(_json_lines, body)
            else:
                decoded = _json_lines(body)
            if not all(isinstance(row, dict) for row in decoded):
                raise _LocalFailure("native local result invalid")
            return decoded
        except asyncio.CancelledError:
            raise
        except _LocalFailure:
            raise
        except Exception:
            raise RuntimeError("native local request failed") from None
        finally:
            _, cancelled = await _settle(asyncio.create_task(cleanup()))
            if cancelled:
                raise asyncio.CancelledError
