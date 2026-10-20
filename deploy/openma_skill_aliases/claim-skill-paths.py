"""Guest-only stable skill binding claims. CLI always targets the guest root."""

import base64
import hashlib
import json
import os
import re
import stat
import sys
import tempfile
import zipfile
from pathlib import Path
from urllib.parse import quote, unquote

KEYS = {"name", "skill_id", "version", "archive_sha256"}


def validate(binding):
    if not isinstance(binding, dict) or set(binding) != KEYS:
        raise ValueError("Invalid skill binding")
    if any(not isinstance(value, str) for value in binding.values()):
        raise ValueError("Invalid skill binding value")
    if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", binding["name"]) or len(binding["name"]) > 64:
        raise ValueError("Invalid stable skill name")
    if any(not binding[key] or len(binding[key]) > 191 for key in ("skill_id", "version")):
        raise ValueError("Invalid immutable skill identity")
    if not re.fullmatch(r"[0-9a-f]{64}", binding["archive_sha256"]):
        raise ValueError("Invalid skill archive digest")
    if len(json.dumps(binding, ensure_ascii=False, separators=(",", ":")).encode()) > 2048:
        raise ValueError("Skill binding exceeds receipt bounds")


def directories(root, relative, create=False):
    current = root
    for component in Path(relative).parts:
        current = current / component
        try:
            info = current.lstat()
        except FileNotFoundError:
            if not create:
                return
            try:
                current.mkdir()
            except FileExistsError:
                pass
            info = current.lstat()
        if not stat.S_ISDIR(info.st_mode):
            raise ValueError("Stable skill path is not a plain directory")


def receipt_matches(path, binding):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > 2048:
            raise ValueError("Invalid stable skill receipt")
        raw = os.read(fd, 2049)
        if len(raw) > 2048 or json.loads(raw) != binding:
            raise ValueError("Stable skill binding mismatch")
    finally:
        os.close(fd)


def plain_tree(path):
    """A prior guest turn may have edited aliases; never follow its symlinks."""
    if not path.exists():
        return
    pending, count = [path], 0
    while pending:
        current = pending.pop()
        for item in current.iterdir():
            count += 1
            if count > 8192:
                raise ValueError("Stable skill tree exceeds validation bounds")
            mode = item.lstat().st_mode
            if stat.S_ISDIR(mode):
                pending.append(item)
            elif not stat.S_ISREG(mode):
                raise ValueError("Stable skill tree contains a non-plain entry")


def apply(bindings, operation, root=Path("/")):
    if operation not in {"check", "claim"} or not isinstance(bindings, list):
        raise ValueError("Invalid claim request")
    if len(json.dumps(bindings, ensure_ascii=False, separators=(",", ":")).encode()) > 48 * 1024:
        raise ValueError("Skill claim request exceeds bounds")
    names = set()
    for binding in bindings:
        validate(binding)
        if binding["name"] in names:
            raise ValueError("Duplicate stable skill name")
        names.add(binding["name"])
    base = root / "home/user/.skills"
    receipts = base / ".openma-bindings"
    directories(root, "home/user/.skills/.openma-bindings")
    for binding in bindings:
        alias, receipt = base / binding["name"], receipts / (binding["name"] + ".json")
        try:
            receipt_matches(receipt, binding)
        except FileNotFoundError:
            if alias.exists() or alias.is_symlink():
                raise ValueError("Unbound stable skill directory") from None
        directories(root, "home/user/.skills/" + binding["name"])
        plain_tree(alias)
    if operation == "check":
        return
    directories(root, "home/user/.skills/.openma-bindings", create=True)
    for binding in bindings:
        receipt = receipts / (binding["name"] + ".json")
        fd, temporary = tempfile.mkstemp(prefix=".claim-" + binding["name"] + "-", dir=receipts)
        try:
            try:
                raw = json.dumps(binding, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
                if os.write(fd, raw) != len(raw):
                    raise OSError("Short skill binding write")
                os.fsync(fd)
            finally:
                os.close(fd)
            try:
                # Publish only complete durable JSON; link cannot overwrite an
                # existing receipt. The O_EXCL tempfile is owned by this call.
                os.link(temporary, receipt, follow_symlinks=False)
            except FileExistsError:
                receipt_matches(receipt, binding)
            directory_fd = os.open(receipts, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            os.unlink(temporary)
        directories(root, "home/user/.skills/" + binding["name"], create=True)


def parent_fd(root, relative, create):
    """Anchor every component to a directory FD; never follow guest symlinks."""
    parts = relative.split("/")
    if any(part in {"", ".", ".."} or "\0" in part for part in parts):
        raise ValueError("Invalid skill target path")
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for part in parts[:-1]:
            if create:
                try:
                    os.mkdir(part, dir_fd=fd)
                except FileExistsError:
                    pass
            next_fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = next_fd
        return fd, parts[-1]
    except BaseException:
        os.close(fd)
        raise


def staging_path(source, digest):
    if not re.fullmatch(r"[0-9a-f]{64}", digest) or not re.fullmatch(r"/tmp/\.openma-skills-[0-9a-f]{32}/" + digest + r"\.zip", source):
        raise ValueError("Invalid skill batch source")
    return source[1:]


def stage(source, digest, root=Path("/")):
    relative = staging_path(source, digest)
    temporary_root, directory = relative.rsplit("/", 1)[0].rsplit("/", 1)
    parent, leaf = parent_fd(root, temporary_root + "/" + directory, False)
    try:
        os.mkdir(leaf, 0o700, dir_fd=parent)
        try:
            owned = os.open(leaf, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
            try:
                fd = os.open(Path(relative).name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=owned)
                os.close(fd)
            finally:
                os.close(owned)
        except BaseException:
            os.rmdir(leaf, dir_fd=parent)
            raise
    finally:
        os.close(parent)


def discard(source, digest, root=Path("/")):
    relative = staging_path(source, digest)
    try:
        parent, leaf = parent_fd(root, relative, False)
    except FileNotFoundError:
        return
    try:
        info = os.stat(leaf, dir_fd=parent, follow_symlinks=False)
        if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600:
            raise ValueError("Skill staging file changed")
        os.unlink(leaf, dir_fd=parent)
        remove_staging_directory(root, relative, parent)
    finally:
        os.close(parent)


def remove_staging_directory(root, relative, owned):
    directory = str(Path(relative).parent)
    parent, leaf = parent_fd(root, directory, False)
    try:
        current, original = os.stat(leaf, dir_fd=parent, follow_symlinks=False), os.fstat(owned)
        if (current.st_dev, current.st_ino) != (original.st_dev, original.st_ino):
            raise ValueError("Skill staging directory changed")
        os.rmdir(leaf, dir_fd=parent)
    finally:
        os.close(parent)


def materialize(source, digest, root=Path("/")):
    """Unpack only a SHA-bound host batch after all immutable claims succeed."""
    staging_path(source, digest)
    source_parent, source_name = parent_fd(root, source[1:], False)
    source_info = None
    try:
        if stat.S_IMODE(os.fstat(source_parent).st_mode) != 0o700:
            raise ValueError("Skill staging directory changed")
        fd = os.open(source_name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=source_parent)
        with os.fdopen(fd, "rb") as archive_file:
            source_info = os.fstat(archive_file.fileno())
            if not stat.S_ISREG(source_info.st_mode) or stat.S_IMODE(source_info.st_mode) != 0o600 or source_info.st_size > 56 * 1024 * 1024:
                raise ValueError("Skill batch source exceeds bounds")
            actual = hashlib.file_digest(archive_file, "sha256").hexdigest()
            if actual != digest:
                raise ValueError("Skill batch digest mismatch")
            archive_file.seek(0)
            with zipfile.ZipFile(archive_file) as archive:
                infos = archive.infolist()
                if not 2 <= len(infos) <= 501 or len({item.filename for item in infos}) != len(infos):
                    raise ValueError("Skill batch entries exceed bounds")
                if any(item.compress_type != zipfile.ZIP_STORED or item.flag_bits & 1 for item in infos):
                    raise ValueError("Skill batch encoding is invalid")
                header = archive.getinfo("__tank_skill_batch__.json")
                if header.file_size > 2 * 1024 * 1024:
                    raise ValueError("Skill batch manifest exceeds bounds")
                manifest = json.loads(archive.read(header))
                if not isinstance(manifest, dict) or set(manifest) != {"version", "scopes"} or manifest["version"] != 1:
                    raise ValueError("Invalid skill batch manifest")
                if not isinstance(manifest["scopes"], list) or not 1 <= len(manifest["scopes"]) <= 500:
                    raise ValueError("Invalid skill batch scopes")
                roots = []
                for scope in manifest["scopes"]:
                    if not isinstance(scope, dict) or set(scope) != {"binding", "canonical_root"}:
                        raise ValueError("Invalid skill batch scope")
                    binding, canonical = scope["binding"], scope["canonical_root"]
                    validate(binding)
                    prefix = "/workspace/.openma/skills/" + quote(binding["skill_id"], safe="~()*!.'-") + "/"
                    if not isinstance(canonical, str) or not canonical.startswith(prefix) or not canonical.endswith("/"):
                        raise ValueError("Invalid canonical skill root")
                    requested = canonical[len(prefix):-1]
                    if not requested or len(unquote(requested)) > 191 or quote(unquote(requested), safe="~()*!.'-") != requested:
                        raise ValueError("Invalid requested skill version")
                    alias = "/home/user/.skills/" + binding["name"] + "/"
                    directories(root, "home/user/.skills/.openma-bindings")
                    receipt_matches(root / "home/user/.skills/.openma-bindings" / (binding["name"] + ".json"), binding)
                    roots.extend([canonical[1:], alias[1:]])
                files, total = [], 0
                for item in infos:
                    if item.filename == header.filename:
                        continue
                    name = item.filename
                    if not any(name.startswith(prefix) for prefix in roots) or len(name) > 5120 or "\\" in name:
                        raise ValueError("Skill batch target is outside its binding")
                    if item.file_size > 32 * 1024 * 1024 or item.is_dir() or any(p in {"", ".", ".."} or "\0" in p for p in name.split("/")):
                        raise ValueError("Invalid skill batch file")
                    total += item.file_size
                    if total > 48 * 1024 * 1024:
                        raise ValueError("Skill batch contents exceed bounds")
                    # Read and CRC-check the entire bounded batch before any target changes.
                    files.append((name, archive.read(item)))
                    directories(root, str(Path(name).parent))
                    try:
                        info = (root / name).lstat()
                        if not stat.S_ISREG(info.st_mode):
                            raise ValueError("Skill target is not a regular file")
                    except FileNotFoundError:
                        pass
                for name, content in files:
                    parent, leaf = parent_fd(root, name, True)
                    temporary = None
                    try:
                        try:
                            prior = os.stat(leaf, dir_fd=parent, follow_symlinks=False)
                            if not stat.S_ISREG(prior.st_mode):
                                raise ValueError("Skill target is not a regular file")
                        except FileNotFoundError:
                            prior = None
                        # The guest process owns only its unique adjacent temporary.
                        for _ in range(16):
                            candidate = ".openma-skill-" + os.urandom(12).hex()
                            try:
                                target = os.open(candidate, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o666, dir_fd=parent)
                            except FileExistsError:
                                continue
                            temporary = candidate
                            break
                        else:
                            raise ValueError("Cannot allocate skill staging file")
                        try:
                            if prior is not None:
                                os.fchmod(target, stat.S_IMODE(prior.st_mode))
                            view = memoryview(content)
                            while view:
                                written = os.write(target, view)
                                if not written:
                                    raise OSError("Short skill file write")
                                view = view[written:]
                            os.fsync(target)
                        finally:
                            os.close(target)
                        os.replace(temporary, leaf, src_dir_fd=parent, dst_dir_fd=parent)
                        temporary = None
                        os.fsync(parent)
                    finally:
                        if temporary is not None:
                            os.unlink(temporary, dir_fd=parent)
                        os.close(parent)
    finally:
        try:
            if source_info is not None:
                current = os.stat(source_name, dir_fd=source_parent, follow_symlinks=False)
                if (current.st_dev, current.st_ino) == (source_info.st_dev, source_info.st_ino):
                    os.unlink(source_name, dir_fd=source_parent)
                    remove_staging_directory(root, source[1:], source_parent)
        finally:
            os.close(source_parent)


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] in {"stage", "materialize", "discard"}:
        operation = sys.argv[1]
        {"stage": stage, "materialize": materialize, "discard": discard}[operation](sys.argv[2], sys.argv[3])
        print({"stage": "TANK_STABLE_SKILL_STAGE_OK", "materialize": "TANK_STABLE_SKILL_FILES_OK", "discard": "TANK_STABLE_SKILL_DISCARD_OK"}[operation])
    else:
        if len(sys.argv) != 3 or len(sys.argv[2]) > 64 * 1024:
            raise SystemExit("Invalid bounded skill claim request")
        payload = base64.b64decode(sys.argv[2], validate=True)
        if len(payload) > 48 * 1024:
            raise SystemExit("Skill claim request exceeds bounds")
        apply(json.loads(payload), sys.argv[1])
        print("TANK_STABLE_SKILL_BINDINGS_OK")
