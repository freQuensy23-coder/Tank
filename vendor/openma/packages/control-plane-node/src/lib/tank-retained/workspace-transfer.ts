export const workspaceTransferProgram = String.raw`
import hashlib, json, os, pathlib, re, stat, sys, tarfile
mode, archive, allowed_json, workspace = sys.argv[1:]
allowed = ['/workspace', '/mnt/session/outputs'] + json.loads(allowed_json)
def record(path, name):
    s = path.lstat()
    kind = 'dir' if stat.S_ISDIR(s.st_mode) else 'file' if stat.S_ISREG(s.st_mode) else 'link' if stat.S_ISLNK(s.st_mode) else None
    if kind is None:
        raise RuntimeError('retained_workspace_special_file:' + name)
    target = os.readlink(path) if kind == 'link' else None
    if target is not None:
        resolved = os.path.normpath(os.path.join('/workspace', os.path.dirname(name), target))
        if not any(resolved == root or resolved.startswith(root + '/') for root in allowed):
            raise RuntimeError('retained_workspace_external_symlink:' + name)
    digest = None
    if kind == 'file':
        h = hashlib.sha256()
        with path.open('rb') as stream:
            for chunk in iter(lambda: stream.read(1048576), b''):
                h.update(chunk)
        digest = h.hexdigest()
    return [name, kind, stat.S_IMODE(s.st_mode), s.st_uid, s.st_gid, s.st_mtime_ns, target, digest]
def inventory():
    root = pathlib.Path(workspace)
    if not root.is_dir() or root.is_symlink():
        raise RuntimeError('retained_workspace_missing')
    entries = [record(root, '.')]
    for parent, dirs, files in os.walk(root, followlinks=False):
        for name in sorted(dirs + files):
            p = pathlib.Path(parent) / name
            relative = p.relative_to(root).as_posix()
            if re.fullmatch(r'\.tank-browser/[a-f0-9]{32}/(?:rpc\.sock|profile/Singleton(?:Lock|Socket|Cookie))', relative):
                continue
            entries.append(record(p, relative))
    return sorted(entries)
def digest(entries):
    return hashlib.sha256(json.dumps(entries, separators=(',', ':')).encode()).hexdigest()
if mode == 'export':
    before = inventory()
    fd = os.open(archive, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'wb') as output, tarfile.open(fileobj=output, mode='w', dereference=False) as tar:
        for entry in before:
            name = entry[0]
            path = pathlib.Path(workspace) / name
            info = tar.gettarinfo(str(path), arcname=name)
            info.pax_headers['tank.mtime_ns'] = str(entry[5])
            if entry[1] == 'file':
                info.type = tarfile.REGTYPE
                info.linkname = ''
                info.size = path.stat().st_size
                with path.open('rb') as data:
                    tar.addfile(info, data)
            else:
                tar.addfile(info)
    if before != inventory():
        raise RuntimeError('retained_workspace_changed_during_transfer')
    print(digest(before))
elif mode == 'import':
    root = pathlib.Path(workspace)
    if root.is_symlink() or any(root.iterdir()):
        raise RuntimeError('retained_upgrade_target_not_empty')
    with tarfile.open(archive, 'r') as tar:
        members = tar.getmembers()
        names = set()
        for member in members:
            p = pathlib.PurePosixPath(member.name)
            if p.is_absolute() or '..' in p.parts or member.name in names or not (member.isdir() or member.isfile() or member.issym()):
                raise RuntimeError('retained_workspace_archive_invalid')
            names.add(member.name)
            if member.issym():
                resolved = os.path.normpath(os.path.join('/workspace', os.path.dirname(member.name), member.linkname))
                if not any(resolved == base or resolved.startswith(base + '/') for base in allowed):
                    raise RuntimeError('retained_workspace_external_symlink')
        links = {member.name for member in members if member.issym()}
        if any(any(parent.as_posix() in links for parent in pathlib.PurePosixPath(member.name).parents) for member in members):
            raise RuntimeError('retained_workspace_symlink_parent')
        for member in sorted(members, key=lambda m: (not m.isdir(), len(pathlib.PurePosixPath(m.name).parts), m.name)):
            target = root / member.name
            if member.isdir():
                target.mkdir(exist_ok=True)
            elif member.issym():
                target.symlink_to(member.linkname)
            else:
                with tar.extractfile(member) as source, target.open('xb') as output:
                    for chunk in iter(lambda: source.read(1048576), b''):
                        output.write(chunk)
        for member in reversed(members):
            target = root / member.name
            os.chown(target, member.uid, member.gid, follow_symlinks=False)
            if not member.issym():
                os.chmod(target, member.mode)
            ns = int(member.pax_headers['tank.mtime_ns'])
            os.utime(target, ns=(ns, ns), follow_symlinks=False)
    print(digest(inventory()))
else:
    raise RuntimeError('retained_workspace_transfer_mode_invalid')
`;
