#!/usr/bin/env python3
"""Install approved credentials in the dedicated Colima VM, outside the source tree.

Values travel over SSH stdin, never process arguments or installer output.
Existing generated database/encryption/internal keys are preserved on rerun.
"""

import argparse
import json
import os
import secrets
import subprocess
from pathlib import Path

from dotenv import dotenv_values

PROVIDED = {
    "openrouter_api_key": ("TANK_OPENROUTER_API_KEY", "OPENROUTER_API_KEY"),
    "telegram_bot_token": ("TANK_TELEGRAM_TOKEN", "TELEGRAM_BOT_TOKEN"),
    "etherscan_api_key": ("TANK_ETHERSCAN_API_KEY", "ETHERSCAN_API_KEY", "ETHERSCAN"),
}
GENERATED = (
    "postgres_password",
    "tank_db_password",
    "oma_db_password",
    "tank_agent_db_password",
    "tank_replay_agent_db_password",
    "oma_api_key",
    "oma_root_secret",
    "oma_auth_secret",
    "codex_gateway_key",
)
REMOTE = r"""
import json,os,pathlib,sys
payload=json.load(sys.stdin)
root=pathlib.Path('/etc/tank/secrets')
root.mkdir(parents=True,exist_ok=True);root.chmod(0o700)
changed=[]
for name,entry in payload.items():
    if name not in {'postgres_password','tank_db_password','oma_db_password','tank_agent_db_password','tank_replay_agent_db_password','oma_api_key','oma_root_secret','oma_auth_secret','codex_gateway_key','openrouter_api_key','telegram_bot_token','etherscan_api_key'}: raise ValueError('Unknown secret name')
    path=root/name
    if path.is_symlink(): raise ValueError('Secret file must not be a symlink')
    existing=path.read_text().strip() if path.is_file() else None
    value=existing if entry['preserve'] and existing else entry['value']
    if not value: raise ValueError('Required secret is empty')
    if value!=existing:
        temporary=root/('.'+name+'.new')
        fd=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
        with os.fdopen(fd,'w') as stream: stream.write(value+'\n')
        temporary.replace(path);changed.append(name)
    if name=='postgres_password': uid,gid,mode=999,999,0o400
    elif name in {'tank_db_password','oma_db_password','tank_agent_db_password','tank_replay_agent_db_password'}: uid,gid,mode=999,1000,0o440
    else: uid,gid,mode=1000,1000,0o400
    os.chown(path,uid,gid);path.chmod(mode)
print(json.dumps({'status':'INSTALLED','secret_count':len(payload),'changed_names':changed,'root':str(root),'values_logged':False}))
"""


def payload_from_env(path):
    values = {key.casefold(): value for key, value in dotenv_values(path).items() if value}
    payload = {}
    for name, aliases in PROVIDED.items():
        value = next((values[a.casefold()].strip() for a in aliases if values.get(a.casefold())), None)
        if not value:
            raise ValueError(f"Approved credential missing: {name}")
        payload[name] = {"value": value, "preserve": False}
    for name in GENERATED:
        payload[name] = {"value": secrets.token_urlsafe(48), "preserve": True}
    return payload


def install(env_file, profile="tank"):
    payload = payload_from_env(env_file)
    environment = {
        key: os.environ[key] for key in ("PATH", "HOME", "USER", "LOGNAME", "LANG") if key in os.environ
    }
    result = subprocess.run(
        ["colima", "-p", profile, "ssh", "--", "sudo", "python3", "-c", REMOTE],
        input=json.dumps(payload),
        text=True,
        capture_output=True,
        env=environment,
    )
    if result.returncode:
        raise RuntimeError("VM secret installation failed; credential values were not logged")
    return json.loads(result.stdout)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--profile", default="tank")
    args = parser.parse_args()
    print(json.dumps(install(args.env_file, args.profile)))
