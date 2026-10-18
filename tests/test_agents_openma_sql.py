"""Pinned OpenMA claim/interrupt SQL regression on an isolated PostgreSQL 16.

Opt in with OPENMA_SQL_TEST_SOURCE_DIR and OPENMA_SQL_TEST_CONTAINER. No
credentials, session models or production tables are read or modified: extracted
SQL runs only against a temporary table that disappears with psql's connection.
"""

import hashlib
import os
import re
import subprocess
from pathlib import Path

import pytest

PINNED_SHA = "a30aec208005cd0514458bf11a79ab0c6a1eea53fcbe65a2d486e02ec6e25a68"
DDL = """CREATE TEMP TABLE tank_oma_claim_test (
workspace_id varchar(191),session_id varchar(191),lane_id varchar(191),id varchar(191),
revision bigint,state text,lease_expires_at_ms bigint,owner_id varchar(191),
attempt_count bigint,max_attempts bigint,deadline_at_ms bigint,admitted_at_ms bigint,
settled_at_ms bigint,interrupt_requested_at_ms bigint,failure text);"""


def sql_statements(source):
    candidate = source.split("private async selectCandidate(", 1)[1].split("private async claimSerial(", 1)[0]
    claim = re.search(r"prepare\(`(.*?)`\)\.bind", candidate, re.DOTALL).group(1)
    claim = re.sub(r'\$\{serial \? `.*?` : ""\}', "", claim, flags=re.DOTALL)
    interrupt = source.split("async requestInterrupt(input:", 1)[1]
    updates = re.findall(r"prepare\(`(.*?)`\)\.bind", interrupt, re.DOTALL)[:3]
    assert len(updates) == 3
    return [claim, *updates]


def numbered(query):
    query = query.replace("managed_session_executions", "tank_oma_claim_test")
    index = 0

    def parameter(_):
        nonlocal index
        index += 1
        return "$" + str(index)

    return re.sub(r"\?", parameter, query)


def run_psql(container, sql):
    return subprocess.run(
        [
            "docker",
            "exec",
            "-u",
            "postgres",
            "-i",
            container,
            "psql",
            "-X",
            "-U",
            "postgres",
            "-d",
            "oma",
            "-At",
            "-v",
            "ON_ERROR_STOP=1",
            "-v",
            "VERBOSITY=verbose",
        ],
        input=sql,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )


@pytest.mark.parametrize("query_index", [0, 1, 2, 3])
def test_pinned_pg_optional_claim_and_interrupt_parameters(query_index):
    root = os.getenv("OPENMA_SQL_TEST_SOURCE_DIR")
    container = os.getenv("OPENMA_SQL_TEST_CONTAINER")
    if not root or not container:
        pytest.skip("Explicit isolated OpenMA source and PostgreSQL container required")
    raw = (Path(root) / "packages/session-runtime-sql/src/coordination.ts").read_bytes()
    assert hashlib.sha256(raw).hexdigest() == PINNED_SHA
    original = sql_statements(raw.decode())[query_index]
    broken = run_psql(container, DDL + "\nPREPARE original AS " + numbered(original) + ";")
    assert broken.returncode != 0 and "42P18" in broken.stderr
    patched = original.replace("? IS NULL", "CAST(? AS VARCHAR(191)) IS NULL")
    assert patched != original
    execution = [
        "EXECUTE patched(NULL,NULL,NULL,NULL,NULL,NULL,100,'worker',100);",
        "EXECUTE patched(100,100,'w','s',NULL,NULL);",
        "EXECUTE patched(100,100,'w','s',100,NULL,NULL);",
        "EXECUTE patched(100,'w','s',100,NULL,NULL);",
    ][query_index]
    sql = DDL + "\nPREPARE patched AS " + numbered(patched) + ";\n" + execution
    if query_index == 0:
        sql += """INSERT INTO tank_oma_claim_test VALUES
('w1','s1','l1','id_a',1,'queued',NULL,NULL,0,3,9999,1,NULL,NULL,NULL),
('w1','s1','l1','id_b',1,'queued',NULL,NULL,0,3,9999,2,NULL,NULL,NULL),
('w1','s2','l2','id_c',1,'queued',NULL,NULL,0,3,9999,3,NULL,NULL,NULL),
('w2','s3','l3','id_d',1,'queued',NULL,NULL,0,3,9999,4,NULL,NULL,NULL);
EXECUTE patched(NULL,NULL,NULL,NULL,NULL,NULL,100,'worker',100);
EXECUTE patched('w1','w1','s2','s2','l2','l2',100,'worker',100);
EXECUTE patched('w2','w2',NULL,NULL,NULL,NULL,100,'worker',100);
"""
    fixed = run_psql(container, sql)
    assert fixed.returncode == 0, fixed.stderr
    if query_index == 0:
        rows = [row for row in fixed.stdout.splitlines() if "|" in row]
        assert rows == ["w1|s1|id_a|1", "w1|s2|id_c|1", "w2|s3|id_d|1"]


@pytest.mark.parametrize("query_index", [0, 1, 2, 3])
def test_pinned_pg_event_interrupt_and_checkpoint_optional_parameters(query_index):
    root = os.getenv("OPENMA_SQL_TEST_SOURCE_DIR")
    container = os.getenv("OPENMA_SQL_TEST_CONTAINER")
    if not root or not container:
        pytest.skip("Explicit isolated OpenMA source and PostgreSQL container required")
    if query_index < 3:
        path = Path(root) / "packages/session-event-store-sql/src/index.ts"
        expected = "b6c3f1c54fd037147c391d4597b91fbd53e71eed134cfe4ffb8287f33ec3bf38"
        raw = path.read_bytes()
        source = raw.decode()
        queries = [
            query
            for query in re.findall(r"prepare\(\s*`([^`]+)`\s*,?\s*\)\s*\.bind", source, re.DOTALL)
            if "? IS NULL" in query
        ]
        assert len(queries) == 3
        original = queries[query_index].replace("managed_sessions", "tank_oma_sessions_test")
        ddl = (
            DDL
            + "CREATE TEMP TABLE tank_oma_sessions_test (workspace_id varchar(191),id varchar(191),revision bigint);"
        )
    else:
        path = Path(root) / "packages/control-plane-node/src/lib/node-managed-workspace-checkpoints.ts"
        expected = "3b96f352b16d1578e1d3f5f2d30082b40200f9b52ba633a6edc116cf91d9bf42"
        raw = path.read_bytes()
        source = raw.decode()
        original = next(
            query
            for query in re.findall(r"prepare\(\s*`([^`]+)`\s*\)\s*\.bind", source, re.DOTALL)
            if "? IS NULL" in query
        )
        current = re.search(r"const current = `([^`]+)`", source).group(1)
        original = original.replace("${current}", current).replace(
            "managed_session_workspace_checkpoints", "tank_oma_checkpoint_test"
        )
        ddl = (
            DDL
            + "ALTER TABLE tank_oma_claim_test ADD attempt_id varchar(191),ADD generation bigint;CREATE TEMP TABLE tank_oma_checkpoint_test (workspace_id varchar(191),session_id varchar(191),candidate_id varchar(191),candidate_json text,published_at_ms bigint,revision bigint);"
        )
    assert hashlib.sha256(raw).hexdigest() == expected
    broken = run_psql(container, ddl + "\nPREPARE original AS " + numbered(original) + ";")
    assert broken.returncode != 0 and "42P18" in broken.stderr
    patched = original.replace("? IS NULL", "CAST(? AS VARCHAR(191)) IS NULL")
    parameters = ",".join(["NULL"] * patched.count("?"))
    fixed = run_psql(
        container,
        ddl + "\nPREPARE patched AS " + numbered(patched) + ";\nEXECUTE patched(" + parameters + ");",
    )
    assert fixed.returncode == 0, fixed.stderr


@pytest.mark.parametrize(
    "relative,expected",
    [
        (
            "packages/agents-store/src/adapters/sql-agent-repo.ts",
            "b3d1190a5eed96c8f3271571fe7b683cdc87062d057ff706788564f0c0e80368",
        ),
        (
            "packages/credentials-store/src/adapters/sql-credential-repo.ts",
            "dfcceff73f74150f7b2d615be0fca638440290118dc667bdbf6a0bd813a9d1d6",
        ),
        (
            "packages/dreams-store/src/adapters/sql-dream-repo.ts",
            "7ad4842c37deec4a3db7606ba57202821c585e8fbbd1e49404884ac3b1951d4a",
        ),
        (
            "packages/environments-store/src/adapters/sql-environment-repo.ts",
            "5a1cfa89a86ec2b8e30668be154ee97815d1127269bf1c01cce583225b8912eb",
        ),
        (
            "packages/evals-store/src/adapters/sql-eval-run-repo.ts",
            "3c1953c0e4c5b4888704cb97e6fe31fd04fdbf0495ee4a8d7a3e6a9bcc44bef4",
        ),
        (
            "packages/files-store/src/adapters/sql-file-repo.ts",
            "d356d9273bc8d79f9c8f650a082d2c84acfb7c04966ab2985d6eb9ced967d017",
        ),
        (
            "packages/memory-store/src/adapters/sql-memory-repo.ts",
            "10fddc992f41b30fb557327caba7b368066d9f66564f98c56a183a39ea52e244",
        ),
        (
            "packages/memory-store/src/adapters/sql-store-repo.ts",
            "e312914049a23b1ee7120e266d881a3855f3b80360180478c40d43f148c5e122",
        ),
        (
            "packages/memory-store/src/adapters/sql-version-repo.ts",
            "21c05b88c4e81ac1f0a35417b679fb46c842e5fac8ab938cc1bcdd3596161634",
        ),
        (
            "packages/sessions-store/src/adapters/sql-session-repo.ts",
            "622290725c218fb54ada8dbb2be32be0bb00c91bd6bf1e8185ddcd36a12c07c2",
        ),
        (
            "packages/vaults-store/src/adapters/sql-vault-repo.ts",
            "840d4d2e9ccef8289068a27cfe7643021d13151831907a5b12713914e04cb921",
        ),
    ],
)
def test_pinned_sql_timestamp_helper_accepts_pg_bigint_strings(relative, expected):
    import json
    import shutil

    root = os.getenv("OPENMA_SQL_TEST_SOURCE_DIR")
    if not root or not shutil.which("node"):
        pytest.skip("Explicit pinned source and Node runtime required")
    raw = (Path(root) / relative).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == expected
    helper = re.search(r"function msToIso\(ms: number\): string \{([^}]+)\}", raw.decode()).group(1)
    original = "function msToIso(ms){" + helper + "}"
    call = '; process.stdout.write(msToIso("1792260359549"));'
    broken = subprocess.run(["node", "-e", original + call], capture_output=True, text=True, check=False)
    assert broken.returncode != 0 and "Invalid time value" in broken.stderr
    patched = original.replace("new Date(ms)", "new Date(Number(ms))")
    script = (
        patched + '; process.stdout.write(JSON.stringify([msToIso("1792260359549"),msToIso(1792260359549)]));'
    )
    fixed = subprocess.run(["node", "-e", script], capture_output=True, text=True, check=False)
    assert fixed.returncode == 0, fixed.stderr
    dates = json.loads(fixed.stdout)
    assert dates == ["2026-10-17T18:05:59.549Z"] * 2


def test_pinned_node_api_key_metadata_timestamps_accept_bigint_strings():
    import shutil

    root = os.getenv("OPENMA_SQL_TEST_SOURCE_DIR")
    if not root or not shutil.which("node"):
        pytest.skip("Explicit pinned source and Node runtime required")
    raw = (Path(root) / "packages/control-plane-node/src/modules/node-runtime.ts").read_bytes()
    assert (
        hashlib.sha256(raw).hexdigest() == "53ffcdf4d9ed514c7130e23d025d6fe2290becb6c6bd2bc349f5a88f59bd28dc"
    )
    old = "created_at: new Date(row.created_at).toISOString(),"
    assert raw.decode().count(old) == 2
    for expr in [old.replace("created_at: ", "").rstrip(",")]:
        source = 'const row={created_at:"1792260359549"};process.stdout.write(' + expr + ");"
        broken = subprocess.run(["node", "-e", source], capture_output=True, text=True, check=False)
        assert broken.returncode != 0 and "Invalid time value" in broken.stderr
        fixed = subprocess.run(
            ["node", "-e", source.replace("new Date(row.created_at)", "new Date(Number(row.created_at))")],
            capture_output=True,
            text=True,
            check=False,
        )
        assert fixed.returncode == 0 and fixed.stdout == "2026-10-17T18:05:59.549Z"
