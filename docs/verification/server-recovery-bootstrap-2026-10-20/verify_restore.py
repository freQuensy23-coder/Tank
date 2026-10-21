"""One-off read-only PostgreSQL16 restore proof. No runtime or schema mutation."""
import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path

MASK = (1 << 256) - 1
SAFE_OMA_COLUMNS = {"id", "created_ms", "updated_ms"}
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def add(state, row, indices):
    state["rows"] += 1
    if indices is None:
        payload = row
    else:
        values = row.rstrip(b"\n").split(b"\t")
        payload = b"\t".join(values[index] for index in indices) + b"\n"
    value = int.from_bytes(hashlib.sha256(payload).digest(), "big")
    state["sum"] = (state["sum"] + value) & MASK
    state["xor"] ^= value


def finish(state):
    return {
        "rows": state["rows"],
        "fingerprint": hashlib.sha256(
            f'{state["rows"]}:{state["sum"]:064x}:{state["xor"]:064x}'.encode()
        ).hexdigest(),
    }


def read_dump(path, oma=False):
    process = subprocess.Popen(
        ["pg_restore", "--data-only", "--file", "-", str(path)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    tables = {}
    sequences = {}
    active = None
    for line in process.stdout:
        if active is None:
            sequence = re.fullmatch(rb"SELECT pg_catalog.setval\('([^\']+)', (-?[0-9]+), (true|false)\);\n", line)
            if sequence:
                name = sequence[1].decode().replace('"', "")
                assert all(IDENTIFIER.fullmatch(x) for x in name.split(".")), "Unsupported sequence"
                sequences[name] = [int(sequence[2]), sequence[3] == b"true"]
            if not line.startswith(b"COPY "):
                continue
            raw_table, raw_columns = line[5:].split(b" (", 1)
            table = raw_table.decode().replace('"', "")
            assert all(IDENTIFIER.fullmatch(x) for x in table.split(".")), "Unsupported table identifier"
            columns = [x.strip().strip(b'"').decode() for x in raw_columns.split(b") FROM stdin;", 1)[0].split(b",")]
            assert all(IDENTIFIER.fullmatch(x) for x in columns), "Unsupported column identifier"
            selected = [index for index, name in enumerate(columns) if name in SAFE_OMA_COLUMNS] if oma else None
            active = {"table": table, "columns": columns, "indices": selected, "rows": 0, "sum": 0, "xor": 0}
            continue
        if line.rstrip(b"\r\n") == b"\\.":
            table = active.pop("table")
            assert table not in tables, "Duplicate COPY table"
            tables[table] = {
                "columns": active["columns"],
                "hash_columns": [active["columns"][index] for index in active["indices"]] if oma else active["columns"],
                **finish(active),
            }
            active = None
        else:
            add(active, line, active["indices"])
    process.stderr.read()  # Do not disclose arbitrary data-bearing diagnostics.
    assert process.wait() == 0 and active is None, "Dump stream failed"
    assert tables, "No COPY tables found"
    return tables, sequences


def verify_live(tables, sequences, oma=False):
    import psycopg
    from psycopg import sql
    from tank.settings import Settings
    settings = Settings()
    url = settings.openma_database_url if oma else settings.database_url
    mismatches = []
    with psycopg.connect(url, autocommit=True) as connection:
        connection.execute("SET TimeZone='UTC'")
        connection.execute("SET DateStyle='ISO, MDY'")
        connection.execute("SET IntervalStyle='postgres'")
        connection.execute("SET extra_float_digits=3")
        connection.execute("SET default_transaction_read_only=on")
        for table, expected in tables.items():
            schema, name = table.split(".")
            columns = expected["hash_columns"] if oma else expected["columns"]
            state = {"rows": 0, "sum": 0, "xor": 0}
            if not columns:
                count = connection.execute(sql.SQL("SELECT count(*) FROM ONLY {}.{}").format(
                    sql.Identifier(schema), sql.Identifier(name)
                )).fetchone()[0]
                actual = {"rows": count}
                matched = count == expected["rows"]
            else:
                query = sql.SQL("COPY (SELECT {} FROM ONLY {}.{}) TO STDOUT").format(
                    sql.SQL(",").join(map(sql.Identifier, columns)),
                    sql.Identifier(schema), sql.Identifier(name),
                )
                with connection.cursor() as cursor:
                    with cursor.copy(query) as copy:
                        pending = b""
                        for block in copy:
                            pending += bytes(block)
                            while b"\n" in pending:
                                row, pending = pending.split(b"\n", 1)
                                add(state, row + b"\n", None)
                        assert not pending, "Incomplete COPY row"
                actual = finish(state)
                matched = actual == {"rows": expected["rows"], "fingerprint": expected["fingerprint"]}
            if not matched:
                mismatches.append({"table": table, "row_count_matches": actual["rows"] == expected["rows"]})
        for sequence, expected in sequences.items():
            schema, name = sequence.split(".")
            actual = connection.execute(sql.SQL("SELECT last_value,is_called FROM {}.{}").format(
                sql.Identifier(schema), sql.Identifier(name)
            )).fetchone()
            if list(actual) != expected:
                mismatches.append({"sequence": sequence, "state_matches": False})
    return mismatches


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dump", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--oma", action="store_true")
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    tables, sequences = read_dump(args.dump, args.oma)
    mismatches = verify_live(tables, sequences, args.oma) if args.verify else None
    report = {
        "mode": "READ_ONLY_PG16_COPY_MULTISET_PROOF",
        "scope": "OMA: all table counts, id/created_ms/updated_ms only; no credential/payload fingerprints" if args.oma else "Tank: full original COPY rows",
        "tables": tables,
        "sequences": sequences,
        "verified_against_live": args.verify,
        "mismatches": mismatches,
        "passed": mismatches == [] if args.verify else None,
        "row_contents_disclosed": False,
    }
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    args.report.chmod(0o600)
    print(json.dumps({"tables": len(tables), "verified": args.verify, "passed": report["passed"]}))
    if mismatches:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
