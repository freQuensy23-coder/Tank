"""Pure parser/projection checks; no server or network/database startup."""
import importlib.util
import io
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("restore_proof", Path(__file__).with_name("verify_restore.py"))
proof = importlib.util.module_from_spec(spec)
spec.loader.exec_module(proof)


def parse(payload, oma=False):
    process = SimpleNamespace(stdout=io.BytesIO(payload), stderr=io.BytesIO(), wait=lambda: 0)
    with patch.object(proof.subprocess, "Popen", return_value=process):
        return proof.read_dump(Path("/never/opened/dump"), oma)


class RestoreProofTests(unittest.TestCase):
    def test_order_independent_but_duplicate_sensitive(self):
        first = b"COPY mkt.rows (id, doc) FROM stdin;\n1\tline\\nline\n2\tvalue\n\\.\n"
        reordered = b"COPY mkt.rows (id, doc) FROM stdin;\n2\tvalue\n1\tline\\nline\n\\.\n"
        duplicated = b"COPY mkt.rows (id, doc) FROM stdin;\n1\tline\\nline\n2\tvalue\n2\tvalue\n\\.\n"
        self.assertEqual(parse(first), parse(reordered))
        self.assertNotEqual(parse(first), parse(duplicated))
        self.assertEqual(parse(first)[0]["mkt.rows"]["rows"], 2)

    def test_oma_excludes_payload_even_for_single_row(self):
        first = b"COPY public.auth (id, ciphertext) FROM stdin;\na\tprivate-one\n\\.\n"
        changed = b"COPY public.auth (id, ciphertext) FROM stdin;\na\tprivate-two\n\\.\n"
        self.assertEqual(parse(first, True), parse(changed, True))
        self.assertNotEqual(parse(first, False), parse(changed, False))
        self.assertEqual(parse(first, True)[0]["public.auth"]["hash_columns"], ["id"])

    def test_sequences_and_incomplete_stream(self):
        payload = b"COPY ops.rows (id) FROM stdin;\n\\.\nSELECT pg_catalog.setval('ops.ids', 17, true);\n"
        self.assertEqual(parse(payload)[1], {"ops.ids": [17, True]})
        with self.assertRaises(AssertionError):
            parse(b"COPY ops.rows (id) FROM stdin;\n1\n")


if __name__ == "__main__":
    unittest.main()
