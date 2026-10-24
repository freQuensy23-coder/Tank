import unittest
from pathlib import Path
from unittest.mock import patch

import public_profile as p


class Tests(unittest.IsolatedAsyncioTestCase):
    def test_real_public_page_has_post_text_and_expected_id(self):
        rows = p.parse_profile(
            (Path(__file__).parent / "fixtures/public-x-profile.html").read_text(),
            "NASA",
        )
        post = next(x for x in rows if x["id"] == "2112959650551173522")
        self.assertEqual(post["author"], "NASA")
        self.assertIn("Jeremy Hansen", post["text"])
        self.assertAlmostEqual(post["publishedTs"], 1792603812.24, places=2)
        with self.assertRaises(RuntimeError):
            p.parse_profile("<title>Log in / X</title>", "NASA")

    async def test_partial_failure_keeps_good_posts_and_reports_account_failure(self):
        def fetch(handle):
            if handle == "broken":
                raise RuntimeError("PUBLIC_PROFILE_HTTP_429")
            return [{"id": "12", "author": handle, "publishedTs": 1}], 123

        with patch.object(p, "fetch_profile", fetch):
            rows, report = await p.collect({"handles": ["good", "broken"]}, set(), 0)
        self.assertEqual(len(rows), 1)
        self.assertEqual(report["accountsRead"], 1)
        self.assertEqual(report["failedAccounts"][0]["handle"], "broken")
        self.assertIsNone(report["error"])
        self.assertFalse(report["reachedHistory"])

    async def test_old_pinned_post_does_not_hide_a_gap_or_cache_regression(self):
        def fetch(handle):
            return [{"id": str(i), "author": handle, "publishedTs": i} for i in [1, 30, 31, 32, 33]], 123

        with patch.object(p, "fetch_profile", fetch):
            rows, report = await p.collect(
                {"handles": ["good"], "_accountState": {"good": {"lastMaxId": "20"}}},
                set(),
                0,
            )
        self.assertEqual(report["coverageGaps"], ["good"])
        with patch.object(p, "fetch_profile", fetch):
            rows, report = await p.collect(
                {"handles": ["good"], "_accountState": {"good": {"lastMaxId": "40"}}},
                set(),
                0,
            )
        self.assertEqual(report["error"], "PUBLIC_SOURCE_UNAVAILABLE")
        self.assertEqual(report["failedAccounts"][0]["error"], "PUBLIC_PROFILE_STALE_SAMPLE")
        self.assertEqual(rows, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
