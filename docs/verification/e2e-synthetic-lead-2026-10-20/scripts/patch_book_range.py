from pathlib import Path

p = Path("src/tank/market/service.py")
s = p.read_text()

old_call = """                range_start = history_start if name == "supply" else recent_start
                rows = await self._range(
                    collection,"""
new_call = """                range_start = history_start if name == "supply" else recent_start
                rows = await (self._book_range if name == "books" else self._range)(
                    collection,"""
assert s.count(old_call) == 1, "call site"
s = s.replace(old_call, new_call)

old_def = """    async def _range(self, collection, field, *, start=None, end=None, filters=None):
        return [
            row
            async for row in self.repo.iter_range(
                collection, field, start=start, end=end, filters=filters, batch_size=100000
            )
        ]
"""
new_def = old_def + '''
    async def _book_range(self, collection, field, *, start=None, end=None, filters=None):
        """One instrument's books, with order-book levels only where Market Truth walks them.

        Slippage walks the levels of the newest integrity-checked book per venue (03 §5.5);
        older rows are read for depth, spread and mid. Two days of BTC levels held ~3.5 GB
        per snapshot, so pages stay small and rows outside the newest BOOK_LEVEL_ROWS drop
        their levels (the slack covers availability-bounded snapshots).
        """
        rows, newest_ok = [], None

        def strip(index):
            if "bids" in rows[index] or "asks" in rows[index]:
                rows[index] = {k: v for k, v in rows[index].items() if k not in ("bids", "asks")}

        async for row in self.repo.iter_range(
            collection, field, start=start, end=end, filters=filters, batch_size=500
        ):
            rows.append(row)
            index = len(rows) - 1
            if row.get("integrity_ok") is True:
                if newest_ok is not None and newest_ok < index - BOOK_LEVEL_ROWS:
                    strip(newest_ok)
                newest_ok = index
            if index >= BOOK_LEVEL_ROWS and index - BOOK_LEVEL_ROWS != newest_ok:
                strip(index - BOOK_LEVEL_ROWS)
        return rows
'''
assert s.count(old_def) == 1, "def"
s = s.replace(old_def, new_def)

anchor = "COLLECTIONS = {"
assert s.count(anchor) == 1, "anchor"
s = s.replace(
    anchor,
    "BOOK_LEVEL_ROWS = 30  # newest book rows per instrument that keep their levels in a snapshot\n" + anchor,
)
p.write_text(s)
print("patched")
