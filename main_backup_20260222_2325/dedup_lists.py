"""Output deduplicated lists for tickers_universe.py"""
from tickers_universe import (
    MACRO_REGIME_EXPANSION,
    NASDAQ_NYSE_500,
    NASDAQ_NYSE_500_PART2,
    NASDAQ_NYSE_500_PART3,
)

def dedup(L):
    return list(dict.fromkeys(L))

def fmt(L, width=10):
    lines = []
    row = []
    for t in L:
        row.append('"' + t + '"')
        if len(row) >= width:
            lines.append(",".join(row))
            row = []
    if row:
        lines.append(",".join(row))
    return lines

for name, L in [
    ("MACRO", MACRO_REGIME_EXPANSION),
    ("NASDAQ_500", NASDAQ_NYSE_500),
    ("P2", NASDAQ_NYSE_500_PART2),
    ("P3", NASDAQ_NYSE_500_PART3),
]:
    u = dedup(L)
    print("=== " + name + " (" + str(len(L)) + " -> " + str(len(u)) + ") ===")
    for line in fmt(u):
        print(line)
    print()
