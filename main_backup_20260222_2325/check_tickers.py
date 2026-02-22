"""Check which tickers have no recent price data (delisted/suspended)."""
import yfinance as yf
from tickers_universe import TICKERS

bad = []
for i, t in enumerate(TICKERS):
    try:
        h = yf.Ticker(t).history(period="5d")
        if h is None or h.empty or len(h) == 0:
            bad.append(t)
    except Exception:
        bad.append(t)
    if (i + 1) % 300 == 0:
        print(i + 1, "checked, bad:", len(bad))

with open("delisted_or_suspended.txt", "w", encoding="utf-8") as f:
    f.write(str(len(bad)) + " tickers with no recent data\n")
    for t in sorted(bad):
        f.write(t + "\n")
print("Done. Bad:", len(bad))
print("Written to delisted_or_suspended.txt")
