"""Extract the index.qmd python chunk and run it, emulating the Quarto env."""
import json
import os
import re
import sys
import tempfile

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
src = open(os.path.join(REPO, "index.qmd"), encoding="utf-8").read()
code = re.search(r"```\{python\}\n(.*?)```", src, re.S).group(1)

outdir = tempfile.mkdtemp(prefix="dashrun_")
os.chdir(outdir)
g = {"__name__": "__main__"}
exec(compile(code, "index.qmd python chunk", "exec"), g)

d = json.load(open(os.path.join(outdir, "events.json")))
print("rows:", len(d))
r = d[0]
print("keys:", sorted(r.keys()))
print("lat/lon types:", type(r["lat"]).__name__, type(r["lon"]).__name__)
print("sample sessions:", r["sessions"][:3])
nonnull_lat = sum(1 for e in d if e["lat"] is not None)
with_sessions = sum(1 for e in d if e["sessions"])
free = sum(1 for e in d if e["is_free"])
print("nonnull lat:", nonnull_lat, "| with sessions:", with_sessions, "| is_free:", free)
types = sorted({e["event_type"] for e in d if e["event_type"]})
print("event types:", types)
