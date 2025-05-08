# Stream‑duplicates a small CSV until it reaches --target rows.
# Uses <1 GB RAM even for 200 million rows.
import argparse, pandas as pd, math, pathlib, tqdm, csv

parser = argparse.ArgumentParser()
parser.add_argument("--src", required=True)
parser.add_argument("--target", type=int, required=True)
args = parser.parse_args()

src_rows = sum(1 for _ in open(args.src, "r", encoding="utf8")) - 1  # minus header
if src_rows <= 0:
    raise SystemExit("source CSV has no rows")

times = args.target // src_rows
remainder = args.target % src_rows

out = pathlib.Path("transactions_upsampled.csv").resolve()
print(f"Writing {args.target:,} rows to {out} …")

with open(args.src, newline="", encoding="utf8") as fsrc, open(out, "w", newline="", encoding="utf8") as fdst:
    reader = csv.reader(fsrc)
    writer = csv.writer(fdst)
    header = next(reader)
    writer.writerow(header)

# stream‑append full copies
for _ in tqdm.tqdm(range(times), desc="blocks"):
    df = pd.read_csv(args.src, dtype=str)
    df.to_csv(out, mode="a", header=False, index=False)

if remainder:
    df = pd.read_csv(args.src, nrows=remainder, dtype=str)
    df.to_csv(out, mode="a", header=False, index=False)

print(f"wrote {args.target:,} rows → {out}")