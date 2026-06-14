"""
Merge public transport stop categories into a unified dataset.

Input:
  - data/public_transport.csv (71,447 records, 6 categories)

Categories:
  - bus_stop (66,316)      -> weight: 0
  - train_station (2,634)   -> weight: 2
  - tram_stop (1,607)       -> weight: 1
  - public_transport (752)  -> weight: 1
  - subway (121)            -> weight: 10
  - train_stop (16)         -> weight: 2

Output:
  - data/public_transport_merged.csv (unified format with weights)
"""
import csv
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
INPUT_CSV = DATA_DIR / "public_transport.csv"
OUTPUT_CSV = DATA_DIR / "public_transport_merged.csv"

CATEGORY_WEIGHTS = {
    "bus_stop": 0,
    "train_station": 2,
    "tram_stop": 1,
    "public_transport": 1,
    "subway": 10,
    "train_stop": 2,
}


def main():
    with open(INPUT_CSV, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    output_rows = []
    for row in rows:
        kind = row.get("poi_kind", "").strip().lower()
        weight = CATEGORY_WEIGHTS.get(kind, 1)  # default weight = 1
        output_rows.append({
            "lat": row["latitude"],
            "lon": row["longitude"],
            "kind": kind,
            "weight": weight,
        })

    fieldnames = ["lat", "lon", "kind", "weight"]
    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)

    # Print category counts
    from collections import Counter
    counts = Counter(r["kind"] for r in output_rows)
    print("Category counts:")
    for kind, count in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"  {kind}: {count}")
    print(f"\nTotal: {len(output_rows)} rows written to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
