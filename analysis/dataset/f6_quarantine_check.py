import subprocess
import sys

from common import ROOT, write_csv

QUARANTINED = ROOT / "out" / "dataset" / "reference-q1h"
BOUNDED = ROOT / "out" / "week" / "reference-300"


def main():
    subprocess.run([sys.executable, "-m", "trading.reference", "--ticks", "data/week/ticks", "--out", str(QUARANTINED),
                    "--quarantine-ms", "3600000"], cwd=ROOT, check=True, stdout=subprocess.DEVNULL)
    diff = subprocess.run([sys.executable, "-m", "trading.diff", str(QUARANTINED), str(BOUNDED)], cwd=ROOT,
                          capture_output=True, text=True)
    print(diff.stdout.strip())
    rows = [["left", "reference.py --quarantine-ms 3600000 (no lateness bound)"],
            ["right", "reference.py --late-bound-ms 300 (out/week/reference-300)"],
            ["identical", diff.returncode == 0]]
    rows += [["diff", line] for line in diff.stdout.strip().splitlines()]
    write_csv("f6_quarantine_check", ["measure", "value"], rows)
    sys.exit(diff.returncode)


if __name__ == "__main__":
    main()
