import argparse
import json
import subprocess
import sys

from common import ROOT, write_csv

SAVED = ROOT / "out" / "heartbeat" / "check-week-300.json"
RERUN = ROOT / "out" / "dataset" / "check-week-300.json"
MARGINS = ["0", "300", "500", "700", "1000", "2000"]


def main():
    parser = argparse.ArgumentParser(description="F7: ticks behind the heartbeat watermark per margin")
    parser.add_argument("--rerun", action="store_true", help="recompute with trading.heartbeat_check instead of reading the saved run")
    args = parser.parse_args()
    source = SAVED
    if args.rerun:
        RERUN.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run([sys.executable, "-m", "trading.heartbeat_check", "--ticks", "data/week/ticks", "--bound-ms", "300",
                        "--margins", *MARGINS, "--quarantine-ms", "3600000", "--out", str(RERUN)], cwd=ROOT, check=True)
        source = RERUN
    check = json.loads(source.read_text())
    rows = [["none", check["without_heartbeats"]["event_late"], "", check["without_heartbeats"]["window_late"]]]
    for margin in MARGINS:
        m = check["margins"][margin]
        rows.append([margin, m["event_late"], m["event_late_only_with_heartbeats"], m["window_late"]])
    write_csv("f7_heartbeat_margins",
              ["heartbeat_margin_ms", "ticks_behind_watermark", "only_because_of_heartbeats", "ticks_in_closed_window"], rows)
    print("ticks", check["ticks"], "quarantined", check["quarantined"], "bound_ms", check["params"]["bound_ms"])
    for row in rows:
        print(*row)


if __name__ == "__main__":
    main()
