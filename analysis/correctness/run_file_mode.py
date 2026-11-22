"""Correctness matrix in file mode: every engine against the batch reference, on the same ticks.

Runs, for one tick folder:
  1. trading.reference with --late-bound-ms BOUND (the oracle),
  2. the Python engine (file replay) with --out-of-orderness-ms BOUND,
  3. the Flink job (file source) at each parallelism in --parallelism, if it has been built (make flink-build),
     once with Flink's default periodic watermarks and once with --watermark-every-event,
then trading.diff for each engine against the reference. Works on Windows, macOS and Linux (no make needed).

The log goes to analysis/correctness/results/<label>.txt in the same format as Person 2's
correctness log, so collect_verified.py picks both up.

    python analysis/correctness/run_file_mode.py --ticks data/synthetic/ticks --label synthetic-local
    python analysis/correctness/run_file_mode.py --ticks data/week/ticks --label week-file-mode \\
        --bound-ms 300 --quarantine-ms 3600000
"""
import argparse
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
FLINK = ROOT / "flink-job"
MAIN = "fi.aalto.cse4780.trading.EmaCrossoverJob"


def run(cmd, log):
    started = time.perf_counter()
    proc = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    seconds = time.perf_counter() - started
    if proc.returncode != 0:
        log.write(f"    command failed ({proc.returncode}): {' '.join(map(str, cmd))}\n")
        log.write("    " + (proc.stderr or proc.stdout).strip().replace("\n", "\n    ")[-2000:] + "\n")
    return proc, seconds


def flink_classpath():
    classes, deps = FLINK / "target" / "classes", FLINK / "target" / "classpath.txt"
    if not classes.exists() or not deps.exists():
        return None
    # classpath.txt is written by Maven with the platform's separator
    return os.pathsep.join([str(classes), deps.read_text().strip()])


def java():
    home = os.environ.get("JAVA_HOME")
    if home and (Path(home) / "bin").exists():
        return str(Path(home) / "bin" / "java")
    return shutil.which("java")


def diff(left, right, log):
    # not run(): a failing diff prints sample rows, and only the two summary lines belong in the log
    proc = subprocess.run([sys.executable, "-m", "trading.diff", str(left), str(right)], cwd=ROOT,
                          capture_output=True, text=True)
    for line in proc.stdout.splitlines():
        if line.startswith(("ema", "advisories")):
            log.write(f"    {line}\n")
    return proc.returncode == 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ticks", required=True, help="tick folder from trading.prep (parquet + csv)")
    parser.add_argument("--label", required=True, help="name of the log, e.g. synthetic-local or week-file-mode")
    parser.add_argument("--bound-ms", type=int, default=300, help="out-of-orderness bound, same for all engines")
    parser.add_argument("--quarantine-ms", type=int, default=None,
                        help="also quarantine mis-stamped ticks in reference and Python engine (Flink file mode has "
                             "no system time; the 300 ms bound drops the same ticks on the week)")
    parser.add_argument("--parallelism", default="1,2,4")
    parser.add_argument("--out", default=str(ROOT / "out" / "correctness"))
    parser.add_argument("--skip-python", action="store_true")
    parser.add_argument("--skip-flink", action="store_true")
    parser.add_argument("--skip-every-event", action="store_true",
                        help="only run Flink with its default periodic watermarks, not also with --watermark-every-event")
    args = parser.parse_args(argv)

    out = Path(args.out) / args.label
    out.mkdir(parents=True, exist_ok=True)
    (HERE / "results").mkdir(exist_ok=True)
    log_path = HERE / "results" / f"{args.label}.txt"
    quarantine = ["--quarantine-ms", str(args.quarantine_ms)] if args.quarantine_ms else []
    ok = True
    with log_path.open("w", encoding="utf-8") as log:
        log.write(f"== correctness ({args.label}): file mode vs batch reference, bound {args.bound_ms} ms, "
                  f"ticks {args.ticks}\n")
        ref = out / "reference"
        proc, secs = run([sys.executable, "-m", "trading.reference", "--ticks", args.ticks, "--out", str(ref),
                          "--late-bound-ms", str(args.bound_ms), *quarantine], log)
        if proc.returncode != 0:
            raise SystemExit(f"reference failed, see {log_path}")
        log.write(f"  reference built in {secs:.1f} s\n")

        if not args.skip_python:
            target = out / "python"
            proc, secs = run([sys.executable, "-m", "trading.engine", "--ticks", args.ticks, "--out", str(target),
                              "--out-of-orderness-ms", str(args.bound_ms), *quarantine], log)
            log.write(f"  python-file p=1 exit={proc.returncode}\n")
            ok &= proc.returncode == 0 and diff(target, ref, log)

        if not args.skip_flink:
            cp, jvm = flink_classpath(), java()
            if not cp or not jvm:
                log.write("  flink-file skipped: build it first (make flink-build) and put java on PATH\n")
                ok = False
            modes = [("flink-file", [])]
            if not args.skip_every_event:
                modes.append(("flink-file-ew", ["--watermark-every-event"]))
            for name, extra in modes:
                for p in [int(x) for x in args.parallelism.split(",") if x and cp and jvm]:
                    target = out / f"{name}-p{p}"
                    shutil.rmtree(target, ignore_errors=True)
                    proc, secs = run([jvm, "-cp", cp, MAIN, "--source", "file", "--input", args.ticks, "--out",
                                      str(target), "--out-of-orderness-ms", str(args.bound_ms), "--parallelism",
                                      str(p), *extra], log)
                    log.write(f"  {name} p={p} exit={proc.returncode}\n")
                    ok &= proc.returncode == 0 and diff(target, ref, log)

        tests = subprocess.run([sys.executable, "-m", "unittest", "discover", "-s", "tests", "-t", "."],
                               cwd=ROOT, capture_output=True, text=True)
        summary = [line for line in tests.stderr.splitlines() if line.startswith(("Ran ", "OK", "FAILED"))]
        log.write("\n".join(summary) + "\n")
        ok &= tests.returncode == 0
    print(log_path.read_text(encoding="utf-8"))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
