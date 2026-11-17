import argparse
import csv
import json
import os
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
import uuid
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from trading import topics
from trading.core import quarantine_sql
from trading.files import default_metadata_events, metadata_changes, tick_files

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ["docker", "compose", "-f", "docker-compose.yml", "-f", "docker-compose.flink.yml"]
JAR = "/opt/job/ema-crossover-flink-1.0.0.jar"
CHECKPOINTS = ROOT / "checkpoints"
CHECKPOINT_DIR = "file:///opt/checkpoints"
GAUGES = ("busyTimeMsPerSecond", "backPressuredTimeMsPerSecond", "idleTimeMsPerSecond", "numRecordsOutPerSecond")
SUBTASK_METRICS = ("accumulateBusyTimeMs", "accumulateBackPressuredTimeMs", "accumulateIdleTimeMs", "numRecordsOut")
ACCUMULATED = {"busy": "accumulateBusyTimeMs", "backpressure": "accumulateBackPressuredTimeMs", "idle": "accumulateIdleTimeMs"}
DONE = ("FINISHED", "FAILED", "CANCELED")
ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def sh(*cmd, env=None, check=True):
    result = subprocess.run(cmd, cwd=ROOT, env=env, capture_output=True, text=True)
    if check and result.returncode != 0:
        sys.exit(f"{' '.join(cmd)} failed:\n{result.stdout}{result.stderr}")
    return result


def rest(base, path):
    with urllib.request.urlopen(base + path, timeout=10) as response:
        return json.load(response)


def cpuset_of(container):
    result = sh("docker", "inspect", container, "--format", "{{.State.Running}} {{.HostConfig.CpusetCpus}}", check=False)
    if result.returncode != 0:
        return None, None
    running, _, cpuset = result.stdout.strip().partition(" ")
    return running == "true", cpuset


def preflight(args):
    running, cpuset = cpuset_of("broker")
    if not running:
        sys.exit("broker container is not running (make kafka-up)")
    if cpuset != args.broker_cpuset:
        sys.exit(f"broker runs on cpuset '{cpuset or 'all'}', expected '{args.broker_cpuset}'")
    if args.expected is not None:
        expected = args.expected
    elif args.ticks and args.quarantine_ms is not None:
        expected = duckdb.connect().execute(f"SELECT count(*) FILTER (WHERE NOT {quarantine_sql(args.quarantine_ms)}) "
                                            "FROM read_parquet(?)", [[str(f) for f in tick_files(args.ticks)]]).fetchone()[0]
    elif args.ticks:
        expected = sum(pq.ParquetFile(f).metadata.num_rows for f in tick_files(args.ticks))
    else:
        sys.exit("give --ticks or --expected so the topic size can be checked")
    if args.enrich and args.expected is None:
        expected += sum(1 for _ in metadata_changes(default_metadata_events(args.ticks)))
    found = topics.count(args.bootstrap, args.topic)
    if found != expected:
        sys.exit(f"topic {args.topic} holds {found} messages, expected {expected} (make bench-preload)")
    return expected


class CpuSampler(threading.Thread):
    def __init__(self, names):
        super().__init__(daemon=True)
        self.names = names
        self.samples = []
        self.proc = subprocess.Popen(["docker", "stats", "--format", "{{.Name}} {{.CPUPerc}}", *names],
                                     stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)

    def run(self):
        for line in self.proc.stdout:
            for name, value in re.findall(r"(\S+) ([\d.]+)%", ANSI.sub("", line)):
                self.samples.append((time.time(), name, float(value)))

    def stop(self):
        self.proc.terminate()
        self.proc.wait()

    def mean(self, start, end):
        result = {}
        for name in self.names:
            values = [v for t, n, v in self.samples if n == name and start <= t <= end]
            result[name] = round(statistics.fmean(values), 1) if values else None
        return result


def start_cluster(args, p, cpuset):
    env = dict(os.environ, TM_SLOTS=str(p), TM_CPUSET=cpuset, BROKER_CPUSET=args.broker_cpuset)
    sh(*COMPOSE, "up", "-d", "--force-recreate", "--no-deps", "jobmanager", "taskmanager", env=env)
    deadline = time.time() + 180
    while True:
        try:
            overview = rest(args.rest, "/overview")
            if overview["taskmanagers"] == 1 and overview["slots-total"] == p and overview["slots-available"] == p:
                break
        except (OSError, ValueError, KeyError):
            pass
        if time.time() > deadline:
            sys.exit(f"Flink cluster did not offer {p} slots within 180 s")
        time.sleep(1)
    placement = {name: cpuset_of(name)[1] for name in ("broker", "jobmanager", "taskmanager")}
    if placement["taskmanager"] != cpuset or placement["jobmanager"] != args.broker_cpuset:
        sys.exit(f"unexpected cpusets {placement}")
    return placement


def stop_cluster():
    sh(*COMPOSE, "rm", "-s", "-f", "jobmanager", "taskmanager", check=False)


def control_args(args):
    extra = []
    if args.subscriptions_topic:
        extra += ["--subscriptions-topic", args.subscriptions_topic]
    if args.enrich:
        extra.append("--enrich")
    return extra


def fault_tolerance_args(args):
    extra = []
    if args.checkpoint_ms:
        extra += ["--checkpoint-ms", str(args.checkpoint_ms), "--checkpoint-dir", args.checkpoint_dir]
        if args.checkpoint_mode:
            extra += ["--checkpoint-mode", args.checkpoint_mode]
    if args.delivery:
        extra += ["--delivery", args.delivery]
    return extra


def submit(args, p, group, ema_topic, adv_topic):
    result = sh("docker", "exec", "jobmanager", "flink", "run", "-d", JAR, "--source", "kafka",
                "--bootstrap", args.job_bootstrap, "--topic", args.topic, "--group", group, "--bounded",
                "--out-of-orderness-ms", str(args.ooo_ms), "--parallelism", str(p),
                "--ema-topic", ema_topic, "--advisories-topic", adv_topic, *control_args(args),
                *fault_tolerance_args(args))
    match = re.search(r"JobID ([0-9a-f]{32})", result.stdout + result.stderr)
    if not match:
        sys.exit(f"no JobID in flink run output:\n{result.stdout}{result.stderr}")
    return match.group(1)


def sample_vertex(args, jid, vertex):
    entry = {"status": vertex["status"]}
    if vertex["status"] != "RUNNING":
        return entry
    base = f"/jobs/{jid}/vertices/{vertex['id']}/subtasks"
    for metric in rest(args.rest, f"{base}/metrics?get={','.join(GAUGES)}&agg=avg,max,sum"):
        entry[metric["id"]] = {k: metric[k] for k in ("avg", "max", "sum") if k in metric}
    subtasks = []
    for index in range(vertex["parallelism"]):
        values = rest(args.rest, f"{base}/{index}/metrics?get={','.join(SUBTASK_METRICS)}")
        subtasks.append({m["id"]: float(m["value"]) for m in values})
    if all(len(s) == len(SUBTASK_METRICS) for s in subtasks):
        entry["subtasks"] = subtasks
    return entry


class TaskManagerKill:
    def __init__(self, args, jid, records, p, cpuset):
        self.args, self.jid, self.records = args, jid, records
        self.env = dict(os.environ, TM_SLOTS=str(p), TM_CPUSET=cpuset, BROKER_CPUSET=args.broker_cpuset)
        self.info = None

    def check(self, sample):
        if self.info is not None:
            try:
                metric = rest(self.args.rest, f"/jobs/{self.jid}/metrics?get=numRestarts")
                if metric:
                    self.info["num_restarts"] = int(float(metric[0]["value"]))
            except (OSError, ValueError, KeyError, IndexError):
                pass
            return
        if sample.get("state") != "RUNNING":
            return
        source = next((n for n in sample["vertices"] if n.startswith("Source: tick-topic")), None)
        read = records_out(sample, source)
        if not read or read < self.args.kill_taskmanager_at * self.records:
            return
        completed = rest(self.args.rest, f"/jobs/{self.jid}/checkpoints")["counts"]["completed"]
        if completed < 2:
            return
        killed = time.time()
        sh("docker", "kill", "taskmanager")
        sh(*COMPOSE, "up", "-d", "--no-deps", "taskmanager", env=self.env)
        self.info = {"killed_at": killed, "taskmanager_started_at": time.time(), "source_records_read": int(read),
                     "share_read": round(read / self.records, 4), "checkpoints_completed": completed}


def monitor(args, jid, kill=None):
    samples = []
    deadline = time.time() + args.timeout_s
    while True:
        t = time.time()
        try:
            job = rest(args.rest, f"/jobs/{jid}")
            vertices = {v["name"]: sample_vertex(args, jid, v) for v in job["vertices"]}
        except (OSError, ValueError, KeyError) as error:
            samples.append({"t": t, "error": str(error)})
            time.sleep(args.interval)
            continue
        samples.append({"t": t, "state": job["state"], "vertices": vertices})
        if kill is not None:
            kill.check(samples[-1])
        if job["state"] in DONE:
            return job, samples
        if t > deadline:
            sh("docker", "exec", "jobmanager", "flink", "cancel", jid, check=False)
            sys.exit(f"job {jid} still {job['state']} after {args.timeout_s} s, cancelled")
        time.sleep(max(0.0, args.interval - (time.time() - t)))


def records_out(sample, vertex):
    subtasks = sample.get("vertices", {}).get(vertex, {}).get("subtasks")
    return sum(s["numRecordsOut"] for s in subtasks) if subtasks else None


def source_window(samples, source):
    reading = [s for s in samples if (records_out(s, source) or 0) > 0]
    if not reading:
        return None
    start = reading[0]["t"]
    end = next((s["t"] for s in samples if s["t"] > start and "vertices" in s
                and s["vertices"][source]["status"] != "RUNNING"), samples[-1]["t"])
    return start, end


def middle(samples, window, share):
    start, end = window
    lo, hi = start + (1 - share) / 2 * (end - start), end - (1 - share) / 2 * (end - start)
    return [s for s in samples if lo <= s["t"] <= hi and "vertices" in s]


def task_seconds(a, b):
    return sum(b[m] - a[m] for m in ACCUMULATED.values()) / 1000


def vertex_stats(name, chosen):
    with_subtasks = [s for s in chosen if "subtasks" in s["vertices"].get(name, {})]
    stats = {}
    if len(with_subtasks) >= 2:
        first, last = with_subtasks[0]["vertices"][name]["subtasks"], with_subtasks[-1]["vertices"][name]["subtasks"]
        pairs = [(a, b, task_seconds(a, b)) for a, b in zip(first, last)]
        for key, metric in ACCUMULATED.items():
            per = [(b[metric] - a[metric]) / seconds for a, b, seconds in pairs if seconds > 0]
            if per:
                stats[f"{key}_ms_per_s"] = {"avg": round(statistics.fmean(per), 1), "max": round(max(per), 1)}
    for gauge in GAUGES[:3]:
        values = [s["vertices"][name][gauge] for s in chosen if gauge in s["vertices"].get(name, {})]
        if values:
            stats[f"{gauge}_gauge"] = {k: round(statistics.fmean(v[k] for v in values), 1) for k in ("avg", "max")}
    return stats


def steady_rates(samples, source, chosen, span_s):
    counted = [(s["t"], s["vertices"][source]["subtasks"]) for s in samples
               if "subtasks" in s.get("vertices", {}).get(source, {})]
    rates = []
    for s in chosen:
        current = s["vertices"].get(source, {}).get("subtasks")
        earlier = [subtasks for t, subtasks in counted if t <= s["t"] - span_s]
        if not current or not earlier:
            continue
        pairs = [(a, b, task_seconds(a, b)) for a, b in zip(earlier[-1], current)]
        if all(seconds > 0 for _, _, seconds in pairs):
            rates.append(sum((b["numRecordsOut"] - a["numRecordsOut"]) / seconds for a, b, seconds in pairs))
    meter = [s["vertices"][source]["numRecordsOutPerSecond"]["sum"] for s in chosen
             if "numRecordsOutPerSecond" in s["vertices"].get(source, {})]
    return (round(statistics.median(rates)) if rates else None,
            round(statistics.median(meter)) if meter else None)


def subtask_shares(args, jid, vertex, field):
    detail = rest(args.rest, f"/jobs/{jid}/vertices/{vertex['id']}")
    counts = [s["metrics"][field] for s in detail["subtasks"]]
    total = sum(counts)
    return {"records": counts, "busiest_share": round(max(counts) / total, 4) if total else None}


def verify(args, ema_topic, adv_topic):
    work = Path(tempfile.mkdtemp(prefix="verify-", dir=args.run_dir))
    extra = (["--isolation", args.isolation] if args.isolation else []) + (["--dedupe"] if args.dedupe else [])
    extra += ["--enriched"] if args.enrich else []
    try:
        sh(sys.executable, "-m", "trading.collect", "--bootstrap", args.bootstrap, "--from-start",
           "--ema-topic", ema_topic, "--advisories-topic", adv_topic, "--out", str(work), *extra)
        collected = json.loads((work / "metrics.json").read_text())
        result = sh(sys.executable, "-m", "trading.diff", str(work), str(args.verify), check=False)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    counts = {name: {k: int(v) for k, v in re.findall(r"(\w+)=(\d+)", fields)}
              for name, fields in re.findall(r"^(ema|advisories): (.*)$", result.stdout, re.M)}
    collected = {k: v for k, v in collected.items() if not k.startswith("latency")}
    return {"reference": str(args.verify), "identical": result.returncode == 0, **counts, "collect": collected,
            "diff": result.stdout}


def checkpoint_stats(args, jid):
    data = rest(args.rest, f"/jobs/{jid}/checkpoints")
    keep = ("id", "status", "trigger_timestamp", "latest_ack_timestamp", "end_to_end_duration", "state_size",
            "checkpointed_size", "alignment_buffered", "processed_data", "persisted_data", "failure_message")
    return {"config": rest(args.rest, f"/jobs/{jid}/checkpoints/config"), "counts": data["counts"],
            "summary": data["summary"], "restored": data["latest"].get("restored"),
            "history": [{k: h.get(k) for k in keep} for h in data.get("history", [])]}


def recovery_stats(args, jid, job, samples, kill, checkpoints):
    info = dict(kill.info or {})
    if not info:
        return {"killed": False}
    killed = info["killed_at"]
    entries = rest(args.rest, f"/jobs/{jid}/exceptions").get("exceptionHistory", {}).get("entries", [])
    info["exceptions"] = [{"timestamp": e["timestamp"], "exception": e.get("exceptionName"), "task": e.get("taskName"),
                           "endpoint": e.get("endpoint")} for e in entries]
    info["attempts"] = {v["name"]: max(s["attempt"] for s in rest(args.rest, f"/jobs/{jid}/vertices/{v['id']}")["subtasks"])
                        for v in job["vertices"]}
    restored = checkpoints["restored"]
    info["restored"] = restored
    if entries:
        info["failure_detected_s"] = round(min(e["timestamp"] for e in entries) / 1000 - killed, 1)
    if restored:
        restore_t = restored["restore_timestamp"] / 1000
        info["restore_s"] = round(restore_t - killed, 1)
        source = next(v["name"] for v in job["vertices"] if v["name"].startswith("Source: tick-topic"))
        after = [s for s in samples if s["t"] >= restore_t and "vertices" in s]
        running = next((s["t"] for s in after if all(v["status"] == "RUNNING" for v in s["vertices"].values())), None)
        resumed = next((s["t"] for s in after if (records_out(s, source) or 0) > 0), None)
        info["all_running_s"] = round(running - killed, 1) if running else None
        info["source_resumed_s"] = round(resumed - killed, 1) if resumed else None
        used = next((h for h in checkpoints["history"] if h["id"] == restored["id"]), None)
        if used:
            info["restored_checkpoint_age_at_kill_s"] = round(killed - used["latest_ack_timestamp"] / 1000, 1)
    return info


def run_flink(args, p, cpuset, run, records):
    tag = f"p{p}-r{run}-{uuid.uuid4().hex[:6]}"
    ema_topic, adv_topic = f"bench-ema-{tag}", f"bench-adv-{tag}"
    if args.checkpoint_ms:
        CHECKPOINTS.mkdir(exist_ok=True)
    placement = start_cluster(args, p, cpuset)
    topics.create(args.bootstrap, [ema_topic, adv_topic], 8)
    try:
        return measure_flink(args, p, run, records, tag, ema_topic, adv_topic, placement, cpuset)
    finally:
        if not args.keep_topics:
            topics.delete(args.bootstrap, [ema_topic, adv_topic])


def start_gateway(args, tag, ema_topic, adv_topic):
    out = args.run_dir / f"gateway-{tag}.json"
    proc = subprocess.Popen([sys.executable, "-m", "trading.gateway", "--bootstrap", args.bootstrap,
                             "--ema-topic", ema_topic, "--advisories-topic", adv_topic,
                             "--subscriptions-topic", args.subscriptions_topic, "--from-start",
                             "--idle-exit-s", "10", "--sample", str(args.gateway_sample),
                             "--sample-dir", str(args.run_dir / f"gateway-{tag}"), "--out", str(out)],
                            cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    return proc, out


def finish_gateway(gateway):
    proc, out = gateway
    try:
        _, err = proc.communicate(timeout=1800)
    except subprocess.TimeoutExpired:
        proc.kill()
        return {"error": "gateway did not finish within 1800 s"}
    if proc.returncode != 0:
        return {"error": err[-2000:]}
    return json.loads(out.read_text())


def measure_flink(args, p, run, records, tag, ema_topic, adv_topic, placement, cpuset):
    gateway = start_gateway(args, tag, ema_topic, adv_topic) if args.gateway else None
    try:
        return flink_result(args, p, run, records, tag, ema_topic, adv_topic, placement, gateway, cpuset)
    finally:
        if gateway and gateway[0].poll() is None:
            gateway[0].kill()


def flink_result(args, p, run, records, tag, ema_topic, adv_topic, placement, gateway, cpuset):
    cpu = CpuSampler(["broker", "jobmanager", "taskmanager"])
    cpu.start()
    kill = None
    try:
        jid = submit(args, p, f"bench-{tag}", ema_topic, adv_topic)
        if args.kill_taskmanager_at is not None:
            kill = TaskManagerKill(args, jid, records, p, cpuset)
        job, samples = monitor(args, jid, kill)
    finally:
        cpu.stop()
    result = {"engine": "flink", "p": p, "run": run, "topic": args.topic, "records": records,
              "ooo_ms": args.ooo_ms, "cpusets": placement, "job_id": jid, "state": job["state"]}
    if fault_tolerance_args(args):
        result["fault_tolerance"] = {"checkpoint_ms": args.checkpoint_ms, "checkpoint_mode": args.checkpoint_mode,
                                     "delivery": args.delivery}
    if args.keep_topics:
        result["output_topics"] = [ema_topic, adv_topic]
    if args.checkpoint_ms:
        result["checkpoints"] = checkpoint_stats(args, jid)
        shutil.rmtree(CHECKPOINTS / jid, ignore_errors=True)
        if kill is not None:
            result["recovery"] = recovery_stats(args, jid, job, samples, kill, result["checkpoints"])
    if job["state"] != "FINISHED":
        result.update({"exceptions": rest(args.rest, f"/jobs/{jid}/exceptions"), "samples": samples})
        return result
    vertices = job["vertices"]
    source = next(v for v in vertices if v["name"].startswith("Source: tick-topic"))
    keyed = next(v for v in vertices if v["name"].startswith(("KeyedProcess", "Co-Keyed-Process")))
    window = source_window(samples, source["name"])
    chosen = middle(samples, window, args.steady_share) if window else []
    steady, meter = steady_rates(samples, source["name"], chosen, args.rate_span_s)
    job_s = (job["end-time"] - job["start-time"]) / 1000
    source_s = source["duration"] / 1000
    result.update({
        "job_ms": job["end-time"] - job["start-time"],
        "job_timestamps": job.get("timestamps"),
        "vertices": [{"name": v["name"], "parallelism": v["parallelism"], "duration_ms": v["duration"],
                      **vertex_stats(v["name"], chosen)} for v in vertices],
        "throughput": {"records_per_s_job": round(records / job_s), "records_per_s_source": round(records / source_s),
                       "records_per_s_steady": steady, "records_per_s_meter_median": meter},
        "steady_window": {"share": args.steady_share, "samples": len(chosen),
                          "source_seconds": round(window[1] - window[0], 1) if window else None},
        "source_split": subtask_shares(args, jid, source, "write-records"),
        "keyed_split": subtask_shares(args, jid, keyed, "read-records"),
        "cpu_percent_mean": cpu.mean(*window) if window else None,
        "outputs": {"ema": topics.count(args.bootstrap, ema_topic), "advisories": topics.count(args.bootstrap, adv_topic)},
    })
    if control_args(args):
        result["control"] = control_args(args)
    if gateway:
        result["gateway"] = finish_gateway(gateway)
    if args.verify and run <= args.verify_runs:
        result["verify"] = verify(args, ema_topic, adv_topic)
    result["samples"] = samples
    return result


def run_python(args, records):
    tag = f"py-{uuid.uuid4().hex[:6]}"
    ema_topic, adv_topic = f"bench-ema-{tag}", f"bench-adv-{tag}"
    topics.create(args.bootstrap, [ema_topic, adv_topic], 8)
    work = Path(tempfile.mkdtemp(prefix="python-", dir=args.run_dir))
    cpu = CpuSampler(["broker"])
    cpu.start()
    started = time.time()
    try:
        extra = ["--enrich"] if args.enrich else []
        sh(sys.executable, "-m", "trading.engine", "--kafka", args.bootstrap, "--topic", args.topic,
           "--group", f"bench-{tag}", "--out", str(work), "--out-of-orderness-ms", str(args.ooo_ms),
           "--idle-partition-ms", "86400000", "--idle-exit-s", "10", "--ema-topic", ema_topic,
           "--advisories-topic", adv_topic, *extra)
        metrics = json.loads((work / "metrics.json").read_text())
        cpu.stop()
        result = {"engine": "python", "p": 1, "run": 1, "topic": args.topic, "records": records,
                  "ooo_ms": args.ooo_ms, "metrics": metrics,
                  "throughput": {"records_per_s_engine": metrics["events_per_second"]},
                  "cpu_percent_mean": cpu.mean(started, started + metrics["wall_seconds"]),
                  "outputs": {"ema": topics.count(args.bootstrap, ema_topic),
                              "advisories": topics.count(args.bootstrap, adv_topic)}}
        if args.verify:
            result["verify"] = verify(args, ema_topic, adv_topic)
        return result
    finally:
        cpu.stop()
        shutil.rmtree(work, ignore_errors=True)
        topics.delete(args.bootstrap, [ema_topic, adv_topic])


MEASURES = ("job", "source", "steady")


def summary_rows(results):
    base = {}
    for m in MEASURES:
        values = [r["throughput"][f"records_per_s_{m}"] for r in results if r["engine"] == "flink" and r["p"] == 1
                  and r["throughput"][f"records_per_s_{m}"]]
        base[m] = statistics.median(values) if values else None
    rows = []
    for r in results:
        row = {"engine": r["engine"], "p": r["p"], "run": r["run"], "records": r["records"]}
        if r["engine"] == "python":
            rate = r["throughput"]["records_per_s_engine"]
            row.update({f"rps_{m}": rate for m in MEASURES})
        else:
            row.update({f"rps_{m}": r["throughput"][f"records_per_s_{m}"] for m in MEASURES})
            row["rps_meter_median"] = r["throughput"]["records_per_s_meter_median"]
            row["job_s"] = round(r["job_ms"] / 1000, 2)
        for m in MEASURES:
            speedup = row[f"rps_{m}"] / base[m] if base[m] and row[f"rps_{m}"] else None
            row[f"speedup_{m}"] = round(speedup, 3) if speedup else None
            row[f"efficiency_{m}"] = round(speedup / r["p"], 3) if speedup else None
        if r["engine"] == "flink":
            busy = [(v.get("busy_ms_per_s", {}).get("avg", -1), v) for v in r["vertices"]]
            top = max(busy, key=lambda b: b[0])[1]
            source = next(v for v in r["vertices"] if v["name"].startswith("Source"))
            row.update({
                "busiest_vertex": top["name"],
                "busiest_busy_ms": top.get("busy_ms_per_s", {}).get("avg"),
                "busiest_busy_max_ms": top.get("busy_ms_per_s", {}).get("max"),
                "source_backpressure_ms": source.get("backpressure_ms_per_s", {}).get("avg"),
                "source_busiest_share": r["source_split"]["busiest_share"],
                "keyed_busiest_share": r["keyed_split"]["busiest_share"],
            })
        for name, value in (r.get("cpu_percent_mean") or {}).items():
            row[f"cpu_{name}"] = value
        if "checkpoints" in r:
            summary = r["checkpoints"]["summary"]
            row.update({"checkpoint_ms": r["fault_tolerance"]["checkpoint_ms"],
                        "checkpoint_mode": r["fault_tolerance"]["checkpoint_mode"] or "exactly-once",
                        "delivery": r["fault_tolerance"]["delivery"] or "none",
                        "checkpoints_completed": r["checkpoints"]["counts"]["completed"],
                        "checkpoint_duration_ms_p50": summary["end_to_end_duration"].get("p50"),
                        "checkpoint_duration_ms_max": summary["end_to_end_duration"].get("max"),
                        "checkpoint_size_bytes_p50": summary["checkpointed_size"].get("p50")})
        if "recovery" in r:
            row.update({k: r["recovery"].get(k) for k in ("failure_detected_s", "restore_s", "source_resumed_s")})
        if "verify" in r:
            v = r["verify"]
            row["verify"] = ("identical" if v["identical"] else
                             "ema mismatched={mismatched} only_left={only_left} only_right={only_right}".format(**v["ema"]))
        rows.append(row)
    return rows


def fmt(value):
    if value is None or value == "":
        return "-"
    if isinstance(value, float):
        return f"{value:,.2f}"
    if isinstance(value, int):
        return f"{value:,}"
    return str(value)


def markdown(rows):
    head = ["engine", "p", "run", "rec/s job", "rec/s source", "rec/s steady", "speedup job/source/steady",
            "efficiency job/source/steady", "busiest vertex: busy avg/max ms/s", "source back-pressure ms/s",
            "CPU % broker/JM/TM", "verify"]
    lines = ["| " + " | ".join(head) + " |", "|" + "---|" * len(head)]
    for r in rows:
        trio = lambda key: " / ".join(fmt(r.get(f"{key}_{m}")) for m in MEASURES)
        busiest = f"{r['busiest_vertex']}: {fmt(r['busiest_busy_ms'])} / {fmt(r['busiest_busy_max_ms'])}" if "busiest_vertex" in r else "-"
        cpu = " / ".join(fmt(r.get(f"cpu_{n}")) for n in ("broker", "jobmanager", "taskmanager"))
        lines.append("| " + " | ".join([r["engine"], str(r["p"]), str(r["run"]), fmt(r["rps_job"]), fmt(r["rps_source"]),
                                         fmt(r["rps_steady"]), trio("speedup"), trio("efficiency"), busiest,
                                         fmt(r.get("source_backpressure_ms")), cpu, r.get("verify", "-")]) + " |")
    groups = {}
    for r in rows:
        groups.setdefault((r["engine"], r["p"]), []).append(r)
    head = ["engine", "p", "runs"] + [f"rec/s {m} median [min-max]" for m in MEASURES] + ["speedup job/source/steady",
                                                                                         "efficiency job/source/steady"]
    lines += ["", "| " + " | ".join(head) + " |", "|" + "---|" * len(head)]
    for (engine, p), group in groups.items():
        cells = [engine, str(p), str(len(group))]
        for m in MEASURES:
            values = [g[f"rps_{m}"] for g in group if g[f"rps_{m}"]]
            cells.append(f"{fmt(round(statistics.median(values)))} [{fmt(min(values))}-{fmt(max(values))}]" if values else "-")
        for key in ("speedup", "efficiency"):
            parts = []
            for m in MEASURES:
                values = [g[f"{key}_{m}"] for g in group if g[f"{key}_{m}"]]
                parts.append(fmt(round(statistics.median(values), 3)) if values else "-")
            cells.append(" / ".join(parts))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines) + "\n"


def write_summary(run_dir, results):
    rows = summary_rows(results)
    fields = []
    for row in rows:
        fields += [k for k in row if k not in fields]
    with (run_dir / "summary.csv").open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    text = markdown(rows)
    (run_dir / "summary.md").write_text(text)
    return text


def main(argv=None):
    parser = argparse.ArgumentParser(description="Throughput benchmark: drain a preloaded topic with the Docker Flink cluster")
    parser.add_argument("--topic", required=True)
    parser.add_argument("--ticks", help="tick directory the topic was preloaded from (gives the expected message count)")
    parser.add_argument("--expected", type=int, help="expected message count instead of --ticks")
    parser.add_argument("--parallelism", type=int, nargs="+", default=[1, 2, 4])
    parser.add_argument("--cpusets", nargs="+", help="TaskManager cpuset per parallelism (default: the first P vCPUs)")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--ooo-ms", type=int, default=300)
    parser.add_argument("--out", default="out/bench")
    parser.add_argument("--verify", help="reference result folder to diff the outputs against")
    parser.add_argument("--verify-runs", type=int, default=1_000_000, help="verify only the first N runs of each p")
    parser.add_argument("--python-baseline", action="store_true")
    parser.add_argument("--quarantine-ms", type=int, help="the topic was preloaded with replay --quarantine-ms N")
    parser.add_argument("--enrich", action="store_true",
                        help="the topic was preloaded with replay --with-metadata; pass --enrich to the engines")
    parser.add_argument("--subscriptions-topic", help="pass this compacted subscriptions topic to the Flink job")
    parser.add_argument("--gateway", action="store_true", help="run trading.gateway on each Flink run's outputs")
    parser.add_argument("--gateway-sample", type=int, default=3, help="subscribers whose deliveries the gateway writes to files")
    parser.add_argument("--checkpoint-ms", type=int, help="pass --checkpoint-ms to the job (storage: --checkpoint-dir)")
    parser.add_argument("--checkpoint-mode", choices=["exactly-once", "at-least-once"])
    parser.add_argument("--checkpoint-dir", default=CHECKPOINT_DIR, help="inside the containers; ./checkpoints on the host")
    parser.add_argument("--delivery", choices=["none", "at-least-once", "exactly-once"], help="the job's Kafka sinks")
    parser.add_argument("--isolation", choices=["read_committed", "read_uncommitted"],
                        help="collect --isolation for --verify (default: collect's, read_committed)")
    parser.add_argument("--dedupe", action="store_true", help="collect --dedupe for --verify")
    parser.add_argument("--kill-taskmanager-at", type=float, metavar="SHARE",
                        help="docker kill the TaskManager once the source has read this share of the topic and at "
                             "least 2 checkpoints completed, then start it again")
    parser.add_argument("--keep-topics", action="store_true", help="keep each run's output topics")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--job-bootstrap", default="broker:19092")
    parser.add_argument("--rest", default="http://localhost:8081")
    parser.add_argument("--broker-cpuset", default="4-5")
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--steady-share", type=float, default=0.8)
    parser.add_argument("--rate-span-s", type=float, default=5.0)
    parser.add_argument("--timeout-s", type=float, default=3600)
    parser.add_argument("--keep-cluster", action="store_true", help="leave the last JobManager and TaskManager running")
    args = parser.parse_args(argv)
    cpusets = args.cpusets or ["0" if p == 1 else f"0-{p - 1}" for p in args.parallelism]
    if len(cpusets) != len(args.parallelism):
        parser.error("--cpusets needs one entry per --parallelism value")
    if args.verify:
        args.verify = Path(args.verify).resolve()
    if args.gateway and not args.subscriptions_topic:
        parser.error("--gateway needs --subscriptions-topic")
    if args.kill_taskmanager_at is not None and not args.checkpoint_ms:
        parser.error("--kill-taskmanager-at needs --checkpoint-ms")

    records = preflight(args)
    args.run_dir = Path(args.out).resolve() / f"{args.topic}-{time.strftime('%Y%m%d-%H%M%S')}"
    args.run_dir.mkdir(parents=True)
    results = []
    try:
        for p, cpuset in zip(args.parallelism, cpusets):
            for run in range(1, args.runs + 1):
                result = run_flink(args, p, cpuset, run, records)
                (args.run_dir / f"flink-p{p}-r{run}.json").write_text(json.dumps(result, indent=1))
                if result["state"] != "FINISHED":
                    sys.exit(f"job {result['job_id']} ended {result['state']}, see {args.run_dir}")
                results.append(result)
                t = result["throughput"]
                print(f"flink p={p} run={run} {result['state']} job={t['records_per_s_job']:,}/s "
                      f"source={t['records_per_s_source']:,}/s steady={t['records_per_s_steady'] or 0:,}/s "
                      f"cpu={result['cpu_percent_mean']} outputs={result['outputs']} "
                      f"deliveries={result.get('gateway', {}).get('counts', {}).get('deliveries', '-')} "
                      f"verify={result.get('verify', {}).get('identical', '-')}"
                      + (f" checkpoints={result['checkpoints']['counts']}" if "checkpoints" in result else "")
                      + (f" recovery={ {k: v for k, v in result['recovery'].items() if k.endswith('_s')} }"
                         if "recovery" in result else ""),
                      flush=True)
    finally:
        if not args.keep_cluster:
            stop_cluster()
    if args.python_baseline:
        result = run_python(args, records)
        (args.run_dir / "python.json").write_text(json.dumps(result, indent=1))
        results.append(result)
        print(f"python {result['throughput']['records_per_s_engine']:,}/s "
              f"verify={result.get('verify', {}).get('identical', '-')}", flush=True)
    if results:
        print(write_summary(args.run_dir, results))
    print(f"results in {args.run_dir}")
    failed = [r for r in results if r.get("state", "FINISHED") != "FINISHED" or not r.get("verify", {}).get("identical", True)]
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
