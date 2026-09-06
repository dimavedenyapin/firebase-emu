#!/usr/bin/env python3
"""Reusable resource measurement for the Rust firebase-emu binary.

Starts one emulator process on isolated loopback ports, waits for readiness,
samples RSS/%CPU with ps, optionally runs a workload subprocess, then stops
the emulator and prints a JSON summary. Raw timestamped samples go to JSONL.

Emulator-only usage is measured from the Rust process and its Node Functions
children. The workload generator runs as a separate PID and is reported
separately, never added to emulator totals.

CPU convention: macOS ps %CPU, where 100% = one logical core.

Usage examples are in scripts/bench/README.md.
"""
import argparse
import json
import os
import signal
import socket
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
BINARY = os.path.join(REPO, "target/release/firebase-emu")


def port_open(host, port):
    s = socket.socket()
    s.settimeout(0.3)
    try:
        return s.connect_ex((host, port)) == 0
    finally:
        s.close()


def ps_sample(pids):
    """Return {pid: {rss_kb, cpu_pct, time_s}} using one ps call."""
    out = {}
    if not pids:
        return out
    try:
        proc = subprocess.run(
            ["ps", "-o", "pid=,rss=,%cpu=,cputime=", "-p", ",".join(map(str, pids))],
            capture_output=True, text=True, timeout=5,
        )
    except Exception:
        return out
    for line in proc.stdout.splitlines():
        parts = line.split()
        if len(parts) < 4:
            continue
        try:
            pid = int(parts[0])
            rss_kb = int(parts[1])
            cpu_pct = float(parts[2])
            t = parts[3]
            # cputime is [[dd-]hh:]mm:ss[.frac]
            days = 0
            if "-" in t:
                days, t = t.split("-", 1)
                days = int(days)
            segs = t.split(":")
            try:
                if len(segs) == 3:
                    secs = int(segs[0]) * 3600 + int(segs[1]) * 60 + float(segs[2])
                elif len(segs) == 2:
                    secs = int(segs[0]) * 60 + float(segs[1])
                else:
                    secs = float(segs[0])
            except ValueError:
                continue
            secs += days * 86400
            out[pid] = {"rss_kb": rss_kb, "cpu_pct": cpu_pct, "time_s": round(secs, 2)}
        except ValueError:
            continue
    return out


def child_pids(root_pid):
    """All descendant PIDs of root_pid (one level recursion via ps)."""
    try:
        proc = subprocess.run(["ps", "-o", "pid=,ppid=", "-ax"],
                              capture_output=True, text=True, timeout=5)
    except Exception:
        return []
    children = {}
    for line in proc.stdout.splitlines():
        parts = line.split()
        if len(parts) != 2:
            continue
        try:
            children.setdefault(int(parts[1]), []).append(int(parts[0]))
        except ValueError:
            continue
    found = []
    stack = [root_pid]
    while stack:
        pid = stack.pop()
        for child in children.get(pid, []):
            found.append(child)
            stack.append(child)
    return found


def summarize(samples):
    rss = [s["rss_kb"] for s in samples if s["rss_kb"] is not None]
    cpu = [s["cpu_pct"] for s in samples if s["cpu_pct"] is not None]
    if not rss:
        return {}
    rss_sorted = sorted(rss)
    return {
        "samples": len(rss),
        "rss_kb": {"min": min(rss), "median": statistics.median(rss),
                   "p95": rss_sorted[min(len(rss_sorted) - 1, int(len(rss_sorted) * 0.95))],
                   "max": max(rss),
                   "min_mib": round(min(rss) / 1024, 2),
                   "median_mib": round(statistics.median(rss) / 1024, 2),
                   "max_mib": round(max(rss) / 1024, 2)},
        "cpu_pct": {"avg": round(sum(cpu) / len(cpu), 2) if cpu else 0,
                    "max": round(max(cpu), 2) if cpu else 0},
    }


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["no-functions", "functions"], required=True)
    ap.add_argument("--firestore-port", type=int, default=18280)
    ap.add_argument("--auth-port", type=int, default=18299)
    ap.add_argument("--storage-port", type=int, default=18399)
    ap.add_argument("--functions-port", type=int, default=18401)
    ap.add_argument("--project", default="demo-bench")
    ap.add_argument("--idle-seconds", type=float, default=10.0)
    ap.add_argument("--interval", type=float, default=0.5)
    ap.add_argument("--workload", default=None,
                    help="optional command to run during measurement, e.g. 'node scripts/bench/workload.mjs'")
    ap.add_argument("--workload-timeout", type=int, default=180)
    ap.add_argument("--ready-timeout", type=int, default=30)
    ap.add_argument("--out", default=None, help="raw JSONL output path")
    ap.add_argument("--label", default="")
    args = ap.parse_args()

    for p in (args.firestore_port, args.auth_port, args.storage_port, args.functions_port):
        if port_open("127.0.0.1", p):
            print(f"port {p} is already in use; pick isolated ports", file=sys.stderr)
            return 2

    env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"),
           "FIREBASE_EMU_HOST": "127.0.0.1",
           "FIRESTORE_EMU_PORT": str(args.firestore_port),
           "FIREBASE_AUTH_EMU_PORT": str(args.auth_port),
           "FIREBASE_STORAGE_EMU_PORT": str(args.storage_port),
           "FIREBASE_FUNCTIONS_EMU_PORT": str(args.functions_port),
           "FIREBASE_DATABASE_EMU_PORT": str(free_port()),
           "PUBSUB_EMULATOR_PORT": str(free_port()),
           "GCLOUD_PROJECT": args.project}
    if args.mode == "functions":
        node22 = os.environ.get("FIREBASE_FUNCTIONS_NODE_22") or subprocess.run(
            ["node", "-p", "process.execPath"], capture_output=True, text=True).stdout.strip()
        env["FIREBASE_FUNCTIONS_NODE_22"] = node22
        env["FIXTURE_EVENT_LOG"] = os.path.join(
            "/tmp", f"bench-events-{os.getpid()}-{int(time.time())}.jsonl")
        cmd = [BINARY, "--config",
               os.path.join(REPO, "functions-runtime/fixtures"),
               "--project", args.project]
    else:
        cmd = [BINARY, "--no-functions"]

    samples = []
    t_spawn = time.monotonic()
    wall_start = datetime.now(timezone.utc).isoformat()
    child = subprocess.Popen(cmd, cwd=REPO, env=env,
                             stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                             text=True, start_new_session=True)
    stderr_text = []
    ready_at = None
    try:
        # Readiness: all three ports accept TCP (functions mode also waits for ready line).
        deadline = time.monotonic() + args.ready_timeout
        err_buf = ""
        import select
        while time.monotonic() < deadline:
            if child.poll() is not None:
                rest = child.stderr.read() or ""
                print(f"emulator exited early: {rest[-2000:]}", file=sys.stderr)
                return 3
            ok = all(port_open("127.0.0.1", p) for p in
                     (args.firestore_port, args.auth_port, args.storage_port))
            if ok and args.mode == "no-functions":
                ready_at = time.monotonic()
                break
            if ok and args.mode == "functions":
                # non-blocking drain of stderr for the ready marker
                import fcntl
                try:
                    fl = fcntl.fcntl(child.stderr, fcntl.F_GETFL)
                    fcntl.fcntl(child.stderr, fcntl.F_SETFL, fl | os.O_NONBLOCK)
                    try:
                        chunk = child.stderr.read() or ""
                    except Exception:
                        chunk = ""
                    if chunk:
                        err_buf += chunk
                        stderr_text.append(chunk)
                    if f"Functions emulator ready on 127.0.0.1:{args.functions_port}" in err_buf:
                        ready_at = time.monotonic()
                        break
                except Exception:
                    pass
            time.sleep(0.1)
        if ready_at is None:
            print("emulator readiness timeout", file=sys.stderr)
            try:
                child.stderr.close()
            except Exception:
                pass
            return 3
        startup_s = round(ready_at - t_spawn, 3)

        rust_pid = child.pid
        # Give Functions children a moment to spawn before first sample.
        if args.mode == "functions":
            time.sleep(2.0)

        workload_proc = None
        workload_result = None
        t_work_start = None
        if args.workload:
            wenv = dict(os.environ)
            wenv.update({
                "FIRESTORE_EMULATOR_HOST": f"127.0.0.1:{args.firestore_port}",
                "FIREBASE_AUTH_EMULATOR_HOST": f"127.0.0.1:{args.auth_port}",
                "FIREBASE_STORAGE_EMULATOR_HOST": f"127.0.0.1:{args.storage_port}",
                "FUNCTIONS_EMU_HOST": f"127.0.0.1:{args.functions_port}",
                "GCLOUD_PROJECT": args.project,
                "GOOGLE_CLOUD_PROJECT": args.project,
                "BENCH_PROJECT": args.project,
            })
            t_work_start = time.monotonic()
            workload_proc = subprocess.Popen(args.workload, shell=True, cwd=REPO,
                                             env=wenv, stdout=subprocess.PIPE,
                                             stderr=subprocess.PIPE, text=True,
                                             start_new_session=True)

        # Sample loop: idle phase, or until workload exits + 5s retained tail.
        t_phase_end = None
        if args.workload:
            deadline = time.monotonic() + args.workload_timeout
        else:
            deadline = time.monotonic() + args.idle_seconds
        workload_pids = []
        while time.monotonic() < deadline:
            descendants = child_pids(rust_pid)
            node_pids = []
            for pid in descendants:
                try:
                    out = subprocess.run(["ps", "-o", "comm=", "-p", str(pid)],
                                         capture_output=True, text=True, timeout=5)
                    if "node" in (out.stdout or "").lower():
                        node_pids.append(pid)
                except Exception:
                    pass
            if workload_proc is not None:
                workload_pids = [workload_proc.pid] + child_pids(workload_proc.pid)
                if workload_proc.poll() is not None and t_phase_end is None:
                    t_phase_end = time.monotonic() + 5.0  # retained-memory tail
                    try:
                        wout, werr = workload_proc.communicate(timeout=5)
                        workload_result = {"exit": workload_proc.returncode,
                                           "stdout_tail": wout[-3000:],
                                           "stderr_tail": werr[-3000:]}
                    except Exception as e:
                        workload_result = {"exit": workload_proc.poll(), "error": str(e)}
                if t_phase_end is not None and time.monotonic() >= t_phase_end:
                    break
            else:
                if time.monotonic() >= (ready_at + args.idle_seconds):
                    break
            watch = [rust_pid] + node_pids
            got = ps_sample(watch)
            wgot = ps_sample(workload_pids) if workload_pids else {}
            ts = datetime.now(timezone.utc).isoformat()
            rust = got.get(rust_pid, {})
            node_rss = sum(v["rss_kb"] for k, v in got.items() if k in node_pids and "rss_kb" in v)
            node_cpu = round(sum(v["cpu_pct"] for k, v in got.items() if k in node_pids), 2)
            tree_rss = (rust.get("rss_kb", 0) or 0) + node_rss
            tree_cpu = round((rust.get("cpu_pct", 0) or 0) + node_cpu, 2)
            # NOTE: tree RSS sums per-process RSS; shared pages are double-counted.
            samples.append({
                "ts": ts, "rust_pid": rust_pid,
                "rust_rss_kb": rust.get("rss_kb"), "rust_cpu_pct": rust.get("cpu_pct"),
                "rust_time_s": rust.get("time_s"),
                "node_pids": node_pids,
                "node_rss_kb": node_rss, "node_cpu_pct": node_cpu,
                "tree_rss_kb": tree_rss, "tree_cpu_pct": tree_cpu,
                "workload_rss_kb": sum(v["rss_kb"] for v in wgot.values()),
            })
            time.sleep(args.interval)

        # Final CPU-time snapshot.
        final = ps_sample([rust_pid])
        rust_final = final.get(rust_pid, {})
        elapsed = round(time.monotonic() - t_spawn, 2)
        rust_series = [s for s in samples if s["rust_rss_kb"] is not None]
        summary = {
            "label": args.label, "mode": args.mode, "wall_start": wall_start,
            "binary": BINARY, "project": args.project,
            "ports": {"firestore": args.firestore_port, "auth": args.auth_port,
                      "storage": args.storage_port, "functions": args.functions_port},
            "startup_s": startup_s, "elapsed_s": elapsed,
            "rust": summarize([{"rss_kb": s["rust_rss_kb"], "cpu_pct": s["rust_cpu_pct"]}
                               for s in rust_series]),
            "node_children": summarize([{"rss_kb": s["node_rss_kb"], "cpu_pct": s["node_cpu_pct"]}
                                        for s in samples]),
            "tree_rss_kb": summarize([{"rss_kb": s["tree_rss_kb"], "cpu_pct": s.get("tree_cpu_pct")}
                                      for s in samples]),
            "rust_cputime_s": rust_final.get("time_s"),
            "workload": workload_result,
            "workload_summary": summarize(
                [{"rss_kb": s["workload_rss_kb"] or 0, "cpu_pct": None} for s in samples
                 if s.get("workload_rss_kb")]) if args.workload else None,
            "note": ("tree RSS sums process RSS; shared memory is double-counted; "
                     "workload generator is reported separately and excluded from emulator totals"),
        }
        if args.out:
            with open(args.out, "w") as f:
                for s in samples:
                    f.write(json.dumps(s) + "\n")
            summary["raw_jsonl"] = args.out
        print(json.dumps(summary, indent=2))
        return 0
    finally:
        try:
            os.killpg(os.getpgid(child.pid), signal.SIGTERM)
        except Exception:
            pass
        try:
            child.wait(timeout=10)
        except Exception:
            try:
                os.killpg(os.getpgid(child.pid), signal.SIGKILL)
            except Exception:
                pass
        # Reap stray Node children of the emulator only (never unrelated PIDs).
        for pid in child_pids(child.pid):
            try:
                os.kill(pid, signal.SIGTERM)
            except Exception:
                pass
        # Confirm ports are free again.
        for _ in range(20):
            if not any(port_open("127.0.0.1", p) for p in
                       (args.firestore_port, args.auth_port, args.storage_port)):
                break
            time.sleep(0.25)


if __name__ == "__main__":
    sys.exit(main())
