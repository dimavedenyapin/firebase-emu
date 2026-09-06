# Emulator resource benchmark

Small reusable harness. Measures the running emulator processes only,
never build/compiler usage. Workload generator is tracked separately and
excluded from emulator totals.

## Scenarios

1. Startup + idle without Functions: `measure.py --mode no-functions`
2. Idle with Node22 Functions fixture: `measure.py --mode functions`
3. Representative load: `measure.py --mode functions --workload "node scripts/bench/workload.mjs"`

## Reproduce (isolated ports 18280/18299/18399/18401)

```sh
cd /path/to/firebase-emu
python3 scripts/bench/measure.py --mode no-functions --idle-seconds 10 --out /tmp/bench-nofunc.jsonl --label idle-no-functions
python3 scripts/bench/measure.py --mode functions --idle-seconds 15 --out /tmp/bench-func-idle.jsonl --label idle-functions
python3 scripts/bench/measure.py --mode functions --workload "node scripts/bench/workload.mjs" --out /tmp/bench-load.jsonl --label load
```

`measure.py` spawns one emulator, waits for loopback readiness
(plus the Functions ready line), samples `ps` RSS/`%CPU` every 0.5 s,
runs the optional workload as a separate process, keeps a 5 s
retained-memory tail, then SIGTERMs only its own process group and
confirms the ports are free. It never touches unrelated processes.

CPU convention: macOS `ps %CPU`, 100% = one logical core.
Tree RSS sums per-process RSS; shared pages are double-counted.
The workload generator RSS/CPU is reported separately.
