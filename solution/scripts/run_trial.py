"""One end-to-end trial against a fresh mock provider, then verification.

    python solution/scripts/run_trial.py --seed 42 --failure-scale 1 --kills 3

Starts the mock on its own port, runs the dispatcher with its own database and output file,
optionally `kill -9`s the dispatcher at random moments and restarts it (as the graders will),
and finally runs verify.py. Each trial lives in data/trials/<name>/ (git-ignored) and uses its
own ports, so several trials can run in parallel.
"""
from __future__ import annotations

import argparse
import random
import shutil
import subprocess
import sys
import time
from pathlib import Path

import httpx

REPO = Path(__file__).resolve().parents[2]
PY = sys.executable


def wait_for(url: str, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            httpx.get(url, timeout=1.0)
            return
        except httpx.HTTPError:
            time.sleep(0.2)
    raise RuntimeError(f"{url} did not come up")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--speed", type=float, default=600.0)
    ap.add_argument("--failure-scale", type=float, default=1.0)
    ap.add_argument("--provider-port", type=int, default=8099)
    ap.add_argument("--webhook-port", type=int, default=8200)
    ap.add_argument("--kills", type=int, default=0, help="how many times to kill -9 the dispatcher")
    ap.add_argument("--kill-min", type=float, default=0.5, help="min real seconds before each kill")
    ap.add_argument("--kill-max", type=float, default=5.0, help="max real seconds before each kill")
    ap.add_argument("--down-max", type=float, default=2.0, help="max real seconds to stay down after a kill")
    ap.add_argument("--timeout", type=float, default=900.0, help="real seconds to wait for the final run")
    a = ap.parse_args()

    name = f"seed{a.seed}-x{a.speed:g}-f{a.failure_scale:g}-k{a.kills}"
    work = REPO / "data" / "trials" / name
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True)
    provider = f"http://127.0.0.1:{a.provider_port}"
    rng = random.Random(f"{a.seed}:{a.kills}")

    mock_log = open(work / "mock.log", "w")
    mock = subprocess.Popen(
        [PY, "-m", "mock_telephony", "--seed", str(a.seed), "--speed", str(a.speed),
         "--failure-scale", str(a.failure_scale), "--port", str(a.provider_port)],
        cwd=REPO / "assignment-b-workflow", stdout=mock_log, stderr=subprocess.STDOUT)
    try:
        wait_for(f"{provider}/v1/clock")
        disp_log = open(work / "dispatcher.log", "a")
        cmd = [PY, "-m", "dispatcher", "--provider", provider, "--port", str(a.webhook_port),
               "--db", str(work / "dispatcher.db"), "--out", str(work / "final_state.csv")]
        kills_left, run_no, rc = a.kills, 0, None
        started = time.monotonic()
        while True:
            run_no += 1
            disp_log.write(f"\n===== run {run_no} =====\n")
            disp_log.flush()
            proc = subprocess.Popen(cmd, cwd=REPO / "solution", stdout=disp_log, stderr=subprocess.STDOUT)
            if kills_left:
                try:
                    rc = proc.wait(timeout=rng.uniform(a.kill_min, a.kill_max))
                    break  # finished before we got to kill it
                except subprocess.TimeoutExpired:
                    proc.kill()  # SIGKILL: no cleanup, no flush, exactly like kill -9
                    proc.wait()
                    kills_left -= 1
                    print(f"[{name}] kill -9 #{a.kills - kills_left} during run {run_no}", flush=True)
                    time.sleep(rng.uniform(0, a.down_max))
                    continue
            rc = proc.wait(timeout=a.timeout)
            break
        elapsed = time.monotonic() - started
        verify = subprocess.run([PY, str(REPO / "solution" / "scripts" / "verify.py"), "--provider", provider,
                                 "--csv", str(work / "final_state.csv")], capture_output=True, text=True)
        (work / "verify.txt").write_text(verify.stdout + verify.stderr)
        result = verify.stdout.strip().splitlines()[-1] if verify.stdout.strip() else "RESULT: (no output)"
        print(f"[{name}] dispatcher exit={rc} runs={run_no} kills={a.kills - kills_left} "
              f"real={elapsed:.0f}s {result}  -> {work / 'verify.txt'}", flush=True)
        return 0 if (rc == 0 and verify.returncode == 0) else 1
    finally:
        mock.terminate()
        mock.wait(timeout=10)


if __name__ == "__main__":
    sys.exit(main())
