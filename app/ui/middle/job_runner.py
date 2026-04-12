from __future__ import annotations

import os
import subprocess
import sys
import threading
from pathlib import Path

from app.ui.middle.ops_job_store import OpsJobStore


def repo_root_from_here(file_path: str) -> Path:
    return Path(file_path).resolve().parents[2]


def _iter_lines(stream):
    try:
        for line in iter(stream.readline, ""):
            if not line:
                break
            yield line.rstrip("\n")
    except Exception:
        return


def run_subprocess_job_in_thread(
    *,
    store: OpsJobStore,
    argv: list[str],
    cwd: str | Path,
    env: dict[str, str] | None = None,
    title: str,
) -> str:
    """
    Start a subprocess job in a background thread and return the job_id.

    The subprocess output is streamed into ops_job_events.
    """
    cwd_str = str(cwd)
    job_id = store.create_job(command=title, args=argv, cwd=cwd_str)

    def _worker() -> None:
        merged_env = os.environ.copy()
        if env:
            merged_env.update({str(k): str(v) for k, v in env.items()})

        try:
            store.append_event(job_id=job_id, stream="meta", line=f"cwd: {cwd_str}")
            store.append_event(job_id=job_id, stream="meta", line=f"argv: {argv}")

            p = subprocess.Popen(
                argv,
                cwd=cwd_str,
                env=merged_env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )

            # Read stdout/stderr concurrently to avoid deadlocks.
            def _pump(s, stream_name: str) -> None:
                if s is None:
                    return
                for line in _iter_lines(s):
                    if line.strip():
                        store.append_event(job_id=job_id, stream=stream_name, line=line)

            t_out = threading.Thread(target=_pump, args=(p.stdout, "stdout"), daemon=True)
            t_err = threading.Thread(target=_pump, args=(p.stderr, "stderr"), daemon=True)
            t_out.start()
            t_err.start()

            exit_code = p.wait()
            t_out.join(timeout=1.0)
            t_err.join(timeout=1.0)

            status = "succeeded" if exit_code == 0 else "failed"
            store.finish_job(job_id=job_id, exit_code=int(exit_code), status=status)
        except Exception as e:
            store.append_event(job_id=job_id, stream="stderr", line=f"{type(e).__name__}: {e}")
            store.finish_job(job_id=job_id, exit_code=None, status="failed")

    threading.Thread(target=_worker, daemon=True).start()
    return job_id


def python_module_argv(module: str, args: list[str]) -> list[str]:
    return [sys.executable, "-m", str(module), *[str(a) for a in args]]


def python_script_argv(script_path: str | Path, args: list[str] | None = None) -> list[str]:
    a = [sys.executable, str(script_path)]
    if args:
        a.extend([str(x) for x in args])
    return a

