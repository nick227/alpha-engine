from __future__ import annotations

import argparse
import asyncio
import subprocess
import sys
import time
import json
import os
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable


# Best-effort load .env so running `python start.py` or `python -m app.cli.start` picks up local config.
try:
    from dotenv import load_dotenv  # type: ignore

    load_dotenv(override=False)
except Exception:
    pass


_REPO_ROOT = Path(__file__).resolve().parents[2]
_ANIM_ENABLED = True
_STATE_PATH = _REPO_ROOT / "data" / "cli_state.json"


def _preflight_bars_provider() -> str | None:
    """
    Ensure a historical bars provider is available before kicking off long runs.

    Returns the provider name if available, else None.
    """
    from app.core.bars import build_bars_provider

    configured = str(os.getenv("HISTORICAL_BARS_PROVIDER", "") or "").strip()
    allow_mock = str(os.getenv("ALLOW_MOCK_BARS", "false") or "").strip().lower() == "true"

    candidates: list[str] = []
    if configured:
        candidates.append(configured)
    else:
        candidates.extend(["alpaca", "polygon", "yfinance"])
        if allow_mock:
            candidates.append("mock")

    last_err: str | None = None
    for name in candidates:
        try:
            build_bars_provider(name)
            return str(name).strip().lower()
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            continue

    print()
    print("Preflight failed")
    print("---------------")
    if configured:
        print(f"HISTORICAL_BARS_PROVIDER={configured} (unavailable)")
    else:
        print("HISTORICAL_BARS_PROVIDER is not set.")
    if last_err:
        print(f"Last error: {last_err}")
    print()
    print("Fix by setting one of:")
    print("  - HISTORICAL_BARS_PROVIDER=alpaca and ALPACA_API_KEY/ALPACA_API_SECRET (or APCA_API_KEY_ID/APCA_API_SECRET_KEY)")
    print("  - HISTORICAL_BARS_PROVIDER=polygon and POLYGON_API_KEY")
    print("  - HISTORICAL_BARS_PROVIDER=yfinance (requires yfinance installed)")
    print("  - (local only) ALLOW_MOCK_BARS=true and HISTORICAL_BARS_PROVIDER=mock")
    print()
    return None


def _isoz(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_date_or_iso(value: str, *, end_of_day: bool = False) -> str:
    s = str(value).strip()
    if "T" in s:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return _isoz(dt)
    dt = datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
    if end_of_day:
        dt = dt + timedelta(hours=23, minutes=59, seconds=59)
    return _isoz(dt)


def _parse_range(range_str: str) -> tuple[str, str]:
    s = str(range_str).strip()
    if ":" not in s:
        raise ValueError("range must be formatted like YYYY-MM-DD:YYYY-MM-DD")
    a, b = s.split(":", 1)
    start = _parse_date_or_iso(a, end_of_day=False)
    end = _parse_date_or_iso(b, end_of_day=True)
    return start, end


def _load_state() -> dict[str, Any] | None:
    try:
        if not _STATE_PATH.exists():
            return None
        payload = json.loads(_STATE_PATH.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def _save_state(payload: dict[str, Any]) -> None:
    try:
        _STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        _STATE_PATH.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    except Exception:
        pass


def _supports_animation() -> bool:
    try:
        return bool(sys.stdout.isatty())
    except Exception:
        return False


def _animate_line(prefix: str, frames: list[str], *, seconds: float = 0.9, interval: float = 0.08) -> None:
    if not _ANIM_ENABLED:
        return
    end = time.monotonic() + float(seconds)
    i = 0
    while time.monotonic() < end:
        frame = frames[i % len(frames)]
        sys.stdout.write("\r" + prefix + " " + frame)
        sys.stdout.flush()
        time.sleep(float(interval))
        i += 1
    sys.stdout.write("\r" + prefix + " [ok]\n")
    sys.stdout.flush()


def _play_intro_animation() -> None:
    if not _ANIM_ENABLED:
        return
    print("Boot sequence")
    print("-------------")
    _animate_line("Warming up quant hamsters", ["(>'-')>", "<('-'<)", "^( '-' )^"], seconds=1.1)
    _animate_line("Aligning alpha particles", [".", "..", "...", "....", "...", ".."], seconds=0.9, interval=0.12)
    _animate_line("Polishing charts", ["[=     ]", "[==    ]", "[===   ]", "[====  ]", "[===== ]", "[======]"], seconds=0.8)
    print()


def _welcome() -> None:
    lines = [
        "+-----------------------------------+",
        "|        Alpha Engine Launcher       |",
        "+-----------------------------------+",
        "",
        "Pick a tool, then answer a few prompts for the required flags.",
        "",
        "How it works:",
        "  1) Choose a tool + subcommand",
        "  2) Fill in required flags (optional flags are, well... optional)",
        "  3) Confirm the exact command before it runs",
        "",
        "Tips:",
        "  - Press Enter to accept defaults (when shown).",
        "  - Type 'q' to quit at any prompt.",
        "  - You can always run the direct commands (see `.README.md`).",
        "",
        "Tools available:",
        "  - Backfill ingest + manage Target Stocks universe",
        "  - Score predictions, backfill scores, rank strategies, promotions",
        "  - Demo pipeline runner",
        "",
    ]
    print("\n".join(lines))
    _play_intro_animation()


def _input_line(prompt: str) -> str:
    try:
        return input(prompt)
    except (EOFError, KeyboardInterrupt):
        return "q"


def _prompt_choice(title: str, options: list[str]) -> int:
    print()
    print(title)
    for i, opt in enumerate(options, start=1):
        print(f"  [{i}] {opt}")
    while True:
        raw = _input_line("Select an option: ").strip()
        if raw.lower() in {"q", "quit", "exit"}:
            raise SystemExit(0)
        try:
            n = int(raw)
        except ValueError:
            print("Enter a number (or 'q' to quit).")
            continue
        if 1 <= n <= len(options):
            return n - 1
        print(f"Pick 1..{len(options)}.")


def _prompt_yes_no(prompt: str, *, default: bool) -> bool:
    suffix = " [Y/n] " if default else " [y/N] "
    while True:
        raw = _input_line(prompt + suffix).strip()
        if raw.lower() in {"q", "quit", "exit"}:
            raise SystemExit(0)
        if raw == "":
            return default
        if raw.lower() in {"y", "yes"}:
            return True
        if raw.lower() in {"n", "no"}:
            return False
        print("Enter y/n (or 'q' to quit).")


def _display_flag(action: argparse.Action) -> str:
    if action.option_strings:
        long_flags = [s for s in action.option_strings if s.startswith("--") and not s.startswith("--no-")]
        if long_flags:
            return long_flags[0]
        return action.option_strings[0]
    return str(action.dest)


def _is_help_action(action: argparse.Action) -> bool:
    return getattr(action, "dest", None) == "help" or "-h" in getattr(action, "option_strings", [])


def _is_subparser_action(action: argparse.Action) -> bool:
    return isinstance(action, argparse._SubParsersAction)


def _get_subparsers(parser: argparse.ArgumentParser) -> argparse._SubParsersAction:
    for a in parser._actions:
        if _is_subparser_action(a):
            return a
    raise RuntimeError("Parser has no subcommands")


def _parse_typed(raw: str, action: argparse.Action) -> Any:
    if raw.lower() in {"q", "quit", "exit"}:
        raise SystemExit(0)
    if raw == "":
        return None
    t = getattr(action, "type", None) or str
    try:
        return t(raw)
    except Exception as e:  # noqa: BLE001 - surface the parse issue to user
        raise ValueError(str(e)) from e


def _prompt_value(action: argparse.Action, *, required: bool, show_default: bool) -> Any:
    flag = _display_flag(action)
    help_text = (getattr(action, "help", None) or "").strip()
    default = getattr(action, "default", None)
    choices = getattr(action, "choices", None)

    while True:
        meta: list[str] = []
        if choices:
            meta.append(f"choices={list(choices)}")
        if show_default and default not in (None, argparse.SUPPRESS):
            meta.append(f"default={default!r}")
        meta_str = f" ({', '.join(meta)})" if meta else ""

        prompt = f"{flag}{meta_str}"
        if help_text:
            prompt += f"\n  {help_text}\n> "
        else:
            prompt += ": "

        raw = _input_line(prompt).strip()
        if raw == "" and not required:
            return None
        if raw == "" and required:
            print("This value is required.")
            continue

        if isinstance(action, argparse.BooleanOptionalAction):
            # Accept y/n, true/false, 1/0.
            val = raw.lower()
            if val in {"y", "yes", "true", "1"}:
                return True
            if val in {"n", "no", "false", "0"}:
                return False
            print("Enter y/n (or true/false).")
            continue

        if getattr(action, "nargs", None) in {"+", "*"} or type(action).__name__ == "_AppendAction":
            parts = [p.strip() for p in raw.split(",") if p.strip()]
            if required and not parts:
                print("Provide at least one value (comma-separated).")
                continue
            casted: list[Any] = []
            for p in parts:
                try:
                    casted.append(_parse_typed(p, action))
                except ValueError as e:
                    print(f"Invalid value: {e}")
                    casted = []
                    break
            if casted:
                return casted
            continue

        try:
            val = _parse_typed(raw, action)
        except ValueError as e:
            print(f"Invalid value: {e}")
            continue

        if choices and val not in choices:
            print(f"Value must be one of: {list(choices)}")
            continue
        return val


def _action_is_boolean(action: argparse.Action) -> bool:
    if isinstance(action, argparse.BooleanOptionalAction):
        return True
    cls = type(action).__name__
    return cls in {"_StoreTrueAction", "_StoreFalseAction"}


def _boolean_default(action: argparse.Action) -> bool:
    d = getattr(action, "default", None)
    return bool(d) if d is not None else False


def _build_argv_for_subcommand(subparser: argparse.ArgumentParser, subcommand: str) -> list[str]:
    actions = [a for a in subparser._actions if not _is_help_action(a)]

    required_actions: list[argparse.Action] = []
    optional_actions: list[argparse.Action] = []

    for a in actions:
        if _is_subparser_action(a):
            continue
        if not a.option_strings:
            # positional
            required_actions.append(a)
            continue
        if getattr(a, "required", False):
            required_actions.append(a)
        else:
            optional_actions.append(a)

    argv: list[str] = [subcommand]

    # Required first
    if required_actions:
        print()
        print("Required flags")
        print("--------------")
    for a in required_actions:
        if _action_is_boolean(a):
            chosen = _prompt_yes_no(f"{_display_flag(a)}?", default=_boolean_default(a))
            _append_boolean(argv, a, chosen)
            continue
        required = True
        val = _prompt_value(a, required=required, show_default=False)
        _append_value(argv, a, val)

    if optional_actions:
        print()
        print("Optional flags")
        print("--------------")
        if _prompt_yes_no("Configure optional flags?", default=False):
            for a in optional_actions:
                if _action_is_boolean(a):
                    chosen = _prompt_yes_no(f"{_display_flag(a)}?", default=_boolean_default(a))
                    _append_boolean(argv, a, chosen, include_when_default=False)
                    continue
                val = _prompt_value(a, required=False, show_default=True)
                if val is None:
                    continue
                _append_value(argv, a, val)

    return argv


def _append_boolean(argv: list[str], action: argparse.Action, chosen: bool, *, include_when_default: bool = True) -> None:
    default = _boolean_default(action)
    if not include_when_default and chosen == default:
        return

    if isinstance(action, argparse.BooleanOptionalAction):
        positives = [s for s in action.option_strings if s.startswith("--") and not s.startswith("--no-")]
        negatives = [s for s in action.option_strings if s.startswith("--no-")]
        pos = positives[0] if positives else action.option_strings[0]
        neg = negatives[0] if negatives else f"--no-{pos.lstrip('-')}"
        argv.append(pos if chosen else neg)
        return

    # store_true/store_false
    cls = type(action).__name__
    flag = _display_flag(action)
    if cls == "_StoreTrueAction":
        if chosen:
            argv.append(flag)
        return
    if cls == "_StoreFalseAction":
        if not chosen:
            argv.append(flag)
        return


def _append_value(argv: list[str], action: argparse.Action, val: Any) -> None:
    if val is None:
        return
    if not action.option_strings:
        if isinstance(val, list):
            argv.extend([str(v) for v in val])
        else:
            argv.append(str(val))
        return

    flag = _display_flag(action)
    if isinstance(val, list):
        # For append/nargs, repeat the flag per value.
        for v in val:
            argv.extend([flag, str(v)])
    else:
        argv.extend([flag, str(val)])


@dataclass(frozen=True)
class _LauncherItem:
    label: str
    description: str
    run: Callable[[], int]


def _run_backfill_cli() -> int:
    from app.ingest import backfill_cli

    parser = backfill_cli.build_parser()
    sub = _get_subparsers(parser)
    cmds = list(sub.choices.keys())
    idx = _prompt_choice("Backfill CLI — choose a command:", [f"{c} — {sub.choices[c].description}" for c in cmds])
    cmd = cmds[idx]
    argv = _build_argv_for_subcommand(sub.choices[cmd], cmd)
    _confirm_or_exit(["python", "-m", "app.ingest.backfill_cli", *argv])
    return int(asyncio.run(backfill_cli.main(argv)))


def _run_score_predictions_cli() -> int:
    from app.engine import score_predictions_cli

    parser = score_predictions_cli.build_parser()
    sub = _get_subparsers(parser)
    cmds = list(sub.choices.keys())
    idx = _prompt_choice(
        "Prediction Scoring CLI — choose a command:",
        [f"{c} — {sub.choices[c].description}" for c in cmds],
    )
    cmd = cmds[idx]
    argv = _build_argv_for_subcommand(sub.choices[cmd], cmd)
    _confirm_or_exit(["python", "-m", "app.engine.score_predictions_cli", *argv])
    return int(score_predictions_cli.main(argv))


def _run_demo_script() -> int:
    script = _REPO_ROOT / "scripts" / "demo_run.py"
    if not script.exists():
        print(f"Missing script: {script}")
        return 2
    # Run in a subprocess so environment/side-effects match normal usage.
    _confirm_or_exit([sys.executable, str(script)])
    proc = subprocess.run([sys.executable, str(script)], cwd=str(_REPO_ROOT))
    return int(proc.returncode)


def _print_underlying_help() -> int:
    print()
    print("Direct entrypoints (non-interactive)")
    print("---------------------------------")
    print("  python -m app.ingest.backfill_cli --help")
    print("  python -m app.engine.score_predictions_cli --help")
    print("  python scripts/demo_run.py")
    print()
    return 0


def _confirm_or_exit(cmd: list[str]) -> None:
    cmd_str = " ".join([_shell_quote(x) for x in cmd])
    print()
    print("About to run")
    print("------------")
    print(cmd_str)
    if not _prompt_yes_no("Run this command now?", default=True):
        raise SystemExit(0)
    _animate_line("Launching", ["-", "--", "---", "----"], seconds=0.6, interval=0.12)


def _shell_quote(s: str) -> str:
    v = str(s)
    if v == "":
        return '""'
    if any(ch.isspace() for ch in v) or '"' in v:
        return '"' + v.replace('"', '\\"') + '"'
    return v


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Alpha Engine interactive CLI launcher",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python start.py\n"
            "  python start.py --list\n"
            "  python start.py --no-welcome\n"
            "\n"
            "Direct (non-interactive) entrypoints:\n"
            "  python -m app.ingest.backfill_cli --help\n"
            "  python -m app.engine.score_predictions_cli --help\n"
        ),
    )
    p.add_argument("--no-welcome", action="store_true", help="Skip welcome banner")
    p.add_argument("--no-anim", action="store_true", help="Disable the cute startup animation")
    p.add_argument("--list", action="store_true", help="List launcher tools and exit")
    p.add_argument("--help-direct", action="store_true", help="Show underlying direct entrypoints and exit")
    return p


def main(argv: list[str] | None = None) -> int:
    global _ANIM_ENABLED
    if str(_REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(_REPO_ROOT))

    args = build_parser().parse_args(argv)
    _ANIM_ENABLED = (not bool(args.no_anim)) and _supports_animation()
    if args.list:
        for item in _launcher_items():
            print(f"- {item.label}: {item.description}")
        return 0
    if args.help_direct:
        return _print_underlying_help()

    if not args.no_welcome:
        _welcome()

    items = _launcher_items()
    idx = _prompt_choice(
        "What do you want to run?",
        [f"{it.label} — {it.description}" for it in items] + ["Exit"],
    )
    if idx == len(items):
        return 0
    return int(items[idx].run())


def _launcher_items() -> list[_LauncherItem]:
    return [
        _LauncherItem(
            label="Backfill + Target Stocks",
            description="Ingest/backfill data and manage target universe",
            run=_run_backfill_cli,
        ),
        _LauncherItem(
            label="Scoring + Rankings",
            description="Score predictions, rank strategies, promotions",
            run=_run_score_predictions_cli,
        ),
        _LauncherItem(
            label="Full Pipeline Run (recommended)",
            description="Backfill -> generate predictions -> score -> rank -> select champion",
            run=_run_full_pipeline_run,
        ),
        _LauncherItem(
            label="Demo Pipeline",
            description="Run scripts/demo_run.py end-to-end",
            run=_run_demo_script,
        ),
        _LauncherItem(
            label="Help (Direct Commands)",
            description="Show the non-interactive module/script entrypoints",
            run=_print_underlying_help,
        ),
    ]


def _stability_label(samples: int) -> str:
    n = int(samples)
    if n >= 100:
        return "High"
    if n >= 30:
        return "Medium"
    return "Low"


def _run_full_pipeline_run() -> int:
    from app.ingest.backfill_runner import BackfillRunner
    from app.db.repository import AlphaRepository
    from app.engine.predicted_series_builder import BuildConfig, PredictedSeriesBuilder
    from app.engine.prediction_scoring_runner import PredictionScoringRunner

    print()
    print("Full Pipeline Run")
    print("-----------------")

    state = _load_state() or {}
    use_state = False
    if state.get("full_pipeline"):
        last = state.get("full_pipeline") or {}
        try:
            print("Last run:")
            print(f"  ticker: {last.get('ticker')}")
            print(f"  ingress: {last.get('ingress_range')}")
            print(f"  prediction: {last.get('prediction_range')}")
            print()
            use_state = _prompt_yes_no("Reuse last windows + ticker?", default=True)
        except Exception:
            use_state = False

    if use_state:
        last = state.get("full_pipeline") or {}
        ticker = str(last.get("ticker") or "NVDA").strip().upper()
        timeframe = str(last.get("timeframe") or "1d").strip()
        tenant_id = str(last.get("tenant_id") or "backfill").strip()
        db_path = str(last.get("db") or "data/alpha.db").strip()
        ingress_range = str(last.get("ingress_range") or "").strip()
        prediction_range = str(last.get("prediction_range") or "").strip()
    else:
        ticker_raw = _input_line("Ticker (default NVDA; or type ALL): ").strip()
        ticker = (ticker_raw or "NVDA").strip().upper()
        timeframe = "1d"
        tenant_id = "backfill"
        db_path = "data/alpha.db"

        while True:
            prediction_range = _input_line("Prediction range (YYYY-MM-DD:YYYY-MM-DD): ").strip()
            if prediction_range.lower() in {"q", "quit", "exit"}:
                raise SystemExit(0)
            try:
                _parse_range(prediction_range)
                break
            except Exception as e:
                print(f"Invalid range: {e}")

        ingress_range = _input_line("Ingress range (YYYY-MM-DD:YYYY-MM-DD, blank = 30d before prediction start): ").strip()
        if ingress_range.lower() in {"q", "quit", "exit"}:
            raise SystemExit(0)
        if not ingress_range:
            pred_start, _pred_end = _parse_range(prediction_range)
            dt_start = datetime.fromisoformat(pred_start.replace("Z", "+00:00"))
            ing_end = _isoz(dt_start - timedelta(seconds=1))
            ing_start = _isoz(dt_start - timedelta(days=30))
            ingress_range = f"{ing_start[:10]}:{ing_end[:10]}"
        else:
            try:
                _parse_range(ingress_range)
            except Exception as e:
                print(f"Invalid ingress range: {e}")
                return 2

    ing_start, ing_end = _parse_range(ingress_range)
    pred_start, pred_end = _parse_range(prediction_range)

    # Preflight: verify bars provider exists before running a long backfill.
    provider = _preflight_bars_provider()
    if provider is None:
        return 2
    print(f"Historical bars provider: {provider}")

    # 1) Backfill + replay (generates predictions + (best-effort) consensus signals)
    print()
    print("Step 1/5  Backfill")
    print("---------------")
    print(f"ingress:   {ing_start} -> {ing_end}")
    print(f"db:        {db_path}")
    print()

    start_dt = datetime.fromisoformat(ing_start.replace("Z", "+00:00"))
    end_dt_inclusive = datetime.fromisoformat(ing_end.replace("Z", "+00:00"))
    end_dt_exclusive = end_dt_inclusive + timedelta(seconds=1)

    _confirm_or_exit(["python", "-m", "app.ingest.backfill_cli", "backfill-range", "--start", ing_start[:10], "--end", ing_end[:10]])
    asyncio.run(
        BackfillRunner(db_path=db_path).backfill_range(
            start_time=start_dt,
            end_time=end_dt_exclusive,
            replay=True,
            skip_completed=True,
        )
    )

    # 2) (Best-effort) verify predictions exist for ticker / window
    print()
    print("Step 2/5  Generate predictions")
    print("------------------------------")
    repo = AlphaRepository(db_path)
    try:
        if ticker != "ALL":
            row = repo.conn.execute(
                """
                SELECT COUNT(*) as n
                FROM predictions
                WHERE tenant_id = ?
                  AND ticker = ?
                  AND timestamp >= ?
                  AND timestamp <= ?
                """,
                (tenant_id, ticker, ing_start, ing_end),
            ).fetchone()
            n = int(row["n"]) if row is not None and "n" in row.keys() else int(row[0] if row else 0)
            print(f"predictions found in ingress window: {n}")
            if n == 0:
                print("warning: no predictions found for this ticker in the ingress window; scoring may fall back or be empty.")
        else:
            print("ticker=ALL (skipping prediction presence check)")

        # 3) Build predicted series
        print()
        print("Step 3/5  Build predicted series")
        print("-------------------------------")
        run_id = repo.create_prediction_run(
            tenant_id=tenant_id,
            timeframe=timeframe,
            regime=None,
            ingress_start=ing_start,
            ingress_end=ing_end,
            prediction_start=pred_start,
            prediction_end=pred_end,
        )
        builder = PredictedSeriesBuilder(repository=repo)
        cfg = BuildConfig(
            model="directional_drift",
            signal_source="consensus",
            cap_daily_return=0.05,
            vol_lookback=20,
            skip_if_exists=True,
            tenant_id=tenant_id,
        )
        tickers = None if ticker == "ALL" else [ticker]
        build_results = builder.build_for_run(run_id=str(run_id), tickers=tickers, config=cfg)
        built = sum(1 for r in build_results if not r.skipped)
        skipped = sum(1 for r in build_results if r.skipped)
        print(f"run_id={run_id} built={built} skipped={skipped}")

        # 4) Score predictions
        print()
        print("Step 4/5  Score predictions")
        print("---------------------------")
        scorer = PredictionScoringRunner(repository=repo)
        scored_rows = scorer.score_run(
            run_id=str(run_id),
            tenant_id=str(tenant_id),
            ticker=None if ticker == "ALL" else ticker,
            timeframe=str(timeframe),
            materialize_actual=True,
            autobuild_predicted_series=False,
        )
        print(f"series_scored={len(scored_rows)}")

        # 5) Rank + select champion
        print()
        print("Step 5/5  Rank + champion")
        print("-------------------------")
        if ticker == "ALL":
            print("ticker=ALL: skipping champion selection (champions are per-ticker).")
            return 0

        ranked = repo.rank_strategies(
            tenant_id=tenant_id,
            ticker=ticker,
            timeframe=timeframe,
            min_samples=1,
            min_total_forecast_days=0,
            limit=5,
        )
        if not ranked:
            print("No ranked strategies found (did scoring produce rows?).")
            return 2

        champ = repo.select_efficiency_champion(
            tenant_id=tenant_id,
            ticker=ticker,
            timeframe=timeframe,
            min_samples=10,
            min_total_forecast_days=0,
        ) or ranked[0]

        champ_id = str(champ["strategy_id"])
        alpha = float(champ["avg_efficiency_rating"])
        samples = int(champ["samples"])
        total_days = int(champ.get("total_forecast_days") or 0)

        repo.upsert_efficiency_champion_record(
            tenant_id=tenant_id,
            ticker=ticker,
            timeframe=timeframe,
            forecast_days=None,
            regime=None,
            strategy_id=champ_id,
            strategy_version=(str(champ.get("strategy_version") or "") or None),
            avg_efficiency_rating=alpha,
            samples=samples,
            total_forecast_days=total_days,
        )

        print()
        print("Summary")
        print("-------")
        print(f"Champion:  {champ_id}")
        print(f"Ticker:    {ticker}")
        print(f"Alpha:     {alpha:.2f}")
        print(f"Samples:   {samples}")
        print(f"Stability: {_stability_label(samples)}")
        print()

        _save_state(
            {
                **(state or {}),
                "full_pipeline": {
                    "ticker": ticker,
                    "timeframe": timeframe,
                    "tenant_id": tenant_id,
                    "db": db_path,
                    "ingress_range": ingress_range,
                    "prediction_range": prediction_range,
                    "saved_at": _isoz(datetime.now(timezone.utc)),
                },
            }
        )
        return 0
    finally:
        try:
            repo.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
