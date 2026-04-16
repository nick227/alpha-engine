from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import app.cli.start as start


def test_parse_date_or_iso_accepts_date_and_iso() -> None:
    assert start._parse_date_or_iso("2026-01-15") == "2026-01-15T00:00:00Z"  # noqa: SLF001
    assert start._parse_date_or_iso("2026-01-15", end_of_day=True) == "2026-01-15T23:59:59Z"  # noqa: SLF001

    # ISO with Z suffix.
    assert start._parse_date_or_iso("2026-01-15T14:30:00Z") == "2026-01-15T14:30:00Z"  # noqa: SLF001


def test_parse_range_parses_and_sets_end_of_day() -> None:
    a, b = start._parse_range("2026-01-01:2026-01-05")  # noqa: SLF001
    assert a == "2026-01-01T00:00:00Z"
    assert b == "2026-01-05T23:59:59Z"


def test_shell_quote_quotes_spaces_and_escapes_quotes() -> None:
    assert start._shell_quote("") == '""'  # noqa: SLF001
    assert start._shell_quote("abc") == "abc"  # noqa: SLF001
    assert start._shell_quote("a b") == '"a b"'  # noqa: SLF001
    assert start._shell_quote('a"b') == '"a\\"b"'  # noqa: SLF001


def test_stability_label_buckets() -> None:
    assert start._stability_label(10) == "Low"  # noqa: SLF001
    assert start._stability_label(30) == "Medium"  # noqa: SLF001
    assert start._stability_label(99) == "Medium"  # noqa: SLF001
    assert start._stability_label(100) == "High"  # noqa: SLF001


def test_state_roundtrip_uses_overridden_path(tmp_path, monkeypatch) -> None:
    state_path = tmp_path / "cli_state.json"
    monkeypatch.setattr(start, "_STATE_PATH", state_path)

    payload = {"saved_at": start._isoz(datetime(2026, 1, 1, tzinfo=timezone.utc)), "x": 1}  # noqa: SLF001
    start._save_state(payload)  # noqa: SLF001
    loaded = start._load_state()  # noqa: SLF001
    assert loaded == payload


def test_main_list_does_not_prompt(monkeypatch, capsys) -> None:
    # Ensure --list path is non-interactive.
    monkeypatch.setattr(start, "_launcher_items", lambda: [start._LauncherItem("X", "Y", lambda: 0)])  # noqa: SLF001
    rc = start.main(["--no-welcome", "--list"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "- X: Y" in out


def test_main_help_direct_calls_print_underlying_help(monkeypatch) -> None:
    monkeypatch.setattr(start, "_print_underlying_help", lambda: 42)
    rc = start.main(["--help-direct"])
    assert rc == 42


def test_argparse_helpers_display_flag_and_subparsers() -> None:
    p = argparse.ArgumentParser(add_help=True)
    p.add_argument("--alpha", "-a", help="x")
    act = next(a for a in p._actions if getattr(a, "dest", None) == "alpha")
    assert start._display_flag(act) == "--alpha"  # noqa: SLF001
    assert start._is_help_action(next(a for a in p._actions if getattr(a, "dest", None) == "help"))  # noqa: SLF001

    sub = p.add_subparsers(dest="cmd")
    sub.add_parser("run")
    sp = start._get_subparsers(p)  # noqa: SLF001
    assert isinstance(sp, argparse._SubParsersAction)
    assert "run" in sp.choices


def test_parse_typed_converts_and_quit_exits() -> None:
    p = argparse.ArgumentParser(add_help=False)
    act_int = p.add_argument("--n", type=int)
    assert start._parse_typed("3", act_int) == 3  # noqa: SLF001
    assert start._parse_typed("", act_int) is None  # noqa: SLF001
    try:
        start._parse_typed("quit", act_int)  # noqa: SLF001
        assert False, "expected SystemExit"
    except SystemExit as e:
        assert int(getattr(e, "code", 0) or 0) == 0


def test_append_boolean_and_values_cover_common_actions() -> None:
    p = argparse.ArgumentParser(add_help=False)
    bool_opt = p.add_argument("--flag", action=argparse.BooleanOptionalAction, default=True)
    store_true = p.add_argument("--v", action="store_true")
    append = p.add_argument("--tag", action="append")
    pos = p.add_argument("file")

    argv: list[str] = []
    start._append_boolean(argv, bool_opt, chosen=False)  # noqa: SLF001
    assert "--no-flag" in argv

    argv2: list[str] = []
    start._append_boolean(argv2, store_true, chosen=True)  # noqa: SLF001
    assert "--v" in argv2

    argv3: list[str] = []
    start._append_value(argv3, append, ["a", "b"])  # noqa: SLF001
    assert argv3 == ["--tag", "a", "--tag", "b"]

    argv4: list[str] = []
    start._append_value(argv4, pos, "x.txt")  # noqa: SLF001
    assert argv4 == ["x.txt"]
