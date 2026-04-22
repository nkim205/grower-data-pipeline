"""
Stdout formatting for S3 retrieve: per-file utilization tables and aggregate byte stats.
"""

from __future__ import annotations

from typing import Any

# Column widths for utilization table (monospace scan)
_W_FILE = 48
_W_FMB = 7
_W_ROWS = 8
_W_TGT = 7
_W_UTIL = 7
_W_PULLED = 10
_W_FLAG = 4


def _format_pulled_bytes(n: int) -> str:
    if n >= 1024 * 1024:
        return f"{n / (1024 * 1024):.2f} MiB"
    if n >= 1024:
        return f"{n / 1024:.1f} KiB"
    return f"{n} B"


def _trunc_file(name: str, width: int = _W_FILE) -> str:
    if len(name) <= width:
        return name
    if width < 3:
        return name[:width]
    keep = (width - 2) // 2
    return f"{name[:keep]}..{name[-(width - 2 - keep):]}"


def _utilization_table_lines(records: list[dict[str, Any]]) -> list[str]:
    def sort_key(r: dict[str, Any]) -> str:
        s = r["key"].split("/")[-1] if "/" in r["key"] else r["key"]
        return s.lower()

    out: list[str] = []
    for r in sorted(records, key=sort_key):
        short = r["key"].split("/")[-1] if "/" in r["key"] else r["key"]
        f_mb = r["f_size"] / (1024 * 1024)
        ru = r["row_utilization"]
        ru_s = f"{ru:>6.1%}" if isinstance(ru, (int, float)) else "   n/a"
        note = (r.get("note") or "")[:1].upper() if r.get("note") else ""
        if note == "H":
            flag = "hi"
        elif note == "L":
            flag = "lo"
        else:
            flag = ""
        line = (
            f"{_trunc_file(short):<{_W_FILE}} "
            f"{f_mb:>{_W_FMB}.1f} "
            f"{r['n_chunk']:>{_W_ROWS}d} "
            f"{r['n_target_rows']:>{_W_TGT}d} "
            f"{ru_s:>{_W_UTIL}} "
            f"{_format_pulled_bytes(r['bytes_pulled']):>{_W_PULLED}} "
            f"{flag:^{_W_FLAG}}"
        )
        out.append(line)
    return out


def _print_utilization_table_header() -> None:
    ind = "  "
    hdr = (
        f"{ind}{'file':<{_W_FILE}} "
        f"{'f_MB':>{_W_FMB}} "
        f"{'rows':>{_W_ROWS}} "
        f"{'target':>{_W_TGT}} "
        f"{'util':>{_W_UTIL}} "
        f"{'pulled':>{_W_PULLED}} "
        f"{'flg':^{_W_FLAG}}"
    )
    print(hdr)
    print(
        f"{ind}{'-' * _W_FILE} "
        f"{'-' * _W_FMB} "
        f"{'-' * _W_ROWS} "
        f"{'-' * _W_TGT} "
        f"{'-' * _W_UTIL} "
        f"{'-' * _W_PULLED} "
        f"{'-' * _W_FLAG}"
    )


def print_utilization_report(
    state: str,
    partial_records: list[dict[str, Any]],
    full_records: list[dict[str, Any]],
    util_high: float,
    util_log_low: float,
) -> None:
    """
    Aligned tables: partial (head+tail) and full-file pulls, sorted by file name.
    Columns: file, f_MB, rows, target, util, pulled, flg (hi/lo/blank).
    """
    if not partial_records and not full_records:
        return
    st = state.upper()
    print(f"[{st}] Utilization  |  flg: hi => util>={util_high:.0%} (tail risk)  lo => <={util_log_low:.0%} (waste)")

    if partial_records:
        print(f"\n  partial (head+tail)  ({len(partial_records)} file(s))")
        _print_utilization_table_header()
        for line in _utilization_table_lines(partial_records):
            print("  " + line)

    if full_records:
        print(f"\n  full file download  ({len(full_records)} file(s), tail setting N/A)")
        _print_utilization_table_header()
        for line in _utilization_table_lines(full_records):
            print("  " + line)
    print()


def print_retrieve_bytes_summary(state: str, bytes_available: int, bytes_downloaded: int) -> None:
    """Scanned S3 size vs bytes actually downloaded, with reduction % when applicable."""
    mb = lambda b: round(b / (1024 * 1024), 2)
    st = state.upper()
    print(f"[{st}] Files scanned: {bytes_available / (1024*1024):.1f} MB available")
    print(f"[{st}] Actually downloaded: {mb(bytes_downloaded)} MB")
    if bytes_available > 0:
        print(f"[{st}] Reduction: {100 - (bytes_downloaded / bytes_available * 100):.1f}%")
