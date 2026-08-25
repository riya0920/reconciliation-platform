#!/bin/bash
# Install the daily reconciliation DAG as a real systemd timer.
#
#     sudo ops/install_timers.sh /path/to/data1-recon-platform
#
# `run_dag.py` runs the real pipeline through a DAG with dependencies, retries
# and an SLA. Nothing invoked it. The README argued a scheduler embedded in the
# application is one nobody can inspect, pause or back-fill from -- that
# argument is right and this stays OUTSIDE the application.
#
# HOW THIS DIFFERS FROM ML-1's TIMER, and the difference is the whole point:
#
#   ML-1 monitoring     Persistent=true, and missed runs COALESCE. Monitoring is
#                       not cumulative -- what matters is the answer now, and
#                       replaying three days of it pages for drift that has
#                       already resolved.
#
#   THIS reconciliation Persistent=false, and missed dates are NOT run by the
#                       timer at all. Settlement state IS cumulative: a later
#                       date computed against an uncorrected predecessor
#                       finishes, reports success, and is wrong. Catching up is
#                       `run_backfill.py`'s job, oldest-first and resumable,
#                       and it is a DIFFERENT job because it is a bulk overwrite
#                       of published figures that may need approval.
#
# A timer that quietly back-filled would bypass the sign-off control in
# src/signoff.py entirely. That is the reason for the flag, not tidiness.
set -euo pipefail

REPO="${1:-}"
if [ -z "$REPO" ] || [ ! -f "$REPO/run_dag.py" ]; then
  echo "usage: $0 /path/to/data1-recon-platform" >&2
  exit 2
fi

PY="${PYTHON:-python3}"
if ! "$PY" -c 'import duckdb, pandas' >/dev/null 2>&1; then
  for cand in /mnt/c/Python314/python.exe /mnt/c/Python313/python.exe /mnt/c/Python312/python.exe; do
    if [ -x "$cand" ] && "$cand" -c 'import duckdb, pandas' >/dev/null 2>&1; then
      PY="$cand"; break
    fi
  done
fi
if ! "$PY" -c 'import duckdb, pandas' >/dev/null 2>&1; then
  echo "no interpreter with duckdb/pandas found; set PYTHON=..." >&2
  echo "Refusing to install a timer that would fail on every fire." >&2
  exit 3
fi
echo "using interpreter: $PY"

# A Windows interpreter needs a Windows path, and systemd treats backslash in
# ExecStart as an escape -- so forward slashes.
SCRIPT="$REPO/run_dag_tick.py"
case "$PY" in
  *.exe) SCRIPT="$(wslpath -w "$REPO/run_dag_tick.py" | tr '\\' '/')" ;;
esac

cat > /etc/systemd/system/data1-recon.service <<EOF
[Unit]
Description=DATA-1 daily reconciliation DAG
Documentation=file://$REPO/README.md

[Service]
Type=oneshot
WorkingDirectory=$REPO
ExecStart=$PY $SCRIPT
# 20 = the DAG ran and the SLA was breached. A real signal, not a crash: the
# run produced correct output and took too long, and a unit marked failed for
# that trains an operator to ignore the colour.
SuccessExitStatus=0 20
EOF

cat > /etc/systemd/system/data1-recon.timer <<'EOF'
[Unit]
Description=Run the DATA-1 reconciliation after the settlement cutoff

[Timer]
# After the cutoff, not at midnight. A reconciliation that runs before the
# file lands reconciles yesterday's file against today's date and reports a
# clean break-free run, which is the most convincing wrong answer available.
OnCalendar=*-*-* 19:30:00
# NOT persistent -- see the header. Settlement state is cumulative and catching
# up is run_backfill.py's job, oldest-first, resumable and subject to approval.
Persistent=false
RandomizedDelaySec=120
AccuracySec=1min
Unit=data1-recon.service

[Install]
WantedBy=timers.target
EOF

systemctl daemon-reload
systemctl enable --now data1-recon.timer
echo "installed. next run:"
systemctl list-timers data1-recon.timer --no-pager
