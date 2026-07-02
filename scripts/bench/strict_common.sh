# shellcheck shell=bash
#
# Σ.AI.3 — shared discipline helpers for the strict bench scripts.
# Sourced by strict_22q.sh / strict_ab.sh / strict_throughput.sh.
#
# Provides:
#   thermal_wait [MAX_WAIT_SEC]   — block until the SoC reports no speed
#                                   limit (pmset -g therm CPU_Speed_Limit
#                                   == 100), or warn and proceed after
#                                   MAX_WAIT_SEC (default 120).
#   thermal_state                 — one-line current thermal state.
#   capture_env OUT_DIR KEY=VAL...— write OUT_DIR/env.json with machine,
#                                   git, engine-version and flag metadata
#                                   plus any extra KEY=VALs given.
#   apply_cache_policy POLICY     — 'warm' is a no-op; 'cold' runs
#                                   /usr/bin/purge via passwordless sudo
#                                   or fails loudly with setup steps.
#
# Rationale: benchmark verdicts were drifting across runs because no run
# recorded WHICH machine, flags, power state, or thermal condition it ran
# under. Results from an M3 Pro and an M4 Max were compared as if
# same-hardware. These helpers make every strict run self-describing.

# Emit the current thermal state. Intel Macs report CPU_Speed_Limit=NN;
# Apple Silicon reports warning-level notes instead ("No thermal warning
# level has been recorded" == clean). Output is one of:
#   CPU_Speed_Limit=NN | warning-level=N | nominal | unavailable
thermal_state() {
    local out line
    out="$(pmset -g therm 2>/dev/null || true)"
    if [[ -z "$out" ]]; then
        echo "unavailable"
        return
    fi
    line="$(grep -E 'CPU_Speed_Limit' <<<"$out" | head -1 || true)"
    if [[ -n "$line" ]]; then
        echo "$line" | tr -d ' \t' # e.g. CPU_Speed_Limit=100
        return
    fi
    line="$(grep -iE 'thermal warning level = [0-9]+' <<<"$out" | head -1 || true)"
    if [[ -n "$line" ]]; then
        echo "warning-level=$(grep -oE '[0-9]+' <<<"$line" | head -1)"
        return
    fi
    if grep -qi 'No thermal warning level has been recorded' <<<"$out"; then
        echo "nominal" # Apple Silicon: no throttle event on record
        return
    fi
    echo "unavailable"
}

# True (0) when the reported state means "not throttled".
thermal_clean() {
    case "$(thermal_state)" in
        nominal|*CPU_Speed_Limit=100|warning-level=0|unavailable) return 0 ;;
        *) return 1 ;;
    esac
}

# Block until thermally clean. On machines/OSes where pmset reports
# nothing usable, warn once and return immediately.
thermal_wait() {
    local max_wait="${1:-120}"
    local waited=0
    local state
    state="$(thermal_state)"
    if [[ "$state" == "unavailable" ]]; then
        echo "  [thermal] pmset reports no usable thermal state on this machine — proceeding unguarded" >&2
        return 0
    fi
    while ! thermal_clean; do
        state="$(thermal_state)"
        if (( waited >= max_wait )); then
            echo "  [thermal] WARNING: still throttled after ${max_wait}s ($state) — proceeding; results suspect" >&2
            return 0
        fi
        echo "  [thermal] throttled ($state) — waiting 5s (${waited}/${max_wait}s)"
        sleep 5
        waited=$((waited + 5))
    done
    return 0
}

# capture_env OUT_DIR [KEY=VAL ...]
# Writes OUT_DIR/env.json. Extra KEY=VALs are recorded under "run".
# Repo root comes from the caller's $REPO (all strict_* scripts set it);
# falls back to git toplevel from CWD. BASH_SOURCE is unreliable here —
# it resolves relative to the CWD at source time, not call time.
capture_env() {
    local out_dir="$1"
    shift
    local repo
    repo="${REPO:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"
    mkdir -p "$out_dir"
    EXTRA_KV="$*" REPO_DIR="$repo" OUT_FILE="$out_dir/env.json" python3 - <<'PYEOF'
import json, os, re, subprocess, sys

def sh(cmd):
    try:
        return subprocess.run(cmd, shell=True, capture_output=True, text=True,
                              timeout=10).stdout.strip()
    except Exception:
        return ""

repo = os.environ["REPO_DIR"]

def lock_version(name):
    """First version for `name` in Cargo.lock (registry deps)."""
    try:
        text = open(os.path.join(repo, "Cargo.lock")).read()
    except OSError:
        return None
    m = re.search(rf'name = "{re.escape(name)}"\nversion = "([^"]+)"', text)
    return m.group(1) if m else None

extra = {}
for kv in os.environ.get("EXTRA_KV", "").split():
    if "=" in kv:
        k, v = kv.split("=", 1)
        extra[k] = v

env = {
    "chip": sh("sysctl -n machdep.cpu.brand_string"),
    "perf_cores": sh("sysctl -n hw.perflevel0.physicalcpu"),
    "efficiency_cores": sh("sysctl -n hw.perflevel1.physicalcpu"),
    "ram_bytes": sh("sysctl -n hw.memsize"),
    "macos": sh("sw_vers -productVersion"),
    "power_source": sh("pmset -g batt | head -1"),
    "thermal": sh("pmset -g therm | grep CPU_Speed_Limit | head -1").strip(),
    "git_sha": sh(f"git -C '{repo}' rev-parse HEAD"),
    "git_branch": sh(f"git -C '{repo}' rev-parse --abbrev-ref HEAD"),
    "git_dirty": bool(sh(f"git -C '{repo}' status --porcelain")),
    "rustc": sh("rustc --version"),
    "engine_versions": {
        "duckdb": lock_version("duckdb"),
        "polars": lock_version("polars"),
        "datafusion": lock_version("datafusion"),
        "ematix-parquet-codec": lock_version("ematix-parquet-codec"),
    },
    "emat_env": {k: v for k, v in os.environ.items()
                 if k.startswith(("EMAT_", "TPCH_", "PARTITIONS"))},
    "run": extra,
}
with open(os.environ["OUT_FILE"], "w") as f:
    json.dump(env, f, indent=2, sort_keys=True)
print(f"  [env] captured -> {os.environ['OUT_FILE']}")
PYEOF
}

# apply_cache_policy warm|cold
# 'cold' purges the filesystem page cache before the caller's invocation.
apply_cache_policy() {
    local policy="$1"
    case "$policy" in
        warm)
            : # discard-first discipline handles warm-cache reporting
            ;;
        cold)
            if sudo -n /usr/bin/purge 2>/dev/null; then
                echo "  [cache] purged page cache (cold policy)"
            else
                cat >&2 <<'EOF'
ERROR: --cache-policy cold requires passwordless sudo for /usr/bin/purge.
Grant it with visudo, adding a line like:
    <your-user> ALL=(root) NOPASSWD: /usr/bin/purge
Or rerun with --cache-policy warm (results reported as warm-cache).
EOF
                return 1
            fi
            ;;
        *)
            echo "ERROR: unknown cache policy '$policy' (warm|cold)" >&2
            return 1
            ;;
    esac
}
