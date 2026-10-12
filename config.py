"""
Configuration for the NiFi self-healing pipeline.

Everything is read from the environment, so a CAI Application, a CAI
session and a laptop can share one code path. Variables are prefixed
`SELFHEAL_`; the LLM and embedding endpoints use the names the CAI
Inference Service documentation uses.

Two settings decide how dangerous a run is, and both default to safe:

    SELFHEAL_DF_CLIENT  mock | live    mock simulates a CDF tenant locally
    SELFHEAL_ARMED      false | true   false turns every write into a dry run

`SELFHEAL_ARMED` can only be set in the environment. The UI shows it but
cannot change it. It defaults to false against a live tenant and to true
against the mock, whose writes only change a local JSON file (so the
demo shows fixes landing); set it to false to rehearse dry runs.
"""

import os


def _bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)

    if value is None:
        return default

    return value.strip().lower() in ("1", "true", "yes", "on")


def _int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))

    except ValueError:
        return default


def _float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))

    except ValueError:
        return default


def _list(name: str) -> tuple:
    return tuple(
        item.strip()
        for item in os.environ.get(name, "").split(",")
        if item.strip()
    )


REPO_ROOT = os.path.dirname(os.path.abspath(__file__))

# ---------------------------------------------------------------------
# Where state lives
# ---------------------------------------------------------------------
#
# /home/cdsw is the CAI project filesystem, so anything written there
# survives application restarts. Off-platform the same layout is created
# under ./var.

_CAI_HOME = "/home/cdsw"
_ON_CAI = os.path.isdir(_CAI_HOME) and os.access(_CAI_HOME, os.W_OK)

DATA_DIR = os.environ.get(
    "SELFHEAL_DATA_DIR",
    os.path.join(_CAI_HOME, "data") if _ON_CAI else os.path.join(REPO_ROOT, "var", "data"),
)

CHROMA_DIR = os.environ.get(
    "SELFHEAL_CHROMA_DIR",
    os.path.join(_CAI_HOME, "chroma") if _ON_CAI else os.path.join(REPO_ROOT, "var", "chroma"),
)

DF_CLIENT = os.environ.get("SELFHEAL_DF_CLIENT", "mock").strip().lower()
MOCK = DF_CLIENT != "live"

# Mock and live never share a fleet, a store or graph checkpoints: a
# mock CRN in the live fleet file would be a guardrail expectation that
# can never match, and mock incidents would pollute the live history.
_SUFFIX = ".mock" if MOCK else ""

DB_PATH = os.path.join(DATA_DIR, f"healer{_SUFFIX}.db")
CHECKPOINT_PATH = os.path.join(DATA_DIR, f"checkpoints{_SUFFIX}.sqlite")
FLEET_FILE = os.path.join(DATA_DIR, f"fleet{_SUFFIX}.json")
MOCK_WORLD_FILE = os.path.join(DATA_DIR, "mock_world.json")
EDITS_DIR = os.path.join(DATA_DIR, "edits")

FLOWS_OUT_DIR = os.path.join(REPO_ROOT, "flows", "out")
PROVISION_OUT_DIR = os.path.join(REPO_ROOT, "provision", "out")
PLAYBOOKS_DIR = os.path.join(REPO_ROOT, "playbooks")
METRIC_OVERRIDES_FILE = os.path.join(PROVISION_OUT_DIR, "metric_overrides.json")

# ---------------------------------------------------------------------
# CDP / DataFlow
# ---------------------------------------------------------------------

# The mock tenant's own CRNs. In mock mode they are the defaults, so the
# guardrails check the mock exactly as they check a live service.
MOCK_SERVICE_CRN = "crn:cdp:df:us-west-1:mock:service:nfh-mock"
MOCK_ENVIRONMENT_CRN = "crn:cdp:environments:us-west-1:mock:environment:nfh-mock"

SERVICE_CRN = os.environ.get("SELFHEAL_SERVICE_CRN", MOCK_SERVICE_CRN if MOCK else "")
ENVIRONMENT_CRN = os.environ.get("SELFHEAL_ENVIRONMENT_CRN", MOCK_ENVIRONMENT_CRN if MOCK else "")

# Every flow definition and deployment this app creates carries this
# prefix, and the guardrails refuse to touch anything that does not.
PREFIX = os.environ.get("SELFHEAL_PREFIX", "nfh-")

# CDF offers NiFi 2.6.0 under this build string. It is also the bundle
# version stamped on every processor the flow builder emits.
NIFI_VERSION = os.environ.get("SELFHEAL_NIFI_VERSION", "2.6.0.4.12.0.2-1")

# Deployments that existed before this project. Never touched, even if
# a fleet file somehow names one.
DENYLIST_DEPLOYMENT_CRNS = _list("SELFHEAL_DENYLIST")

CDP_TIMEOUT_SECONDS = _int("SELFHEAL_CDP_TIMEOUT", 120)

METRICS_TIME_PERIOD = os.environ.get(
    "SELFHEAL_METRICS_TIME_PERIOD", "LAST_THIRTY_MINUTES"
)

# ---------------------------------------------------------------------
# Safety
# ---------------------------------------------------------------------

ARMED = _bool("SELFHEAL_ARMED", MOCK)

COOLDOWN_SECONDS = _int("SELFHEAL_COOLDOWN", 1800)
MAX_ACTIONS_PER_WINDOW = _int("SELFHEAL_MAX_ACTIONS_PER_WINDOW", 3)
ACTION_WINDOW_SECONDS = _int("SELFHEAL_ACTION_WINDOW", 86400)

# ---------------------------------------------------------------------
# Polling and verification
# ---------------------------------------------------------------------
#
# Tier A is cheap state (health, alert counts); tier B is the KPI charts.
# A chart has ~25 buckets per window, so reading it every second mostly
# re-reads the same numbers. Both can be set as low as 1. Intervals are
# wall seconds (they pace CLI calls); the mock's calls cost nothing and
# its clock runs MOCK_SPEED times faster, so it polls more often.

POLL_HEALTH_SECONDS = max(1, _int("SELFHEAL_POLL_HEALTH_SECONDS", 2 if MOCK else 10))
POLL_KPI_SECONDS = max(1, _int("SELFHEAL_POLL_KPI_SECONDS", 3 if MOCK else 30))

# Packs polled at once. Each live read is a 1-3 s subprocess, and tier B
# is three of them per pack.
POLL_WORKERS = max(1, _int("SELFHEAL_POLL_WORKERS", 4))

# Consecutive breaching buckets, inside one response, before a signal.
BREACH_DEBOUNCE = max(1, _int("SELFHEAL_BREACH_DEBOUNCE", 2))

# A rising queue projected to cross its threshold within this long is
# reported as a warning (context for an incident, never one by itself).
TREND_HORIZON_SECONDS = _int("SELFHEAL_TREND_HORIZON", 900)

# Conservation check (S13, S14): downstream FlowFiles per second below
# this fraction of upstream, with no queue growing to explain the gap,
# means data is being dropped.
IMBALANCE_RATIO = _float("SELFHEAL_IMBALANCE_RATIO", 0.9)

# After an incident closes, how long before the same process group can
# open another (simulated seconds in mock mode, like every tenant time).
INCIDENT_COOLDOWN_SECONDS = _int("SELFHEAL_INCIDENT_COOLDOWN", 900)

# After a change finishes: ignore this long (buckets straddling the
# change are unsound), then measure for VERIFY_WINDOW.
VERIFY_SETTLE_SECONDS = _int("SELFHEAL_VERIFY_SETTLE", 300)
VERIFY_WINDOW_SECONDS = _int("SELFHEAL_VERIFY_WINDOW", 900)

AGENT_WORKERS = max(1, _int("SELFHEAL_AGENT_WORKERS", 2))
AUTO_DIAGNOSE = _bool("SELFHEAL_AUTO_DIAGNOSE", True)

# ---------------------------------------------------------------------
# Mock tenant
# ---------------------------------------------------------------------
#
# Simulated seconds per wall second. At 20, a 15-minute verify window
# passes in 45 seconds. The verify windows above are in simulated time.

MOCK_SPEED = max(0.1, _float("SELFHEAL_MOCK_SPEED", 20.0))

# ---------------------------------------------------------------------
# LLM (CAI Inference Service, OpenAI-compatible)
# ---------------------------------------------------------------------

LLM_PROVIDER = os.environ.get("SELFHEAL_LLM_PROVIDER", "cai").strip().lower()
LLM_MODEL_ID = os.environ.get("LLM_MODEL_ID", "")
LLM_ENDPOINT_BASE_URL = os.environ.get("LLM_ENDPOINT_BASE_URL", "")
LLM_CDP_TOKEN = os.environ.get("LLM_CDP_TOKEN", "")
LLM_TEMPERATURE = _float("SELFHEAL_LLM_TEMPERATURE", 0.0)

EMBEDDING_MODEL_ID = os.environ.get("EMBEDDING_MODEL_ID", "")
EMBEDDING_ENDPOINT_BASE_URL = os.environ.get("EMBEDDING_ENDPOINT_BASE_URL", "")
EMBEDDING_CDP_TOKEN = os.environ.get("EMBEDDING_CDP_TOKEN", "")

# "cai" embeds through the endpoint above; "hash" is a local bag-of-words
# embedder, good enough to rank a few dozen playbook sections offline.
EMBEDDING_PROVIDER = os.environ.get(
    "SELFHEAL_EMBEDDING_PROVIDER", "cai" if EMBEDDING_ENDPOINT_BASE_URL else "hash"
).strip().lower()

RAG_TOP_K = _int("SELFHEAL_RAG_TOP_K", 5)

# ---------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------

APP_HOST = os.environ.get("SELFHEAL_APP_HOST", "127.0.0.1")
APP_PORT = _int("CDSW_APP_PORT", _int("SELFHEAL_APP_PORT", 8100))


def ensure_dirs() -> None:
    for path in (DATA_DIR, CHROMA_DIR, EDITS_DIR):
        os.makedirs(path, exist_ok=True)
