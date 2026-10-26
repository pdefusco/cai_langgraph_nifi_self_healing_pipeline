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

# The offline replay (nifi_healer/eval/replay.py) keeps its own mock
# tenant, store and checkpoints here; the Scorecard reads its record.
REPLAY_DIR = os.environ.get("SELFHEAL_REPLAY_DIR", os.path.join(DATA_DIR, "replay"))

FLOWS_OUT_DIR = os.path.join(REPO_ROOT, "flows", "out")
PROVISION_OUT_DIR = os.path.join(REPO_ROOT, "provision", "out")
PLAYBOOKS_DIR = os.path.join(REPO_ROOT, "playbooks")
METRIC_OVERRIDES_FILE = os.path.join(PROVISION_OUT_DIR, "metric_overrides.json")

# ---------------------------------------------------------------------
# CDP / DataFlow
# ---------------------------------------------------------------------

# The mock tenant's own CRNs. In mock mode they are always used, so the
# guardrails check the mock exactly as they check a live service, and a
# project whose environment carries the live CRNs (for its provisioning
# jobs) can still run the app on the mock.
MOCK_SERVICE_CRN = "crn:cdp:df:us-west-1:mock:service:nfh-mock"
MOCK_ENVIRONMENT_CRN = "crn:cdp:environments:us-west-1:mock:environment:nfh-mock"

SERVICE_CRN = MOCK_SERVICE_CRN if MOCK else os.environ.get("SELFHEAL_SERVICE_CRN", "")
ENVIRONMENT_CRN = MOCK_ENVIRONMENT_CRN if MOCK else os.environ.get("SELFHEAL_ENVIRONMENT_CRN", "")

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

# Long-lived CLI processes for reads (cdp/worker.py), so a read stops
# paying cdpcli's ~1 s of imports each time; started as needed. 0 runs
# every call in its own process, as before.
CDP_WORKERS = max(0, _int("SELFHEAL_CDP_WORKERS", 6))

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

# Graduated autonomy (agent/autonomy.py): a low-risk flow edit whose last
# tries on its process group all worked, AUTONOMY_AFTER of them approved
# by a person, approves itself once its review has waited
# AUTONOMY_VETO_SECONDS (incident clock) with nobody deciding. Off on a
# live tenant unless set; the System tab can pause it.
AUTONOMY = _bool("SELFHEAL_AUTONOMY", MOCK)
AUTONOMY_AFTER = max(1, _int("SELFHEAL_AUTONOMY_AFTER", 3))
AUTONOMY_VETO_SECONDS = max(0, _int("SELFHEAL_AUTONOMY_VETO", 600))

# The chaos injector (chaos.py): once a scenario's fault has cleared and
# rested CHAOS_REST, it is planted again, one plant per tick across the
# fleet, at most CHAOS_PER_DAY a day and CHAOS_SPACING apart on any one
# deployment. Off unless set; the System tab can pause it. The tick is
# wall seconds; rest, spacing and the day are the tenant's clock.
CHAOS = _bool("SELFHEAL_CHAOS", False)
CHAOS_TICK = max(1, _int("SELFHEAL_CHAOS_TICK", 30 if MOCK else 600))
CHAOS_REST = max(0, _int("SELFHEAL_CHAOS_REST", 14400))
CHAOS_PER_DAY = max(1, _int("SELFHEAL_CHAOS_PER_DAY", 3))
CHAOS_SPACING = max(0, _int("SELFHEAL_CHAOS_SPACING", 7200))

# ---------------------------------------------------------------------
# Polling and verification
# ---------------------------------------------------------------------
#
# Tier A is cheap state (health, alert counts); tier B is the KPI charts.
# A chart has ~25 buckets per window, so reading it every second mostly
# re-reads the same numbers. Both can be set as low as 1. Intervals are
# wall seconds (they pace CLI calls); the mock's calls cost nothing and
# its clock runs MOCK_SPEED times faster, so it polls more often. A live
# chart bucket is ~72 s, so reading KPIs more than once a minute mostly
# re-reads the newest bucket.

POLL_HEALTH_SECONDS = max(1, _int("SELFHEAL_POLL_HEALTH_SECONDS", 2 if MOCK else 30))
POLL_KPI_SECONDS = max(1, _int("SELFHEAL_POLL_KPI_SECONDS", 3 if MOCK else 60))

# The deployment event log (what changed, for the Monitor tab). Changes
# are rare; the log is read whole (the newest 200 events) each time.
POLL_EVENTS_SECONDS = max(1, _int("SELFHEAL_POLL_EVENTS_SECONDS", 5 if MOCK else 120))

# Packs polled at once; 0 (the default) means one per two deployments,
# at least 4, growing with the fleet. Tier B is three reads per pack.
POLL_WORKERS = max(0, _int("SELFHEAL_POLL_WORKERS", 0))


def poll_workers(deployments: int) -> int:
    return POLL_WORKERS or max(4, -(-deployments // 2))


# Consecutive breaching buckets, inside one response, before a signal.
BREACH_DEBOUNCE = max(1, _int("SELFHEAL_BREACH_DEBOUNCE", 2))

# A rising queue projected to cross its threshold within this long is
# reported as a warning (context for an incident, never one by itself).
TREND_HORIZON_SECONDS = _int("SELFHEAL_TREND_HORIZON", 900)

# A processor's FlowFile and byte counts are totals over this many
# seconds: CDF charts them per 5 minutes (S02 read 120 for 0.4/s), the
# simulator per second. Divided out wherever a rate per second is shown.
COUNT_WINDOW_SECONDS = max(1, _int("SELFHEAL_COUNT_WINDOW_SECONDS", 1 if MOCK else 300))

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

# Each fix is judged again over these longer windows, each from a chart
# coarse enough to hold it (kpi/outcomes.py). The first window closes an
# incident it is clear about; the next closes it either way; later ones
# only flag a regression.
VERIFY_WINDOWS = (VERIFY_WINDOW_SECONDS,) + tuple(sorted({
    int(w) for w in _list("SELFHEAL_VERIFY_LATER_WINDOWS") or ("3600", "43200")
    if w.isdigit() and int(w) > VERIFY_WINDOW_SECONDS}))

# A fix measured WORSENED (its deciding window, or a later regression)
# gets a rollback proposal, for review like any fix; the poller keeps
# trying this long after the verdict while its process group is busy.
PROPOSE_ROLLBACKS = _bool("SELFHEAL_PROPOSE_ROLLBACKS", True)
ROLLBACK_WITHIN_SECONDS = _int("SELFHEAL_ROLLBACK_WITHIN", 3600)

# Retention (the poller's hourly pass): KPI samples, the app's own health
# samples, and the agent checkpoints of incidents closed this long ago.
KEEP_DAYS = max(1, _int("SELFHEAL_KEEP_DAYS", 14))
HEALTH_KEEP_DAYS = max(1, _int("SELFHEAL_HEALTH_KEEP_DAYS", 30))
CHECKPOINT_KEEP_DAYS = max(1, _int("SELFHEAL_CHECKPOINT_KEEP_DAYS", 7))

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

# How long one chat request may take: a reasoning model thinks for minutes
# before it answers. A diagnosis the LLM didn't answer leaves its incident
# NEW; with AUTO_DIAGNOSE it is tried again after each of these waits in
# minutes, then every last one.
LLM_TIMEOUT_SECONDS = max(10, _int("SELFHEAL_LLM_TIMEOUT_SECONDS", 300))
LLM_RETRY_MINUTES = (5, 15, 30)

# A workload token lasts an hour, so a pasted LLM_CDP_TOKEN goes stale.
# With a CDP API key from the endpoint's tenant the app mints its own
# (`cdp iam generate-workload-auth-token`) and renews it this many seconds
# before it expires; LLM_CDP_TOKEN is then ignored. The region is the
# tenant's control plane region, when it is not the CLI default.
LLM_CDP_ACCESS_KEY_ID = os.environ.get("LLM_CDP_ACCESS_KEY_ID", "")
LLM_CDP_PRIVATE_KEY = os.environ.get("LLM_CDP_PRIVATE_KEY", "")
LLM_CDP_REGION = os.environ.get("LLM_CDP_REGION", "")
LLM_TOKEN_WORKLOAD = os.environ.get("SELFHEAL_LLM_TOKEN_WORKLOAD", "DE")
LLM_TOKEN_RENEW_SECONDS = _int("SELFHEAL_LLM_TOKEN_RENEW", 300)

EMBEDDING_MODEL_ID = os.environ.get("EMBEDDING_MODEL_ID", "")
EMBEDDING_ENDPOINT_BASE_URL = os.environ.get("EMBEDDING_ENDPOINT_BASE_URL", "")
EMBEDDING_CDP_TOKEN = os.environ.get("EMBEDDING_CDP_TOKEN", "")

# "cai" embeds through the endpoint above; "hash" is a local bag-of-words
# embedder, good enough to rank a few dozen playbook sections offline.
EMBEDDING_PROVIDER = os.environ.get(
    "SELFHEAL_EMBEDDING_PROVIDER", "cai" if EMBEDDING_ENDPOINT_BASE_URL else "hash"
).strip().lower()

RAG_TOP_K = _int("SELFHEAL_RAG_TOP_K", 5)

# Past cases (rag/memory.py) shown with the playbooks: what was tried on
# similar incidents, what the reviewer did and what it measured. Off for
# a baseline replay (eval/replay.py --no-memory).
MEMORY = _bool("SELFHEAL_MEMORY", True)
MEMORY_TOP_K = _int("SELFHEAL_MEMORY_TOP_K", 3)

# Fix track records (rag/priors.py), on the review: how often each fix
# worked on the incident's group, and on other groups with the same
# symptom (the TOP_K tried most). The agent does not see them.
PRIORS = _bool("SELFHEAL_PRIORS", True)
PRIORS_TOP_K = _int("SELFHEAL_PRIORS_TOP_K", 5)

# A second LLM reading of every valid fix the agent proposes, against the
# same readings and inventory (agent/nodes.critique_proposal): an
# objection it backs with a quoted line sends the fix back for another
# attempt, and is a warning on the last. One more LLM call per attempt.
# Off until the replay shows it helps (eval/replay.py --critic).
CRITIC = _bool("SELFHEAL_CRITIC", False)

# Proposals drawn at once on the first attempt (agent/nodes.diagnose_and_propose):
# of those the validators and the capacity check pass, the fix most of
# them agree on goes on, and with none valid the first is sent back as
# today. The endpoint answers one prompt differently from call to call
# even at temperature 0, so one draw is a sample of one. SAMPLES times
# the LLM calls on the first attempt. 1 (off) until the replay shows it
# helps (eval/replay.py --samples).
SAMPLES = max(1, _int("SELFHEAL_SAMPLES", 1))

# A rejection, or a WRONG_FIX label, has the LLM draft an edit of the
# playbook section the agent relied on (rag/drafter.py), for a person to
# approve on the Playbooks tab. Off: nothing is drafted.
DRAFT_PLAYBOOK_EDITS = _bool("SELFHEAL_DRAFT_PLAYBOOK_EDITS", True)

# ---------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------

APP_HOST = os.environ.get("SELFHEAL_APP_HOST", "127.0.0.1")
APP_PORT = _int("CDSW_APP_PORT", _int("SELFHEAL_APP_PORT", 8100))


def ensure_dirs() -> None:
    for path in (DATA_DIR, CHROMA_DIR, EDITS_DIR):
        os.makedirs(path, exist_ok=True)
