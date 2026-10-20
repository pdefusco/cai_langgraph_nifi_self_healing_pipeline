# NiFi Self-Healing Pipeline (CAI + LangGraph + CDF DataFlow)

A LangGraph agent, running as a Cloudera AI (CAI) Workbench Application, that:

- watches the KPIs of NiFi 2.6.0 flows deployed in CDF DataFlow (Public Cloud, AWS);
- diagnoses breaches using internal playbooks retrieved from Chroma;
- proposes a remediation from a fixed action catalog;
- waits for a human to approve, edit or reject the proposal;
- redeploys the change; and
- measures whether the KPI actually improved.

Every read and write goes through the `cdp df` / `cdp dfworkload` CLI (cdpcli 0.9.164). The NiFi REST API cannot be
used on CDF Public Cloud, because it rejects CDP workload tokens.

```
poller (tier A health ~10s, tier B KPI charts ~30s)
  └─ detectors → incident ─┐
                           ▼
gather_context → retrieve_playbooks → diagnose_and_propose (LLM) → validate_proposal
                                          ▲  invalid, retries left  │
                                          └─────────────────────────┤
                                     NO_ACTION → record_no_action   │
                                                                    ▼
                                    human_review  [interrupt, survives restarts]
                         approve → apply_remediation → VERIFYING → outcome (IMPROVED / NO_CHANGE / WORSENED)
                         edit    → validate_proposal → human_review
                         reject  → record_feedback (reason required)
```

**The LLM never writes flow JSON.** It chooses actions from a catalog, such as `SET_CONCURRENT_TASKS` with
target `S01 Consumer` and value 4, and deterministic code turns that choice into:

- a flow diff, the edit, a validation result, and
- the exact `cdp` commands the reviewer sees before approving.

**Phases.** Phase 1 is this repo: the MVP plus outcome capture from day one. Phase 2 is a dashboard that reads the
same store. Phase 3 learns from the recorded outcomes. Both later phases need the ground-truth outcomes that Phase 1
already writes to the `outcomes` table.

---

## Layout

```
app.py                     CAI Application entry point → nifi_healer/ui/gradio_app.py
config.py                  every setting, from the environment (SELFHEAL_*, LLM_*, EMBEDDING_*)
nifi_healer/
  cdp/                     DF client (live = cdp subprocess, mock = simulated tenant), guardrails, cli_spec
  kpi/                     chart parsing, detectors, outcome evaluator
  flow/                    flow → compact inventory for the LLM; whitelisted edit ops + validation
  actions/                 action catalog (bounds, API surface) and the guarded executor
  agent/                   LangGraph state, prompts, nodes, graph (SqliteSaver checkpoints)
  rag/                     playbook ingest and retrieval (Chroma)
  runtime/poller.py        tiered polling thread, incidents, outcome evaluation
  sim/                     the mock tenant (world model, bootstrap)
  store/                   SQLite store (incidents, proposals, reviews, actions, outcomes)
  ui/                      views.py (plain data) + gradio_app.py (layout)
flows/                     flow builder, scenario modules (scenarios/README.md), packs; flows/out/<pack>.json
playbooks/                 markdown playbooks + cdf-platform-rules.md (ingested into Chroma)
provision/                 00–06 + teardown scripts, and commands.md (the raw cdp commands)
tests/
```

## Test scenarios

There are 45 scenarios in 20 packs. Each pack is one deployment, named `nfh-<pack>`, and each scenario is one process
group inside it. **[flows/scenarios/README.md](flows/scenarios/README.md)** walks through each one, with a diagram
of its flow, how it's graded and its full settings. The walkthroughs are written by hand in
`flows/scenario_walkthroughs.py`, and `python flows/scenario_docs.py` generates the rest. Every scenario declares:

- the fault it injects;
- its KPIs;
- its known-correct fix, used for evaluation; and
- the traps, meaning fixes that look right but are wrong.

| Pack | Size | Scenarios → expected fix |
|---|---|---|
| `p1-throughput` | XS ×1 | S01 Slow Consumer → concurrency · S02 Slow Run Schedule → run schedule · S03 Pipeline Bottleneck → concurrency on the middle stage · S04 Bursty Oscillator → concurrency |
| `p2-backpressure` | XS ×1 | S05 Object Threshold Too Low → object threshold · S06 Data Size Threshold Too Low → size threshold · S07 Fan-out Flood → sink schedule · S08 Runaway Producer → throttle the source |
| `p3-errors` | XS ×1 | S09 Failure Self-Loop → route failure to the DLQ · S10 Unreachable Endpoint → DLQ + timeout + concurrency · S11 Wrong JSON Path → fix the property · S12 Processor-wide Backoff → penalize instead of yield |
| `p4-dataloss-latency` | XS ×1 | S13 Silent Expiration → expiration 0 + concurrency · S14 Auto-terminated Failures → DLQ · S15 Bin Never Fills → Max Bin Age · S16 Disabled Processor → enable it |
| `p5-parameters` | XS ×1 | S17 Wrong Endpoint Parameter → parameter update · S18 Load-test Parameter Left On → parameter update · **S19 Healthy Control → no incident / NO_ACTION** |
| `p6-sizing-cpu` | XS ×1 | S20 CPU Saturated → scale up |
| `p7-sizing-overprov` | M ×2 | S21 Over-provisioned → scale down (cost) |

**The action catalog:**

- `SET_CONCURRENT_TASKS`, `SET_RUN_SCHEDULE`, `SET_BACKPRESSURE` and `SET_FLOWFILE_EXPIRATION`
- `SET_PENALTY_YIELD` and `SET_RETRY_POLICY`
- `SET_PROCESSOR_PROPERTY`, limited to whitelisted keys
- `ROUTE_RELATIONSHIP_TO_DLQ`, only to an existing `*DLQ*` component
- `SET_SCHEDULED_STATE`, `UPDATE_PARAMETER`, `SCALE_DEPLOYMENT` and `NO_ACTION`

Each action family uses one API path:

| Family | Commands |
|---|---|
| Flow edits | `df get-flow-version` → edit → `df import-flow-definition-version` → `df change-flow-version-in-deployment --strategy ONLY_RESTART_AFFECTED_COMPONENTS` |
| Parameters | `dfworkload update-flow-in-deployment --parameter-groups` |
| Sizing | `df update-deployment` |

`restart-deployment` is deliberately left out of the catalog: restarting does not drain a backlog.

## Safety

- **Writes are disarmed by default on a live tenant.** `SELFHEAL_ARMED` defaults to `false` for live and `true` for
  the mock. It can only be set as an environment variable; nothing in the UI changes it. While disarmed, an approval
  is recorded as a `DRY_RUN` along with the exact commands it would have run.
- **Prefix and denylist.** The app only touches deployments and flows whose name starts with `SELFHEAL_PREFIX`
  (`nfh-`) and that the fleet file recorded. It never touches anything in `SELFHEAL_DENYLIST`.
- **Checks before every write.** Before each write the executor checks again:
  - the cooldown (`SELFHEAL_COOLDOWN`) and the actions-per-window limit (`SELFHEAL_MAX_ACTIONS_PER_WINDOW` in
    `SELFHEAL_ACTION_WINDOW`);
  - drift of the base flow version;
  - a half-applied version change; and
  - that the workload endpoint is reachable.
- **Two fixes to one deployment.** S15 and S16 both live in `nfh-p4-dataloss-latency` and are both diagnosed on its
  first version. Once S15's fix is deployed, S16's edit is planned again on the version S15 deployed. It is applied
  only if every version since S16's diagnosis came from this app's own applied actions, and if the diff is still
  exactly the one the reviewer approved. Any other drift is refused: diagnose again.
- **Stale reviews.** An open review whose deployment has since moved to a version this app did not deploy (a
  rebuilt baseline, a hand change) is marked **Stale** in the Review Queue: its proposal reads a flow that is no
  longer running. **Diagnose** replaces it: the review is answered "rediagnose" (recorded, with the reviewer), and
  the agent runs again on the deployed version.
- **Cooldown.** An armed approval inside a deployment's cooldown (30 minutes after the last change, so the previous
  fix can be measured) is turned away before the review is answered. The review stays open; approve it again later.
- **Who decides.** On CAI the Reviewer field defaults to the viewer's login, from the `REMOTE-USER` header CAI adds
  to every Application request. A viewer whose `REMOTE-USER-PERM` is `RO` can read the queue but cannot approve,
  edit or reject.
- **Commands it never uses.** It never uses `STOP_AND_EMPTY_QUEUES`, never sends `--kpis` together with a version
  change, and never restarts a deployment.
- **Credentials.** They are passed only in the subprocess environment, never on a command line and never in a log.

---

## Runbook 1: offline (mock tenant)

No CDP access is needed. The mock tenant simulates all 7 deployments:

- it runs 20× faster than wall time;
- its KPI charts react to applied fixes; and
- it checks every `cdp` argv against cdpcli's bundled spec, so a command that would fail live also fails here.

```bash
python3.12 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python -m pytest -q tests          # all offline: scripted LLM, hash embedder, mock tenant
.venv/bin/python flows/build_all.py --check  # flow definitions build, validate, and are current

# The app. Mock is the default client. Without an LLM, incidents open and wait in the queue as NEW.
export LLM_MODEL_ID=nvidia/nemotron-3-super-120b-a12b
export LLM_ENDPOINT_BASE_URL=https://ml-64288d82-5dd.go01-dem.ylcu-atmi.cloudera.site/namespaces/serving-default/endpoints/goes---nemotron-3-super-120b/v1
export LLM_CDP_ACCESS_KEY_ID=...             # a CDP API key from that endpoint's tenant; never commit it
export LLM_CDP_PRIVATE_KEY=...
.venv/bin/python app.py                      # http://127.0.0.1:8100
```

The model and endpoint above are the CAI Inference Service deployment this repo was developed against. Any
OpenAI-compatible chat endpoint works; set the variables to your own.

**The endpoint's token.** A CDP workload token lasts an hour. With `LLM_CDP_ACCESS_KEY_ID` and
`LLM_CDP_PRIVATE_KEY` set, the app mints its own (`cdp iam generate-workload-auth-token --workload-name DE`) and
renews it five minutes before it expires. The key must come from the **endpoint's** tenant: a token minted in
another tenant gets 401. Without a key, `LLM_CDP_TOKEN` is used as pasted (`export LLM_CDP_TOKEN=...`), and then
the CAI session's `/tmp/jwt`, which only works for an endpoint in the workbench's own tenant. The System tab's LLM
row asks the endpoint whether it accepts the token, and an incident whose run stopped shows why in the Review Queue.

**What to try:**

- **Fleet:** about 20 KPIs breach within seconds, and an incident opens for every scenario except S19.
- **Review Queue:**
  - Each incident shows its evidence, the diagnosis with playbook citations, the diff and the exact commands.
  - **Approve** applies the change to the mock. With writes armed it runs in the background: a flow edit imports
    a version and waits minutes for the deployment, so the click returns at once and the panel follows the change
    until it lands (**Last action: … APPLIED**).
  - **Submit edit** re-validates JSON steps you have changed.
  - **Reject** requires a reason.
- **System → Fast-forward** moves the mock clock past the 5-minute settle and the 15-minute verify window. History
  then shows the outcome verdict. Each fix is judged again 1 h and 12 h after it (the **Windows** column): a clear
  first verdict closes the incident; an unclear one keeps it verifying until the hour; a later WORSENED on a
  resolved incident is flagged as a regression.
- **History → Correct the outcome** lets a reviewer label a fix (right fix, wrong fix, can't tell; a reason is
  required) without touching the measured verdicts, or re-check a window from a later start to skip a stretch the
  fix could not show in. A re-check's verdict replaces the old one, which is kept, and the incident's status
  follows it.
- **Rollbacks.** A fix measured WORSENED (its deciding window, or a later regression) gets a rollback proposal in
  the Review Queue, unless a reviewer labelled it the right fix; **History → Propose a rollback** asks for one by
  hand. A flow edit is undone by re-pointing the deployment at the version it replaced (nothing is imported), and
  only while it still runs the fix's version; parameters and sizing are set back only while they still hold the
  fix's values. The same guards and cooldown apply, and a human approves it. `SELFHEAL_PROPOSE_ROLLBACKS=false`
  turns off the automatic proposals.
- **Scorecard** shows how often the agent's first proposal is valid, picks the scenario's design fix, is approved as
  proposed, and works (a reviewer's label counts over the measured verdict), by agent version, scenario, fix or
  who chose it. Each fix also gets a **lift**, from −100% to +100%: how far the KPI that opened the incident moved
  its good way in the window that decided the verdict (`outcomes.outcome_lift`). A group shows the median and its
  middle half. An agent version is the fingerprint of the prompt text, plus the model and whether memory was on. It
  changes only when what the LLM is shown changes. Every tab opens with a **How to read this page** key
  (`nifi_healer/ui/legend.py`).
- **Offline replay.** `.venv/bin/python -m nifi_healer.eval.replay --trials 3` runs every scenario three times,
  each round on a fresh mock tenant of its own (`var/data/replay`), against the configured LLM: detect, diagnose,
  approve, judge. It is the only place a proposal is approved without a reviewer, and it refuses unless the client
  is the mock; it never touches a live tenant. `--pack`/`--scenario` narrow it; `--oracle` answers with the design
  fix, which tests the harness rather than the agent. On CAI, run it as a job (script `cai_job.py`, Arguments
  `-m nifi_healer.eval.replay --trials 3`). **Scorecard → Incidents: Replay** shows its runs, with how many faults
  were detected. `--fixes 3` keeps watching after each fix and diagnoses the next incident, up to three, so a
  compound fault (p12–p14) is scored as cleared after N fixes. It waits for each fault as long as its scenario's
  `detect_within`, so a slow burn (p16) or a nightly batch (p17) gets hours of simulated time, not minutes. The
  controls (p18–p20: bursts within capacity, rejects already in a DLQ, daily-cycle and idle load, a 30-processor
  pipeline, a busy CPU) pass only if they open no incident over theirs.
- **Past cases.** Each diagnosis is also shown the most similar past cases (`nifi_healer/rag/memory.py`): what the
  agent proposed, what the reviewer did with it and why, and what the verify windows measured. Only judged fixes and
  rejections count, never the replay's oracle runs. A proposal can cite a case id; the Review Queue lists the cases
  the agent saw. In the replay a round learns from the earlier ones; `--no-memory` is the baseline to compare with
  (Scorecard grouped by Replay run or Replay round).
- **Fix track records.** The Review Queue shows how often each fix worked on the same process group, and on other
  groups with the same symptom (`nifi_healer/rag/priors.py`), counted over the same cases. A proposal that already
  failed twice on its group gets a warning. The agent's prompt leaves them out: in the replay they did not change
  how often its fixes worked, with or without past cases.
- **Graduated autonomy.** A low-risk flow edit (concurrency, run schedule, back pressure, penalty/yield, retry) that
  has worked on a process group 3 times in a row with a person's approval, at values a person approved, approves
  itself after a 10-minute veto window in which anyone can approve, reject or **Hold** it
  (`nifi_healer/agent/autonomy.py`). Any miss takes the trust away; rollbacks, edits and warned reviews always wait
  for a person. On for the mock, off on live until `SELFHEAL_AUTONOMY=true`; **System → Pause autonomy** stops it.
- **Chaos and fault episodes.** With `SELFHEAL_CHAOS=true` the app plants a fixed scenario's fault again once its
  deployment is quiet and the scenario has rested 4 h (`nifi_healer/chaos.py`): only that process group swapped back
  to the baseline as a new version, or only its parameters or sizing. One plant per tick across the fleet, at most 3
  a day and 2 h apart per deployment, never on a control; disarmed it is a dry run, and **System → Pause chaos**
  stops it. Each try is in the `chaos_injections` ledger (no parameter values, no user names), which the agent never
  reads; the Monitor shows a plant as a chaos change. `nifi_healer/eval/soak.py` follows each plant to its detection,
  decision, fixes and clearing, and counts false alarms. The **Soak** tab shows them since chaos first ticked:
  detected, time to detect and to clear, fixes, review wait, the autonomy share, false alarms a day and regressions,
  overall, by fault kind and by day, with the latest episodes (an incident opens in History) and a week of App health.
- **Playbook edits.** A rejection, or a wrong-fix label in History, has the LLM draft an edit to the playbook section
  the agent relied on (`nifi_healer/rag/drafter.py`). It waits on the **Playbooks** tab, with its reason and diff,
  until a person approves (or changes, rejects, later reverts) it. Approved edits are kept in the app's database and
  laid over `playbooks/*.md` when they are indexed (`nifi_healer/rag/overlay.py`), so the clone stays clean; the
  agent version gains `/playbooks:<fingerprint>`, and the replay reads them too (`--no-playbook-edits` for the
  files alone). **Make the applied edits permanent** gives them as a patch to commit. The platform rules are never
  edited. `SELFHEAL_DRAFT_PLAYBOOK_EDITS=false` turns drafting off.

The mock tenant persists in `var/data/mock_world.json`. To start over, run
`.venv/bin/python -m nifi_healer.sim.bootstrap --reset` instead of deleting files.

## Runbook 2: live tenant

**Prerequisites:**

- a CDF DataFlow service on AWS, with NiFi `2.6.0.4.12.0.2-1` available;
- a CAI workbench **in the same CDP environment**, because `dfworkload` only answers from inside the environment's
  VPC; and
- a CDP API key with a DataFlow role on that environment.

Set the project environment variables in CAI under **Project Settings → Advanced**:

```
SELFHEAL_DF_CLIENT=live
SELFHEAL_SERVICE_CRN=crn:cdp:df:...:service:...
SELFHEAL_ENVIRONMENT_CRN=crn:cdp:environments:...
CDP_ACCESS_KEY_ID=...            # or ~/.cdp/credentials in the project
CDP_PRIVATE_KEY=...
LLM_MODEL_ID=nvidia/nemotron-3-super-120b-a12b
LLM_ENDPOINT_BASE_URL=https://<cai-inference>/namespaces/serving-default/endpoints/<endpoint>/v1
LLM_CDP_ACCESS_KEY_ID=...        # a CDP API key from the endpoint's tenant: the app mints and renews tokens.
LLM_CDP_PRIVATE_KEY=...          # Or LLM_CDP_TOKEN=... (expires in an hour), or neither for an endpoint in
                                 # the workbench's own tenant (/tmp/jwt).
SELFHEAL_ARMED=false             # leave false until step 6
```

Then, from a CAI session terminal (`provision/commands.md` lists the raw CLI equivalent of every step):

```bash
bash cdsw-build.sh                                    # install deps, check flows/out is current

python provision/00_selftest.py                       # Python/cdpcli, settings, both planes, workload TCP 443
python provision/01_import_flows.py --canary          # import one pack under a throwaway name, read it back
python provision/01_import_flows.py --pack p1-throughput
python provision/02_create_deployments.py --pack p1-throughput --dry-run
python provision/02_create_deployments.py --pack p1-throughput      # billing starts; 10-20 min
python provision/03_discover_kpi_metadata.py --pack p1-throughput   # check metric ids/components before writing
python provision/04_configure_kpis.py --pack p1-throughput          # whole-list replace, waits for kpisDirty=false
python provision/05_register_fleet.py --pack p1-throughput          # fleet file matches the tenant
```

1. Start the app (see the CAI Application settings below) with `SELFHEAL_ARMED=false`. An S01 incident appears with a
   concurrency proposal and the exact commands. Approving it records a dry run.
2. Set `SELFHEAL_ARMED=true` and restart the application. Then approve again.
3. Check that the change landed:
   - A new catalog version appears.
   - `cdp df describe-flow-in-deployment` shows the new `flowVersionCrn`.
   - The outcome becomes `IMPROVED` after the verify window.
4. `python provision/06_reset_pack.py --pack p1-throughput` puts the pack back on its faulty baseline, so you can run
   the experiment again.
5. Provision the remaining packs with the same steps, without `--pack`. S19 must produce no incident, or a
   `NO_ACTION`.
6. Tear down with `python provision/teardown.py --dry-run`, then run it without `--dry-run`. Catalog versions cannot be
   deleted one at a time; the flow is deleted as a whole.

If `03` reports a metric id the tenant does not offer, correct it in `provision/out/metric_overrides.json` and re-run
`03`, rather than editing 21 scenarios.

## CAI Application settings

| Setting | Value |
|---|---|
| Script | `app.py` |
| Runtime | PBJ Workbench · Python 3.12 · Standard |
| Resources | 2 vCPU / 8 GiB |
| Environment | as in Runbook 2 (`SELFHEAL_DF_CLIENT=mock` for a demo without a tenant) |

**Running scripts as CAI jobs.** PBJ Workbench runs a job script cell by cell in an IPython kernel, where
`__file__` and the script's own `__doc__` do not exist, and `sys.argv` belongs to the kernel; the job's Arguments
field arrives as `$JOB_ARGUMENTS`. So every job uses the launcher `cai_job.py` as its script, and the real script
plus its flags as the job arguments. The launcher runs them as an ordinary Python process:

| Job | Script | Arguments |
|---|---|---|
| install + tests | `cai_job.py` | `provision/cai_build.py` |
| selftest | `cai_job.py` | `provision/00_selftest.py` |
| any provision step | `cai_job.py` | `provision/02_create_deployments.py --pack p1-throughput` |

`app.py` is kernel-safe, so the Application runs it directly. The app binds `127.0.0.1:$CDSW_APP_PORT`. State lives on the project filesystem, so it survives restarts:

| Path | Contents |
|---|---|
| `/home/cdsw/data/healer.db` | the store |
| `/home/cdsw/data/checkpoints.sqlite` | LangGraph checkpoints, so pending reviews survive a restart |
| `/home/cdsw/chroma` | the playbooks |
| `/home/cdsw/data/app.owner` | the host and pid of the app process that last started |

Mock and live never share a store, a fleet file or checkpoints: mock files carry a `.mock` suffix.

**"database is locked".** Inside one app process every database has a single serialized connection, so this
error means another process holds the file. That could be a second copy of the app, a session or job running
`app.py`, or a lock the network filesystem has not released yet. The app logs `[app] <handler>: database is locked
(<which> database)`, and if a different process has written `app.owner` since this one started, the log names
it. A review whose save failed this way shows as "marked for review, but … no paused review". Diagnose picks it
up again, and so does an app restart.

## Configuration reference

| Variable | Default | Meaning |
|---|---|---|
| `SELFHEAL_DF_CLIENT` | `mock` | `live` runs the `cdp` CLI; anything else uses the simulated tenant |
| `SELFHEAL_ARMED` | `true` for mock, `false` for live | allow writes; otherwise approvals are dry runs |
| `SELFHEAL_SERVICE_CRN`, `SELFHEAL_ENVIRONMENT_CRN` | (none) | the live DataFlow service and CDP environment; mock mode ignores them and always uses its own CRNs |
| `SELFHEAL_PREFIX` | `nfh-` | names the app may create or touch |
| `SELFHEAL_DENYLIST` | (empty) | deployment CRNs never touched, comma-separated |
| `SELFHEAL_NIFI_VERSION` | `2.6.0.4.12.0.2-1` | NiFi build for deployments and processor bundles |
| `SELFHEAL_POLL_HEALTH_SECONDS` / `_KPI_SECONDS` / `_EVENTS_SECONDS` | 30 / 60 / 120 for live (2 / 3 / 5 for mock) | poll intervals, minimum 1 |
| `SELFHEAL_POLL_WORKERS` | 4, or half the deployments when more | packs polled concurrently |
| `SELFHEAL_CDP_WORKERS` | 6 | long-lived cdp CLI processes for reads (writes always run alone); 0 runs every call alone |
| `SELFHEAL_KEEP_DAYS` / `_HEALTH_KEEP_DAYS` / `_CHECKPOINT_KEEP_DAYS` | 14 / 30 / 7 | KPI samples, App health samples, and closed incidents' checkpoints are pruned after this many days |
| `SELFHEAL_BREACH_DEBOUNCE` | 2 | consecutive breaching buckets before a signal |
| `SELFHEAL_INCIDENT_COOLDOWN` | 900 | seconds before a process group can reopen an incident |
| `SELFHEAL_VERIFY_SETTLE` / `_WINDOW` | 300 / 900 | outcome measurement after a change |
| `SELFHEAL_VERIFY_LATER_WINDOWS` | 3600,43200 | later verdicts on each fix, in seconds |
| `SELFHEAL_COOLDOWN`, `SELFHEAL_MAX_ACTIONS_PER_WINDOW`, `SELFHEAL_ACTION_WINDOW` | 1800, 3, 86400 | write rate limits per deployment |
| `SELFHEAL_AGENT_WORKERS` | 2 | concurrent agent runs |
| `SELFHEAL_AUTO_DIAGNOSE` | `true` | diagnose new incidents automatically (else use the Diagnose button) |
| `SELFHEAL_MOCK_SPEED` | 20 | simulated seconds per wall second |
| `LLM_MODEL_ID`, `LLM_ENDPOINT_BASE_URL`, `LLM_CDP_TOKEN` | (none) | CAI Inference Service chat model (OpenAI-compatible) |
| `LLM_CDP_ACCESS_KEY_ID`, `LLM_CDP_PRIVATE_KEY`, `LLM_CDP_REGION` | (none) | a CDP API key from the endpoint's tenant; the app mints its tokens with it (wins over `LLM_CDP_TOKEN`) |
| `SELFHEAL_LLM_TOKEN_WORKLOAD`, `SELFHEAL_LLM_TOKEN_RENEW` | `DE`, 300 | workload name for minted tokens; seconds before expiry they are renewed |
| `EMBEDDING_MODEL_ID`, `EMBEDDING_ENDPOINT_BASE_URL`, `EMBEDDING_CDP_TOKEN` | (none) | CAI Inference embedder; without it a local hash embedder is used |
| `SELFHEAL_LLM_PROVIDER` / `SELFHEAL_EMBEDDING_PROVIDER` | `cai` / `cai` if an endpoint is set, else `hash` | swap providers in `nifi_healer/llm.py` |
| `SELFHEAL_RAG_TOP_K` | 5 | playbook sections per diagnosis |
| `SELFHEAL_MEMORY` / `SELFHEAL_MEMORY_TOP_K` | true / 3 | show each diagnosis this many similar past cases |
| `SELFHEAL_PRIORS` / `SELFHEAL_PRIORS_TOP_K` | true / 5 | show the reviewer each fix's track record (this many from other groups) |
| `SELFHEAL_AUTONOMY` / `SELFHEAL_AUTONOMY_AFTER` / `SELFHEAL_AUTONOMY_VETO` | mock only / 3 / 600 | trusted fixes approve themselves; wins a person approved to earn trust; veto window in seconds |
| `SELFHEAL_DATA_DIR`, `SELFHEAL_CHROMA_DIR` | `/home/cdsw/...` on CAI, `./var/...` elsewhere | where state lives |
| `SELFHEAL_APP_HOST`, `CDSW_APP_PORT` / `SELFHEAL_APP_PORT` | `127.0.0.1`, 8100 | UI bind address |

## Tests

```bash
.venv/bin/python -m pytest -q tests
```

The suite runs fully offline. It covers:

- the cdpcli spec checks;
- the detectors and the outcome evaluator;
- edits and the catalog bounds;
- the executor and its guardrail refusals;
- RAG ingest and retrieval;
- the whole LangGraph flow on the mock tenant with a scripted LLM: incident → interrupt → approve, edit or reject →
  applied → outcome IMPROVED, plus a pending review surviving a restart; and
- the UI views.
