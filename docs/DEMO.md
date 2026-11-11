# Demo cheat sheet

A walk through the app's tabs for a short demo: what each one shows and what to say about it. Every tab also opens
with its own **How to read this page** key (`nifi_healer/ui/legend.py`). The [README](../README.md) has the
reference architecture; [ARCHITECTURE.md](ARCHITECTURE.md) has the agent's graph.

Before the demo: **System → Selftest** (both DataFlow APIs and the workload endpoint reachable), and check the
**LLM** row says *ready*.

## The tabs

| Tab | The question it answers |
|---|---|
| **Fleet** | What is breaching right now? |
| **Review Queue** | What does the agent think is wrong, and what would it change? Approve, edit, reject or diagnose again |
| **History** | What was done, and what did each verify window (15 min, 1 h, 12 h) measure? |
| **Playbooks** | What has the agent learned from rejections, waiting for a person to approve? |
| **Monitor** | What did each change actually do to the KPIs? |
| **Scorecard** | How good is the agent? |
| **Soak** | Does it hold up over days of faults planted on purpose? |
| **System** | Is the app itself configured and working? |

## Monitor: what did each change do?

- **Fleet strip (top)**: one row per deployment, its band coloured fine, near (80% of the threshold or more), over,
  or no data.
  - Markers show changes: the agent's approved fixes, and changes made outside this app.
  - Incident marks show when incidents opened and how they ended.
  - Click a row to open that deployment below.
- **One deployment (below)**: each change (a new flow version, a parameter update, a resize) starts a new stretch,
  A, B, C… A KPI rule update is only a tick on the rail: it changes the alert, not the flow.
- **Change ledger**: one column per stretch. A cell is the KPI's level at the end of that stretch, green if it moved
  the good way since the previous one, red if the bad way.
- **Change** picker: that change's before and after.
- All times are UTC.

> "Here is the queue before the agent's fix, here is the fix landing, and here is stretch B draining."

## Scorecard: how good is the agent?

**Where the incidents come from**: **Live** (the tenant, reviewed by people) or **Replay** (an offline test on a
simulated tenant, every valid proposal approved automatically, the same faults under different agent versions).

**Agent version** = a fingerprint of the exact text the agent is given (*instructions*), plus the model, memory on or
off, and the set of approved playbook edits. It changes only when what the LLM is shown changes. The replay's
**oracle** has no LLM: it applies the design fix, so it shows the best the measurements allow.

### Reading a row

Read left to right: did it propose something valid, was it the right fix, did a person agree, did it work, and by how
much.

- **Valid first try**: the first proposal passed every deterministic check without a correction.
- **Picked the design fix**: the fix the scenario was built to need (or an acceptable alternative), at a value that
  can clear it. The agent never sees this answer. **Right lever** is the same action and target at any value; *right
  fix, value short* is the right lever with too little of it (2 tasks where 4 are needed).
- **Approved as proposed**: what reviewers did. A trusted fix that approved itself is *auto-approved*, left out.
- **Fix worked**: of the applied fixes with a clear result, the share that worked; a reviewer's label counts over the
  measured verdict.
- **Lift**, −100% to +100%: how far the KPI that opened the incident moved its good way, from when the fix went in to
  the end of the window that decided the verdict, divided by the larger of the two values. +100%: a backlog cleared;
  +50%: halved; 0%: no change; −50%: doubled. With several KPIs, the one that moved least counts. A group shows the
  median and its middle half.
- **Regressions**: fine at first, worse in a later window. **To resolve**: median time from opening to resolved.

### A worked example

The oracle replay on p3, p4, p5, p6, p8, p11, p13, p14 and p17 (19 faulty scenarios, 2026-11-11). Because it is the
oracle, it is the ceiling, not the agent:

> "19 incidents. **Valid first try 100%**: every first proposal passed the checks. **Picked the design fix 100%**.
> **Approved as proposed 100%**. **Fix worked 94.7%**: 18 of the 19 applied fixes measurably worked. **Median lift
> 68%**, middle half 21% to 99%: the KPI that opened each incident moved about two thirds of the way back. **0
> regressions**, **25 minutes** to resolve. In replay we also see **faults detected 19 of 19**, and the healthy
> control stayed quiet."

**Group by: Scenario**, three rows worth pointing at:

| Row | What it shows | What to say |
|---|---|---|
| **S20 CPU Saturated** | SCALE_DEPLOYMENT · expected · worked · lift 60%, measured on system CPU | A clean win: the right fix, and CPU came 60% of the way down |
| **S11 Wrong JSON Path** | SET_PROCESSOR_PROPERTY · expected · worked · lift about 0% | Worked, yet the lift is tiny: the fix stopped the queue growing (IMPROVED), but the backlog drains slowly. Verdict and lift answer different questions |
| **S32 Throttle Parameter And Slow Poster** | UPDATE_PARAMETER · expected · applied · didn't help · lift 0% | A compound fault, two causes: the right first fix alone cannot clear it. The one miss in the 94.7%, and why the replay has `--fixes N` |

**For the real agent**: **Incidents: Live · Since: All · Group by: Scenario**, or **Group by: Agent version** to show
how the instructions changing moved the numbers. Read it the same way: design fix, review, worked, lift.

## Soak: does it hold up over days?

Empty unless `SELFHEAL_CHAOS` is on; if so, present it as the long-running test mode.

- The chaos injector plants a scenario's fault again on a quiet deployment (no open incident, nothing waiting or
  being verified): one plant a tick across the fleet, at most 3 a day per deployment, 2 h apart, after the scenario
  has rested 4 h. Never on a healthy control.
- **The agent never sees what was planted**: its prompts, memory and playbooks don't read the injector's ledger.
- An **episode** follows one plant: incident, first proposal, review, every fix, until its KPIs stay healthy.
  *Cleared*, *missed* (no incident in time), *gave up* (not cleared within a day), else *open*.
- **Tiles**: Detected and Time to detect; Picked the design fix; Cleared, Time to clear and Fixes; Review wait;
  Approved by autonomy; False alarms a day; Regressions and Rollbacks.
- **By fault kind** (classic, cascade, compound, intermittent, slow burn, control) and **by day**: time to clear and
  the autonomy share should improve as the track record grows.
- **App health** over the last week, one point an hour.

## System: is the app itself working?

| Row | What it shows | What to say |
|---|---|---|
| **DataFlow client** | `live`, or `mock (simulated tenant)` | Which tenant the app talks to |
| **Writes** | **ARMED**, or disarmed (dry runs) | The main safety switch. Set only in the project's environment, never from the page; a disarmed approval records the exact `cdp` commands it would run |
| **Service CRN / Environment CRN** | The DataFlow service and CDP environment | Where it is pointed |
| **LLM** | Provider · model · **ready**, or **unavailable** and why · "tokens minted and renewed by the app" | It asks the endpoint whether it accepts the token; the app renews its own hourly workload token |
| **Embeddings** | The embedder | What indexes the playbooks and past cases for search |
| **Poller** | Running, loops; health, KPI and event intervals; poll workers; CLI workers for reads; a **falling behind** warning | The heartbeat. Reads go through long-lived `cdp` processes; each write runs alone |
| **Last poller error** | The last error and when, and how many in the last hour | Usually *none* |
| **Retention** | KPI samples 14 d, App health 30 d, closed incidents' agent runs 7 d | Pruned hourly |
| **Agent** | Workers; auto-diagnose on or off | Off: diagnose by hand with the Diagnose button |
| **Guardrails** | 30 min cooldown, at most 3 actions per 24 h per deployment, prefix `nfh-` | Only `nfh-` deployments it registered can be touched; it never empties queues or restarts a deployment, because both hide a backlog |
| **Verify** | Settle and first window, in seconds | 15 min first verdict, then 1 h and 12 h |
| **Autonomy** | Off, on, or paused by whom, and the rule | A low-risk flow edit that worked 3 times in a row on a group with a person's approval approves itself after a 10 min veto window; any miss takes the trust away |
| **Trusted fixes** | Group, fix, wins, how many a person approved | Listed even while autonomy is off: what it *would* be trusted with |
| **Answer-key reviewer** | Off, or **on** and its limits | For testing on the packs only: decides reviews as the Scorecard grades them, approves playbook drafts that pass (2 a section); approves armed writes only with `SELFHEAL_AUTO_REVIEW_ARMED` |
| **Chaos** | Off, on since…, or paused; the rule; **next pack › group**, or why not yet | The fault injector behind the Soak tab |
| **Data** | Store, checkpoints and Chroma paths | All on the project filesystem, so it survives restarts |
| **Mock clock** | Speed and tenant time | Mock only |

**Buttons and panels**

- **Refresh** re-reads the table; **Poll now** reads every deployment at once.
- **Selftest**: JSON showing whether both DataFlow APIs and the workload endpoint can be reached.
- **Pause / Resume autonomy** and **Pause / Resume chaos**: shown only when the feature is on; the pause records who
  made it. A read-only (`RO`) viewer cannot use them.
- **Fast-forward** (mock only): moves the mock clock 1–120 minutes, to show verify windows closing in seconds.
- **App health**: the app itself over the last day, one sample a minute: how late the reads run, CDP and LLM calls
  (count, failures, slowest 5%), the agent's backlog, CPU and memory, the store's size. Its header checks three
  limits: KPI reads under 60 s late (p95), under 1% of CDP calls failing, CPU under 60% (p95). It refreshes every
  60 s, only while the tab is open.
