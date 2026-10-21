# Work tracker

Every feature, fix, pack and piece of operational work has an ID, `NFH-<n>`, kept here like a ticket. The architecture
these change is in [ARCHITECTURE.md](ARCHITECTURE.md).

How it's kept:

- **IDs are never reused.** A new ticket takes the next number, whatever its type.
- **Append, don't rewrite.** A new ticket goes at the bottom of the index and gets a section under
  [Open tickets](#open-tickets). Progress is a new dated line in its log. Only the index's Status column is edited.
- **A ticket is done when its commit is on main.** Its last log line names the commit, and its section moves to
  [Closed tickets](#closed-tickets).
- Commits and pull requests name the ticket they're for, e.g. `NFH-54: show the agent every reading in its group`.

Types: **Feature**, **Fix**, **Pack** (a deployment of test scenarios), **Ops** (work on the live tenant), **Docs**.
Statuses: **Parked** (agreed, not yet scheduled), **Planned**, **In progress**, **Blocked**, **Done**, **Dropped**. A pack
is **Live** (provisioned) or **Built** (in the repo, not provisioned).

Tickets 1–49 were written up on 2026-10-21 from the git history and the scenario packs; `git show <commit>` dates
them.

## Index

| ID | Type | Title | Status | Commits |
|---|---|---|---|---|
| NFH-1 | Feature | The app: poller, detectors, LangGraph agent, action catalog, guarded executor, review UI, mock tenant | Done | d01bbcb |
| NFH-2 | Feature | Build, test and provisioning run as CAI jobs | Done | 8f98fe2, bd65b12, 3375470 |
| NFH-3 | Fix | Hermetic tests; the mock always uses its own CRNs | Done | 01eccf8 |
| NFH-4 | Fix | Match the live CLI: flow-version output, the tenant's KPI metric ids, SYSTEM KPIs, parameter groups on create | Done | 483b8f6, da95027, 44aaaf1 |
| NFH-5 | Feature | Mint and renew the LLM token; probe the endpoint; show why a run stopped | Done | abd4c6c |
| NFH-6 | Feature | Review Queue robustness: queue position, unusable replies shown, lost checkpoints picked up, no decision on another incident's panel | Done | 1292a1d, 15ddec2, bbb8c48, cd33446 |
| NFH-7 | Feature | Apply approvals in the background; keep a review open through the cooldown; rebase a flow edit onto the app's own later version | Done | 277e8b9, e176991 |
| NFH-8 | Fix | Skip the conservation check across merge/split processors; record the CAI login as reviewer; read-only viewers can't decide | Done | 1b5365c |
| NFH-9 | Feature | Outcome checks: judged by the KPIs that opened the incident, at 15 min, 1 h and 12 h; reviewer labels and re-checks | Done | 52f78ff, 7ce26cc, 2cac41f |
| NFH-10 | Fix | Read a reasoning model's proposal after its thinking | Done | 4caaef6 |
| NFH-11 | Fix | InvokeHTTP's connect timeout under its real key; a new-baseline import; a flow-only reset | Done | 6f76828 |
| NFH-12 | Feature | A review drafted on a flow version no longer deployed is marked stale, and Diagnose replaces it | Done | 3786117 |
| NFH-13 | Feature | Retrieve the playbook sections that name a parameter the group uses | Done | 2df6355 |
| NFH-14 | Feature | History shows each KPI before and after a fix, and each group's in vs out | Done | 68d568b, 927408c |
| NFH-15 | Feature | Monitor tab: a deployment's KPIs as a timeline cut by every change; fleet overview | Done | 53c19ed, 5e2f5ed, 02c5953 |
| NFH-16 | Feature | Scorecard: one case per incident, rates by agent version, scenario and fix; lift from -100% to +100% | Done | 3184f40, aff5a1c, 5e512cc, b69503e, e8033c0, d438b42 |
| NFH-17 | Feature | Propose a rollback, for review, of a fix measured worse | Done | accfbb8 |
| NFH-18 | Feature | Offline replay eval, and a Replay source on the Scorecard | Done | 08b58a2 |
| NFH-19 | Feature | Remediation memory: the agent sees the most similar past cases | Done | 23b7fb1 |
| NFH-20 | Feature | Fix track records, shown to the reviewer only | Done | 3a594b8, 6cec612, f56a3a5 |
| NFH-21 | Feature | Draft playbook edits from rejections, for a person to approve on the Playbooks tab | Done | 121e73a, f9ff130, b1d7bf1 |
| NFH-22 | Feature | Graduated autonomy: a proven low-risk flow edit approves itself after a veto window | Done | 4e290f7 |
| NFH-23 | Feature | Phase 4 M1: fit 20 deployments (CLI worker pool, slower live polls, App health, retention, filtered queue) | Done | 9fc8edd |
| NFH-24 | Feature | Phase 4 M2: CRON scheduling, four new processor kinds, scenario kinds | Done | 7a9943d |
| NFH-25 | Feature | Phase 4 M4: chaos injector and fault episodes | Done | 9fd6950 |
| NFH-26 | Feature | Phase 4 M5: Soak tab | Done | f193cae |
| NFH-27 | Fix | Build an older store without DROP COLUMN; the worker test finds the user site | Done | 0e94d9b |
| NFH-28 | Fix | Report an endpoint's HTML error page as one line of its text | Done | cf03919 |
| NFH-29 | Docs | Scenario guide: walkthroughs and a diagram of each flow (flows/scenarios/README.md) | Done | be5a17e, c3c2892 |
| NFH-30 | Pack | p1-throughput: S01–S04 | Live | d01bbcb |
| NFH-31 | Pack | p2-backpressure: S05–S08 | Live | d01bbcb |
| NFH-32 | Pack | p3-errors: S09–S12 | Live | d01bbcb |
| NFH-33 | Pack | p4-dataloss-latency: S13–S16 | Live | d01bbcb |
| NFH-34 | Pack | p5-parameters: S17–S19 | Live | d01bbcb |
| NFH-35 | Pack | p6-sizing-cpu: S20 | Live | d01bbcb |
| NFH-36 | Pack | p7-sizing-overprov: S21 | Live | d01bbcb |
| NFH-37 | Pack | p8-orders: S22–S23 (cascade) | Live | 8898c01 |
| NFH-38 | Pack | p9-clickstream: S24–S25 (cascade) | Live | 8898c01 |
| NFH-39 | Pack | p10-iot: S26–S27 (cascade) | Live | 8898c01 |
| NFH-40 | Pack | p11-payments: S28–S29 (cascade) | Live | 8898c01 |
| NFH-41 | Pack | p12-warehouse: S30–S31 (compound) | Live | 2fb8574 |
| NFH-42 | Pack | p13-billing: S32–S33 (compound) | Live | 2fb8574 |
| NFH-43 | Pack | p14-capacity: S34 (compound) | Built | 2fb8574 |
| NFH-44 | Pack | p15-bursts: S35–S36 (intermittent) | Built | 9e339df |
| NFH-45 | Pack | p16-slow-burn: S37–S38 (slow burn) | Built | 9e339df |
| NFH-46 | Pack | p17-nightly-batch: S39 (intermittent) | Built | 9e339df |
| NFH-47 | Pack | p18-noisy-bursts: S40–S41 (control) | Built | d8b6da4 |
| NFH-48 | Pack | p19-daily-cycle: S42–S43 (control) | Built | d8b6da4 |
| NFH-49 | Pack | p20-big-healthy: S44–S45 (control) | Built | d8b6da4 |
| NFH-50 | Ops | Phase 4 M6: live rollout and the review pass | In progress | |
| NFH-51 | Feature | Review Queue change table in plain sentences, canvas fields hidden | Parked | |
| NFH-52 | Feature | Scorecard refreshes when its tab opens | Parked | |
| NFH-53 | Feature | Scorecard grades a value "at least the acceptable one", and a resize by its size | Done | 93491e2 |
| NFH-54 | Feature | The agent sees every reading in its process group, not only the breached ones | Done | e93d361 |
| NFH-55 | Feature | Refresh waiting reviews automatically when something that affects them changes | Planned | |
| NFH-56 | Fix | Flow definitions over 64 KB can't be read (p11, p20) | Blocked | |
| NFH-57 | Feature | "Dismiss: platform problem" for a review that failed for reasons other than the diagnosis | Parked | |
| NFH-58 | Feature | A form instead of JSON for editing a proposal's steps | Parked | |
| NFH-59 | Fix | Credit the first fix of a two-fault scenario by the KPI it targeted | Parked | |
| NFH-60 | Feature | Capacity arithmetic in the agent's prompt | Done | 8e7325a |
| NFH-61 | Feature | Validation checks a tasks or run-schedule change against the intake, and sends a short one back | Done | |
| NFH-62 | Ops | Offline before-and-after measurement of NFH-53, 54, 60 and 61 | Planned | |
| NFH-63 | Fix | Import picks the oldest catalog version as baseline; p3 and p5 run the broken v1 | Parked | |
| NFH-64 | Fix | Scenario design: S06 alerts on the wrong measure, S21's threshold is unreachable, S09 can't drain, S20 stalls | Parked | |
| NFH-65 | Ops | Uniform watch-only KPIs on every connection and processor of a group | Planned | |
| NFH-66 | Feature | Environment promotion (Phase 5) | Parked | |
| NFH-67 | Docs | This tracker, and the architecture guide | Done | 5191d36 |

## Open tickets

### NFH-50 · Ops · Phase 4 M6: live rollout and the review pass
p1–p13 are deployed on pdf1019-cdp-env, each EXTRA_SMALL ×1. Every pack's first incidents are being reviewed against
the scenario guide, and what the review shows becomes tickets. p14–p20 wait for NFH-56 and NFH-65.
- 2026-10-20: p1–p13 provisioned. Review pass started.
- 2026-10-21: S01, S05, S08, S13 and S30 fixed live. S02 and S07 approved and draining. Review paused to build
  NFH-53, 54, 55, 60, 61 and 62, so the waiting reviews are re-drafted by the improved agent.

### NFH-51 · Feature · Review Queue change table in plain sentences
The change table shows raw diff paths, including canvas-only fields such as connection bends and positions. Show the
plain sentences the edit already writes (`s09-failure-loop: destination 'S09 Validator' -> 'S09 DLQ'`), with the
cosmetic and id fields behind "show all fields". Display only.
- 2026-10-20: found on S09. Parked.

### NFH-52 · Feature · Scorecard refreshes when its tab opens
The Scorecard renders only on page load and on its own Refresh button.
- 2026-10-20: offered. Parked.

### NFH-55 · Feature · Refresh waiting reviews automatically
Every waiting proposal was drafted before the review pass, so a rejection never reaches the reviews like it. Re-draft
a waiting review when something that affects it changes:
- a change lands on the same deployment;
- a playbook section is added or edited that the review's search now finds, or ranks differently;
- an approval or rejection adds a past case that the review's search now finds.

Only reviews whose evidence changed are re-drafted: the search is re-run (cheap) and compared with what the proposal
was drafted from. Events a minute apart are batched. A review whose problem has cleared is flagged or closed instead
of re-drafted. The old proposal shows as superseded in History.
- 2026-10-20: the user's idea, after S22 and S24. Parked.
- 2026-10-21: the user chose automatic, re-drafting only what an event affected, to keep tokens down. Planned, after
  NFH-54, 60 and 61.

### NFH-56 · Fix · Flow definitions over 64 KB can't be read
`get-flow-version` for nfh-p11-payments returns JSON cut at 65,535 characters, so every p11 review has no inventory
and no p11 fix can be validated. Almost certainly server-side: cdpcli only base64-decodes the API's answer.
p20-big-healthy will hit it too.
- 2026-10-20: found on S28. Asked the CDF product team. Blocks p14–p20.

### NFH-57 · Feature · Dismiss a review for a platform problem
A review that failed for a platform reason (NFH-56) can only be rejected, which counts against the fix, becomes a past
case and drafts a playbook edit. Add a separate outcome that closes the review without teaching or grading the agent.
- 2026-10-20: found on S28. Parked.

### NFH-58 · Feature · A form for editing steps
Editing a proposal means typing a JSON list of steps. Replace it with one row per step: an action dropdown limited to
the proposal's family, a target dropdown from the inventory, fields that fit the action, with JSON behind "edit as
JSON".
- 2026-10-21: asked for on S30. Parked.

### NFH-59 · Fix · Credit a two-fault first fix by its own KPI
S32 needs a parameter fix and then a flow edit, so two proposals. The outcome check judges the first by every KPI that
opened the incident, so a correct first fix can read as no change, or worse. Credit a fix for the KPI it targeted, and
report the KPIs still breached as "still open: likely another fault", with no rollback offered.
- 2026-10-21: found on S32. Parked.

### NFH-61 · Feature · Check capacity before review
For SET_CONCURRENT_TASKS and SET_RUN_SCHEDULE, validation estimates the processor's capacity after the change and
compares it with the measured intake. A change still short of the intake goes back to the agent as a validation
problem ("4 tasks ≈ 3/s, below the intake of 6/s") for its next attempt. Quiet when the intake isn't measured.
- 2026-10-21: planned. The user chose sending it back to the agent over a warning only.
- 2026-10-21: built. `actions/capacity.py` judges a tasks or run-schedule step from the stored samples: the
  ceiling after the change (tasks / run schedule, or the measured rate per task when the processor is busy below its
  ceiling with a backlog in front) against the intake (its rate in plus its queue's growth, or what the sources
  upstream send through a plain chain, a GenerateFlowFile by its settings). Short of the intake by the 10% headroom,
  or no changed processor with a backlog in front, goes back to the agent while it has attempts left; on the last
  attempt and on a reviewer's edit it is a warning, which also keeps autonomy off. Quiet when a measure is missing,
  through a split or branch (S24), on an expiring queue (S25), or for a step that lowers capacity. Every design and
  acceptable fix on p1, p2 and p8–p13 passes it; the oracle replay sends none back.

### NFH-62 · Ops · Measure offline before and after
Run the replay once per scenario on the current prompt for the backlog and pipeline packs (p1, p2, p8–p13), then again
after NFH-53, 54, 60 and 61. Compare the design-fix rate, right target, right value and outcome. No live changes. The
simulator models capacity the way NFH-60 reasons, so the live re-draft (NFH-55) confirms it.
- 2026-10-21: planned.

### NFH-63 · Fix · Import picks the oldest version as baseline
When a flow is in the catalog but not in the fleet record, `provisioning.import_flow` takes the oldest version as
baseline. On pdf1019-cdp-env that deployed p3 and p5 on v1, built before NFH-11, whose InvokeHTTP sends a header with
a space and fails every call. Pick the newest version whose comment starts with "baseline". Then re-baseline p5
(01 with `--new-baseline`, then 06 `--flow-only`), which is a tenant write the user runs.
- 2026-10-21: found on S10. Parked.

### NFH-64 · Fix · Scenario design problems found live
- S06 alerts on percent full, which counts objects only, so six large files read as 0.06% and it never opens an
  incident. Alert on bytes queued instead.
- S21's CPU threshold can't be reached on EXTRA_SMALL.
- S09's backlog can't drain after its fix.
- S20 is stalled live rather than CPU-bound.
- Thresholds on processor counts (S23 `below=20`, the rate KPIs) are written per second, but CDF charts a processor's
  counts as 5-minute totals, so on live they are 300 times off: a `below` alert never fires and an `above` one fires
  far too early. Fix with NFH-65.
- 2026-10-21: found in the review pass. Parked.
- 2026-10-21: the processor-count units found while building NFH-54.

### NFH-65 · Ops · Uniform watch-only KPIs
The KPIs a pack defines were placed by the scenario's designer, often right at the cause, which half-hands the agent
the answer once NFH-54 shows them. Give every connection and processor of a group a watch-only KPI, so the agent's
view is the same everywhere. A tenant write (re-running the 04 KPI step).
- 2026-10-21: planned for before p14–p20.

### NFH-66 · Feature · Environment promotion
How a fix gets from test to production: apply directly, stage then promote, or hand off; and how to test when the data
only exists in production.
- 2026-10-20: parked, to revisit as Phase 5.

## Closed tickets

Tickets 1–29 were done before this tracker began; see the index.

### NFH-60 · Feature · Capacity arithmetic in the prompt
The agent's only guidance on a backlog is "fix the consumer", which it reads as "add tasks", without saying how many.
Replace that rule with a method:
1. Find the first stage with a full input and a low output.
2. Compare its rate with its ceiling (tasks ÷ run schedule). At the ceiling, the schedule is the limit: shorten it.
   Below the ceiling with a backlog in front, processing time is the limit: size tasks by intake ÷ rate per task.
   Sending nothing at all is a stall, failure or misconfiguration, not capacity. High CPU is sizing.
3. Size the value so the new capacity exceeds the intake.

Also check the throughput playbook sections for the same lean towards tasks. New PROMPT_VERSION.
- 2026-10-21: the review pass showed tasks proposed for every backlog (S01, S10, S30) and values too small to clear the
  intake (S30). Planned.
- 2026-10-21: built. The prompt's rule is now the method: find the stage whose input queue is full and whose output
  queue is not; take the intake as its rate in plus its queue's growth, or the source's earlier peak when back
  pressure holds the queue; compare its rate with its ceiling (tasks / run schedule) to choose the schedule or tasks,
  and size the value above the intake. backlog-slow-consumer.md and back-pressure.md say the same with worked numbers
  (2 tasks every 125 ms is 16/s, short of 20/s). Section ids unchanged. New PROMPT_VERSION.
- 2026-10-21: done in 8e7325a.

### NFH-54 · Feature · The agent sees every reading in its process group
The prompt lists only the signals that breached. In a cascade (S22, S24, S26, S28) the agent sees the full ingest
queue and fixes the processor next to it, never seeing that the queues further down are full and the stuck stage sends
nothing. Add a "Current readings" block: every KPI on the group, each queue's depth and each processor's rate in and
out per second, with whether it is rising, draining or flat. Read from the samples the app already stores, so it adds
no CDF calls. Only the KPIs the pack defines; NFH-65 makes that coverage uniform. New PROMPT_VERSION.
- 2026-10-20: found on S20. Parked.
- 2026-10-21: the review pass showed it behind the cascade misses and S01's re-open while draining. Planned; the user
  chose the pack's existing KPIs for now.
- 2026-10-21: built. `kpi/readings.py` reads every KPI the pack defines on the group from the stored samples (last
  15 min): latest, its alert threshold, over/near/fine as the Monitor colours it, and rising, falling or flat. Rates are
  per second: live processor counts are 5-minute totals, so `SELFHEAL_COUNT_WINDOW_SECONDS` (300 live, 1 on the mock)
  divides them out. A source that sent more in the last 2 h shows that peak, since back pressure throttles it. The
  prompt has a "Current readings" section after the signals; new PROMPT_VERSION.
- 2026-10-21: done in e93d361.

### NFH-53 · Feature · Grade a value "at least the acceptable one"
The Scorecard treats a fix's value as irrelevant: any SCALE_DEPLOYMENT counts as S20's design fix, and S30's 2 tasks
counts the same as the 4 and 8 the intake needs. Grade tasks, run schedules and resizes by whether the value reaches
the acceptable one, and a resize by its size. Needed for NFH-62 to measure the value, not only the target.
- 2026-10-20: found on S20. Parked.
- 2026-10-21: S30 showed the same for tasks. Planned with NFH-54.
- 2026-10-21: built. Concurrent tasks, run schedules and resizes must go at least as far from what is deployed as
  the label's value, in the label's direction (S08 slows a source, S21 shrinks). The right lever at too little is a
  new grade, "value short". The Scorecard and the replay report show the right-value rate (Design fix) and the
  right-lever rate side by side; Soak still counts the design fix only. The scenario guide says "or more", "or
  shorter", "or larger" in each grading line.
- 2026-10-21: done in 93491e2.

### NFH-67 · Docs · Tracker and architecture guide
This file, and [ARCHITECTURE.md](ARCHITECTURE.md) with the agent's graph drawn from the code.
- 2026-10-21: started.
- 2026-10-21: both written; `nifi_healer/agent/diagram.py` draws the graph, and `tests/test_architecture_doc.py`
  fails when the drawing is stale or a node is missing from the node table.
- 2026-10-21: done in 5191d36.
