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
| NFH-61 | Feature | Validation checks a tasks or run-schedule change against the intake, and sends a short one back | Done | 2ee434c |
| NFH-62 | Ops | Offline before-and-after measurement of NFH-53, 54, 60 and 61 | Done | f4a3eb3 |
| NFH-63 | Fix | Import picks the oldest catalog version as baseline; p3 and p5 run the broken v1 | Done | 1db88f2 |
| NFH-64 | Fix | Scenario design: S06 alerts on the wrong measure, S21's threshold is unreachable, S09 can't drain, S20 stalls | Parked | |
| NFH-65 | Ops | Uniform watch-only KPIs on every connection and processor of a group | Planned | |
| NFH-66 | Feature | Environment promotion (Phase 5) | Parked | |
| NFH-67 | Docs | This tracker, and the architecture guide | Done | 5191d36 |
| NFH-68 | Fix | A diagnosis the LLM didn't answer is tried again by itself; the LLM timeout is a setting | Done | a4b5b5c |
| NFH-69 | Ops | Why the agent still fixes the wrong stage in a cascade (S22–S27) | In progress | |
| NFH-70 | Fix | The replay backfills KPI history as the app does, so the agent's readings have trends and peaks | Done | dc8c3f2 |
| NFH-71 | Fix | Routing several relationships of one processor to one DLQ is one step, not "appears twice" | Done | e2e5ce3 |
| NFH-72 | Feature | Readings list every connection in flow order, with its limit, how full it is and its expiration | Done | 1848a64 |
| NFH-73 | Feature | A retry keeps its diagnosis; the prompt names auto-terminated and expiration data loss | Done | 6ec47ff |
| NFH-74 | Fix | The replay reads the mock's processor counts per second, not per 5 minutes as on live | Done | 32e15aa |
| NFH-75 | Fix | The Scorecard grades a route to another stage's DLQ short, and accepts 3 tasks on S25 | Done | 37f3236 |
| NFH-76 | Feature | A critic checks each valid proposal against the readings before review | Dropped | bca2d04 |
| NFH-77 | Fix | The replay's clock runs on while the LLM thinks, so the agent reads empty KPIs on CAI | Done | 02a1431 |
| NFH-78 | Fix | The capacity check reads the intake past a measured split, and doesn't call a fix that matches the intake short of it | Done | 375a12d |
| NFH-79 | Fix | S03 and S24 are labelled with the smallest fix that clears them, 3 tasks, not 4 | Done | 373a739 |
| NFH-80 | Fix | The capacity check reads a burst landing in a queue as intake (S04) | Done | 6032349 |
| NFH-81 | Feature | `replay --compare` counts shared scenarios, wins and losses, and the change luck alone makes | Done | d2c69fe |
| NFH-82 | Feature | The prompt gets the capacity sums for each stage with a backlog, before the agent proposes | In progress | |

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

### NFH-69 · Ops · Why the cascades still miss
After NFH-54 and NFH-60 the agent has the right lever in 0 of 12 cascade cases (S22–S27) in NFH-62's after run. Read
what it proposed and why, attempt by attempt, against the design fix, before changing the prompt or the readings.
- 2026-10-22: `replay --show DIR` built: per case, the design fix, then each attempt's steps, diagnosis, rationale and
  the validation problems that sent it back, filtered by `--scenario` or `--pack`. Read-only. The user runs it on the
  after run in the CAI session.
- 2026-10-22: read the user's `--show` of the after run, and the prompts sent locally. Four causes:
  1. The replay shows the agent one sample per KPI: it never backfills history as the app does, so every reading is
     "flat" and a throttled source reads 0/s with no earlier peak (S22, S26). The capacity check was starved the
     same way, so NFH-62 measured NFH-54 and NFH-61 almost blind. Live is not affected. NFH-70.
  2. A queue with no KPI reads as empty. In S22 the agent called s22-extracted "empty" while s22-to-batch read
     10001 queued and Batch Orders 0/s out; S24 the same with s24-parsed. The readings give no back-pressure limit
     or fill either (s23-to-throttle: 1851 queued against 2000). NFH-72, app-side; NFH-65 is the tenant-side fix.
  3. Validation sent a right answer back. S26 round 1 routed three relationships to the DLQ as three steps, refused
     as "appears twice"; the retry dropped the diagnosis and invented a JSON problem. NFH-71 and NFH-73. In S27 the
     agent found the auto-terminated failure on Schema Check, but tried DebugFlow properties it can't edit, and never
     tied auto-termination to ROUTE_RELATIONSHIP_TO_DLQ. NFH-73.
  4. Expiration loss isn't recognised (S25): the agent blamed Parse View, though Parse DLQ reads 0/s in; the
     1-minute expiration on s25-session-buffer is only in the inventory. NFH-72 and NFH-73.
  S22's one "worked" is most likely noise: the merge's 30-minute bin age flushed inside the verify window.
- 2026-10-22: after NFH-70–73, the after run on p8–p10 (`replay-after-3`, 2 trials) against `replay-after-2`:
  right lever 0 → 6 of 12, right value 0 → 5, design fix 0 → 3, worked 2 → 7. S27 found the DLQ route in both trials,
  S25 the right lever in both; S22 and S24 one of two. S23 (more tasks on Extract Carrier: "the throttle has ample
  capacity") and S26 (a yield of 0, then a higher threshold on s26-merged) still miss. But the readings showed every
  processor rate 300x low (S23 Ship Confirmations 0.0333/s against the 10/s detection saw): NFH-74. So after-1 to
  after-3 measured the readings and the capacity check with wrong rates; re-run before judging S23 and S26.
- 2026-10-22: after NFH-74, `replay-after-4` (p8–p10, 2 trials): design fix 3 → 7 of 12, right lever 6 → 9,
  worked 7 → 8, valid on the first try 83%. S22 and S26 2 of 2. Read with `--show`: S27 round 2 routed to the wrong
  DLQ and was graded expected, and S25 round 1's 3 tasks worked but graded short: grader faults, NFH-75. Two real
  misses: S24 round 2 fixed Parse Click next to the alert and read the throttled source as "no data" (round 1 was
  right), and S25 round 2 claimed FlowFiles went to Parse DLQ, which read 0/s in, and changed a JSON path. Both are
  claims the readings contradict: the case for a critic (NFH-76).
- 2026-10-22: re-graded with NFH-75, after-3 → after-4: design fix 25% → 50%, right value 50% → 67%, right lever
  50% → 75%, worked 58% → 67%. after-3 had both S25 rounds at a working value and both S27 routes at Reject DLQ, so
  S24, S25 and S27 have each been right in one run and wrong in another: what is left is mostly variance.

### NFH-82 · Feature · The capacity sums in the prompt
The agent sizes a throughput fix by doing the prompt's sums itself, from the readings and the inventory, and the
capacity check (NFH-61) then sends a short one back, costing an attempt. Do the sums first, with the check's own code,
and give them to the agent as a "Capacity analysis" section: for each stage with a backlog in front, in flow order,
its ceiling, rate and intake, which limit it is at, and the least change that keeps up. No LLM call.
- 2026-10-22: built. A stage whose own way on is at back pressure reads "held back; look at the stage after it". Any
  other gets its ceiling, rate and intake, then the first of: disabled or stopped; sends nothing (a stall, not
  capacity); fed by a GenerateFlowFile at 0 sec (slow the source; no number of tasks keeps up); a ControlRate; already
  has the capacity (and an input queue that back-pressures at under a minute of the intake, S05); at its ceiling or
  not measured (tasks or a run schedule); below a ceiling that clears the intake (waiting, or slow per FlowFile);
  below its ceiling (tasks only). Each size is the least that keeps up with 10% headroom and, when FlowFiles are
  queued, what also drains them by the end of the first verify window (20 min), which is what the outcome is judged
  on. Then the deployment's CPU when it is high. The oracle replay on all 20 scenarios is unchanged (S30 and S32
  NO_CHANGE as before). To keep or drop on a CAI before/after replay.
- 2026-10-22: the analysis disagrees with four labels, left as they are for the user: S02's 1 sec where about 3.6 s
  keeps up (1.2 s drains in 20 min); S04's 4 tasks where 2 keep up and drain; S07's acceptable 100 ms (10/s) and 4
  tasks (4/s), both short of 10.1/s in, where 86 ms keeps up (the label, 0 sec, clears it); and S30's Label Parcel
  needing 5 tasks.

## Closed tickets

Tickets 1–29 were done before this tracker began; see the index.

### NFH-81 · Feature · `--compare` says what luck alone could explain
A before/after replay has one or two cases a scenario, so a few lucky rounds move the totals; and the "all" row
counted scenarios only one side had. To tell a change that helps from noise, the comparison needs the scenarios that
did better and worse, and how big a change luck alone makes.
- 2026-10-22: built. "all" counts the scenarios both records have, and a line names the ones only one has. For the
  right value, valid on the first try and worked, a line gives the scenarios that did better and worse, the change in
  the pooled rate, and the band 90% of 2000 shuffles fall in, each dealing a scenario's cases to before and after at
  random, as if one agent ran both times; a change past it reads "beyond luck". Ten scenarios each going from wrong
  to right is beyond luck (±60 pts); two are not. The rule to keep or drop a change is agreed before each run.
- 2026-10-22: done in d2c69fe.

### NFH-80 · Fix · A burst landing in a queue reads as intake
S04's source sends 600 FlowFiles every 10 minutes, 1/s on average. In the oracle replay for NFH-79 the incident opened
on the KPI detector while a burst was still landing, and the capacity check took the queue's growth then as intake
("about 8/s"), sending the design fix (4 tasks) back twice before it passed as a warning. An earlier oracle run opened
on the DataFlow alert, after the burst, and passed. The queue's growth should count only when it is sustained, or not
for a source that sends in bursts.
- 2026-10-22: found. Planned.
- 2026-10-22: built. The cause was not the queue's growth but the source's 2 h peak, 8–10/s at a burst, taken as
  the intake where the average is 1/s; and between bursts the queue dips under its alert, so the check also said
  nothing changed had a backlog. A source's or split's peak now stands for the intake only when back pressure may be
  holding it (a queue on its way on at or near its limit, or not measured); otherwise its time-weighted average does,
  and the readings show it beside the peak ("…, 1/s on average", where the peak is far above it). A queue whose KPI opened the incident counts as a backlog. In the
  oracle replay on all 20 scenarios S04's 4 tasks are valid on the first try; S30 and S32 read NO_CHANGE as before
  (two faults, one fix applied). Left as is: the check only asks for keeping pace on average, so 2 tasks on S04 pass.
- 2026-10-22: done in 6032349.

### NFH-79 · Fix · S03 and S24 labelled at the smallest fix that clears them
S03 and S24 were labelled 4 tasks, but 3 clears both with room to spare (S03: 3/s against about 2/s; S24: 150/s
against 100/s), so an agent's 3 worked yet graded short. The user's rule: a label is the smallest value that clears
the fault with the prompt's 10% headroom.
- 2026-10-22: both labels lowered to 3 and the scenario guide regenerated. The oracle replay on p1 and p9 clears both
  at 3 tasks (IMPROVED at 15 and 60 min). The Scorecard re-grades stored steps, so earlier runs read on the new rule.
- 2026-10-22: done in 373a739.

### NFH-78 · Fix · The capacity check reads the intake past a split
In `replay-ab2-on` S24's 2 tasks on Sessionize passed the capacity check (100/s against an intake of 50/s) and changed
nothing in three rounds. The clicks come out of a SplitText, so the check couldn't trace the source's rate (2 batches/s)
to the sessionizer, and the intake it had left was Sessionize's own 50/s, held there by back pressure, with its full
queue not growing. The real intake is 100/s: 2 tasks keep pace and never drain the backlog. The prompt's readings had
the same blind spot. And a fix within 10% of the intake was sent back as "below" it (S03's 2/s against 2/s).
- 2026-10-22: built. Every split or fan-out (S07, S18, S24, S33, S44) gets a chart-only KPI on its FlowFiles Out, which
  the readings show with its 2 h peak as for a source ("S24 Split Clicks: 50/s out, up to 100/s in the last 2 h"),
  and the capacity check starts the chain at a split or merge whose output is measured. S24's 2 tasks now go back,
  3 and 4 pass, as the sim agrees (2 tasks leave s24-splits above 8000). A fix that covers the intake but without the
  headroom reads "within 10% of the intake". Live, the new KPIs exist once the packs' KPIs are written again.
- 2026-10-22: done in 375a12d.

### NFH-77 · Fix · The replay's clock runs on while the LLM thinks
The mock's clock runs at `SELFHEAL_MOCK_SPEED` (20x) against wall time. The replay polls only while it detects, so
while the agent pool waits on the LLM (tens of seconds a call, AGENT_WORKERS at a time) the flows run on for many sim
minutes with nothing sampling them, and an incident queued behind others reads "no data in the last 15 min" for most
KPIs. Locally the oracle answers at once, so the tests and local runs never saw it. The live app polls all the time
at 1x and is not affected.
- 2026-10-22: found reading the critic A/B (NFH-76). The CAI replay runs after-3 and after-4 (NFH-69) most likely read
  mostly empty KPIs too, which fits after-4's S24 round 2 reading the throttled source as "no data"; their numbers
  measured the agent partly blind. Built: `World.held()` stops this process's clock, and `replay.diagnose` runs the
  agent inside it, so every incident is read as it was when detection opened it. Approval and judging run with the
  clock going, as they wait for the mock to apply the change. A test with an LLM that takes wall time checks the
  clock stands still while it answers.
- 2026-10-22: done in 02a1431.

### NFH-76 · Feature · A critic before review
Two of after-4's misses rest on claims the readings contradict: S24 round 2 fixed the stage next to the alert while
the readings showed Sessionize capped below its intake, and S25 round 2 blamed parse failures while Parse DLQ read
0/s in. A second LLM call reads the proposal against the same evidence and objects where it can show it wrong.
- A node after `validate_proposal`, for a valid agent proposal only (not a rollback or a reviewer's edit). It sees the
  signals, the Current readings, the inventory and the proposal, but not the playbooks or past cases. It may object,
  never propose: a claim a reading contradicts, a change to a stage that is not the first with a full input and a low
  output, a property change that alters what the flow produces rather than how fast. Each objection must quote the
  reading or inventory line; one that quotes nothing is dropped.
- Objections go back like the capacity check's: a retry while attempts are left, warnings on the last one, which block
  autonomy. A critic that fails or times out lets the proposal through with a warning.
- Off by default (`SELFHEAL_CRITIC`); the agent version gets `+critic`, so the Scorecard and `--compare` tell the
  runs apart. ARCHITECTURE change-log row and graph drawing.
- Measured by replay, with and without, 3 trials each: p8–p10, p1 and p2 for regressions, S19 as the control. Kept if
  the right value rises with no loss on p1/p2 and at most one objection to a design fix. Self-consistency (several
  drafts, then a vote) only if S24's round-to-round variance remains.
- 2026-10-22: planned after after-4 (NFH-69). Waiting for the user's go-ahead.
- 2026-10-22: built as its own node, `critique_proposal`, after `validate_proposal` (`SELFHEAL_CRITIC`, off by
  default). It reads the signals, readings, inventory and proposal, and returns objections, each with the line it
  quotes; an objection whose quote is not in that evidence (12 characters at least) is kept as dropped, not acted on.
  The prompt is versioned on its own (CRITIC_VERSION); PROMPT_VERSION is unchanged, so a run without the critic is
  the after-4 agent, and with it the agent version ends in `/critic:<hash>`. Replay `--critic` for the A/B; the run
  shows the second reading under each round. Also: the CDP worker waited on its pipe with `select()`, which fails on a
  descriptor above 1024, as the full test suite now reaches; it uses `poll()`.
- 2026-10-22: first A/B on CAI (p8–p10, p1, p2, S19; 3 trials each), without → with the critic: design fix 56% →
  28%, right value 69% → 38%, right lever 82% → 50%, valid on the first try 79% → 20%, attempts 1.2 → 2.4, worked
  79% → 50%. Read with `--show`: most readings in both arms were "no data in the last 15 min" (NFH-77), and the critic
  took those as empty queues and zero rates. Its bottleneck rule also fired on fixes that add no capacity: S26's DLQ
  route ("Store Readings is the bottleneck") and S08's throttled source. Changed: a quote from a line with no data
  is dropped, the prompt says such a reading was not measured, and the bottleneck rule covers only more tasks, a
  shorter schedule or a larger deployment; a DLQ route is not a change to what the flow produces. New CRITIC_VERSION.
  Not judged until both arms are re-run with NFH-77.
- 2026-10-22: re-run on CAI with NFH-77 and the new CRITIC_VERSION (`replay-ab2-off`, `replay-ab2-on`), without →
  with: valid on the first try 77% → 39%, design fix 74% → 59%, right lever 85% → 90%, worked 87% → 80%. Read with
  `--show`, 3 of its 13 objections were right (S08 round 1, S24 round 1 twice). Of the 10 wrong, 5 took a
  back-pressured rate for the intake (S01 three times, S03 and S08 round 2), 3 called s23's queue at 93% "not full",
  and 1 read the merge's low output on S24 as FlowFiles not getting through. The critic and the capacity check also
  disagree on the intake (the throttled 1/s against the source's 3/s peak), so the agent is sent back by one for what
  the other asked (S01, S03 and S24 round 2). Not kept: the right value falls and most objections are to fixes that
  work. The code stays, off by default; `replay-ab2-off` is the baseline from here.
- 2026-10-22: dropped.

### NFH-75 · Fix · Grade the DLQ a route reaches; accept 3 tasks on S25
In after-4, S27 round 2 routed Schema Check's failure to S27 Parse DLQ rather than S27 Reject DLQ, and was graded
expected: the grader compared a route's processor and relationships but not its DLQ. The data is saved, but where
nobody watches for it, and the judge saw no change. S25 round 1's 3 tasks on Sessionize (30/s against 20/s) worked
but was graded short against the label's 4.
- 2026-10-22: found in NFH-69 (after-4). Built: a route reaches the label only at the label's DLQ, else it is the
  right lever with the value short; S25 accepts 3 tasks. Scenario guide regenerated.
- 2026-10-22: done in 37f3236.

### NFH-74 · Fix · The replay's processor rates are 300x low on CAI
`config.COUNT_WINDOW_SECONDS` is 300 when the app starts live (CDF charts processor counts per 5 minutes) and 1 on
the mock, which records per second. `replay.isolate` switched a CAI project to the mock but kept 300, so on CAI the
replay's readings and capacity check divided every processor rate by 300. Local runs start on the mock and were
right, so the tests never saw it.
- 2026-10-22: found in NFH-69 (S23: 0.0333/s in the readings for 10/s). Built: `isolate` sets the window to 1, and
  the tests' config restore covers it.
- 2026-10-22: done in 32e15aa.

### NFH-73 · Feature · Keep the diagnosis on retry; name the data-loss levers
A retry after a refused step should keep the diagnosis and change only what was refused. The prompt should say that
data lost at an auto-terminated relationship is fixed by ROUTE_RELATIONSHIP_TO_DLQ (which enables a disabled DLQ), and
data expiring on a queue by speeding up its consumer.
- 2026-10-22: found in NFH-69 (S25, S26, S27). Planned.
- 2026-10-22: built. A retry now shows the agent the diagnosis and steps it proposed, with the problems, and says to
  keep the diagnosis unless a problem shows it wrong and change only what the problems name (a reply that was not a
  proposal is not shown back). The rules say data lost without an error is lost where the inventory drops it: an
  auto-terminated relationship is routed with ROUTE_RELATIONSHIP_TO_DLQ, which enables a disabled DLQ; a queue whose
  FlowFiles expire is fixed by speeding up the processor it feeds, not by lengthening the expiration.
- 2026-10-22: done in 6ec47ff.

### NFH-72 · Feature · Readings in flow order, with limits and expiration
List every connection of the group in flow order: queued against its back-pressure limit, how full, its expiration,
and "not measured" where it has no KPI, so a queue nobody watches no longer reads as empty. App-side; NFH-65 adds the
KPIs on the tenant.
- 2026-10-22: found in NFH-69 (S22, S23, S24, S25). Planned.
- 2026-10-22: built. Given the group's inventory, `readings.render` lists every connection first, in flow order
  (`flow_order`: from the sources along the connections, a loop moving nothing), as "source -> destination": a
  measured queue against its back-pressure limit and how full ("10001 queued, back pressure at 10000 (100% full)"),
  one with no KPI as "not measured" with its limits, and any expiration ("FlowFiles expire after 1 min"). Then the
  rates in flow order. The prompt says a queue that is not measured may still be full. S22's and S24's cascades now
  read as a row of full queues with unmeasured ones between; S25's buffer shows its 1-minute expiration.
- 2026-10-22: done in 1848a64.

### NFH-71 · Fix · One DLQ step for several relationships
The duplicate check keys a step by action, target, property and parameter, so three ROUTE_RELATIONSHIP_TO_DLQ steps
on one processor, one per relationship, are refused as "appears twice", though the action takes a list. Merge such
steps into one before validating.
- 2026-10-22: found in NFH-69 (S26). Planned.
- 2026-10-22: built. `catalog.merge_routes` folds routes of one processor to one DLQ into one step with all their
  relationships, where the first stood; `executor.plan` runs it after normalizing, so the stored proposal holds the
  merged step, and the Scorecard's `match` runs it too, so S26's one step per relationship grades as the design fix.
  Routes to another DLQ stay separate.
- 2026-10-22: done in e2e5ce3.

### NFH-70 · Fix · The replay backfills KPI history
The app fills its store from the 1 h, 12 h and 1 day charts of each pack when it starts (`Poller.backfill`); the
replay only polled, so the agent and the capacity check read one sample per KPI.
- 2026-10-22: found in NFH-69. Built: `detect()` backfills every deployed pack before polling. S01's and S22's
  sources now read "1/s out, up to 2.28/s in the last 2 h" and "0/s out, up to 56.3/s". The oracle replay on p8–p10
  is unchanged (6 of 6 design fixes, all worked).
- 2026-10-22: done in dc8c3f2.

### NFH-62 · Ops · Measure offline before and after
Run the replay once per scenario on the current prompt for the backlog and pipeline packs (p1, p2, p8–p13), then again
after NFH-53, 54, 60 and 61. Compare the design-fix rate, right target, right value and outcome. No live changes. The
simulator models capacity the way NFH-60 reasons, so the live re-draft (NFH-55) confirms it.
- 2026-10-21: planned.
- 2026-10-21: `replay --compare BEFORE AFTER` built: per scenario and in all, before→after, both graded by the
  Scorecard as it is now: design fix, right value, right lever, valid first time, proposals per case, worked and
  median lift. The oracle replay on p1, p2 and p8–p13 is unchanged by NFH-61. The before run (a checkout before
  e93d361) and the after run (main) are CAI jobs for the user, as they need the LLM endpoint.
- 2026-10-21: both runs done (before: a CAI session on 5191d36; after: the replay job on f4a3eb3), one trial each,
  run at the same time. Not yet a fair comparison: the LLM timed out on 5 diagnoses before and 8 after ("Request
  timed out." after 120 s, two retries), leaving those incidents NEW and ungraded. S01, S04, S06 and S23 timed out in
  both. Of the cases graded in both: S30 went from 2 Pick + 2 Label tasks (failed) to 3 + 5 (worked, +47% lift, still
  SHORT of the 4 + 8 design); S02 from 1 sec to 2 sec on Drain (SHORT, lift +36% → +12%); S25 from Sessionize 4 tasks
  (worked) to a wrong property; S24, S26 and S32 unchanged. The capacity check never sent a proposal back. The
  prompt is about 1.25 times longer after (system text 4,647 → 5,915 characters), but the timeouts don't follow
  prompt size. Open question: the check's HEADROOM is 1.1, and S30's 3 + 5 passed it at about 1.3 of the intake.
  Re-run both, one after the other, once NFH-68 retries timeouts.
- 2026-10-22: re-run, two trials each, one after the other, every diagnosis answered (before: 5191d36 with NFH-68's
  retry patched in; after: main at 4e7d41e). All 40 cases, before → after: design fix 50% → 42% (20 → 17 cases),
  right value 52% → 52%, right lever 70% → 62%, valid first time 95% → 85%, proposals per case 1.1 → 1.2, worked
  57% → 57%, median lift +8% → +13%. Overall no clear change: with two trials a scenario moves 50 points on one case.
  Better: S30 (worked 0% → 100%, lift +4% → +67%), S01 (right value 50% → 100%, worked 50% → 100%), S04 (right value
  0% → 50%), S22 (worked 50% → 100%). Worse: S02 and S07 (design fix in 2 → 1 of 2 trials), S25 and S29 (1 → 0),
  S03 (lever 100% → 50%). The cascades NFH-54 and NFH-60 aimed at are unchanged: S22–S27 have the right lever in
  0 of 12 cases after, 1 of 12 before (S25). More proposals were sent back for another attempt (valid first time
  down in S01, S02, S03, S26 and S30), most likely by the capacity check, and S30's ended worked in both trials. S32 has the design fix in both and never
  works in the simulator (see NFH-59). Done; why the cascades still miss is the next question.

### NFH-63 · Fix · Import picks the oldest version as baseline
When a flow is in the catalog but not in the fleet record, `provisioning.import_flow` takes the oldest version as
baseline. On pdf1019-cdp-env that deployed p3 and p5 on v1, built before NFH-11, whose InvokeHTTP sends a header with
a space and fails every call. Pick the newest version whose comment starts with "baseline". Then re-baseline p5
(01 with `--new-baseline`, then 06 `--flow-only`), which is a tenant write the user runs.
- 2026-10-21: found on S10. Parked.
- 2026-10-21: built. With no baseline in the fleet record, `import_flow` takes the newest version whose comment
  starts with "baseline" (the first import, or a rebuild by `--new-baseline`), else the oldest. The app's own
  versions ("nifi-healer …") and a canary never qualify. p5's record already names v1, so it still needs the
  re-baseline above.
- 2026-10-21: p5 re-baselined by the user: 01 `--new-baseline` imported v3 and recorded it, and 06 `--flow-only`
  moved the deployment from v1 to v3 (one step, the parameters left as they were). p3's record named v1 and it ran
  v3, the agent's S09 fix built on v1, so its S10 call still sent the bad header; 01 `--new-baseline` imported v4
  (the same definition as v2, never recorded) and 06 `--flow-only` moved it from v3 to v4, undoing the S09 fix as a
  reset does. Closed.

### NFH-68 · Fix · Retry a diagnosis the LLM didn't answer
The before and after replays (NFH-62) lost 5 and 8 diagnoses to "Request timed out.": the chat client gave the
reasoning model 120 s and two retries, and the incident was left NEW until an app restart or a Diagnose click. Make
the timeout a setting, have the app ask again on a backoff, and have the replay ask again within its round.
- 2026-10-21: found in NFH-62's runs. Planned.
- 2026-10-21: built. `SELFHEAL_LLM_TIMEOUT_SECONDS` (default 300) sets the chat client's timeout, with one client
  retry instead of two. The graph counts runs in a row that ended without an LLM answer; on every KPI poll, with
  `SELFHEAL_AUTO_DIAGNOSE` on, `graph.retry_failed()` queues such an incident again 5, 15, then every 30 minutes
  after its last failure (in memory; a restart re-queues every NEW incident anyway). The Review Queue says when the
  next try is. The replay asks again up to 3 times in a round, 30 s apart, with the clock stopped.

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
- 2026-10-21: done in 2ee434c.

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
