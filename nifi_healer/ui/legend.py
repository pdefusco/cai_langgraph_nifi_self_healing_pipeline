"""
"How to read this page": a plain-English key at the top of each tab,
for a reader who knows NiFi but not this app. Markdown, one per tab.
"""

import config
from nifi_healer.ui.views import window_label

_FIRST, _SECOND, *_LATER = config.VERIFY_WINDOWS
_WINDOWS = ", ".join(window_label(w) for w in config.VERIFY_WINDOWS)
_AUTONOMY = (f"""

The one exception, while autonomy is on: a **trusted fix** (a low-risk flow edit that has worked on this process
group {config.AUTONOMY_AFTER} times in a row with a person's approval) shows a countdown and approves itself after
{window_label(config.AUTONOMY_VETO_SECONDS)} unless someone approves, rejects or presses **Hold for review** first.
When the countdown ends, the guardrails are checked again; if one says no, the review is handed back to you with the
reason.""" if config.AUTONOMY else "")

FLEET = f"""
The fleet as the poller last read it from CDF DataFlow. It refreshes every second.

**The pieces**
- **Pack**: one DataFlow deployment (`nfh-...`). Each pack holds a few test scenarios.
- **Process group**: one scenario inside a pack, such as S01. Every fault is planted in exactly one group, so an
  incident is always opened for one process group.
- **KPI**: a chart that DataFlow keeps for one queue or processor (FlowFiles queued, bytes sent, ...) with an alert
  threshold. The agent sees only what these charts show.

**Deployments table**
- **State**: DataFlow's own health for the deployment. **Flow** is the flow version running now.
- **Active alerts**: alerts DataFlow raised. **Groups not ok**: process groups with a KPI that is not ok.
- **KPIs read**: how many of the pack's KPI charts came back on the last poll.

**Process groups table**
- **In / Out**: FlowFiles entering and leaving the group per chart bucket. **Out / in** near 1 means nothing is
  being lost or held up. Well under 1, with no queue growing, means data is being dropped.
- **Queued (watched)**: FlowFiles waiting in the group's watched queues.

**KPIs table: Status**
- **ok**: within its threshold.
- **BREACHING**: over its threshold for {config.BREACH_DEBOUNCE} buckets in a row. This opens an incident.
- **warning**: under its threshold but rising fast enough to cross it within
  {window_label(config.TREND_HORIZON_SECONDS)}. It is shown to the agent but never opens an incident by itself.
- **IMBALANCE**: FlowFiles leave slower than they arrive and no queue explains the gap (silent data loss).
- **ALERT**: DataFlow itself reports the KPI's alert as active. **no chart**: DataFlow returned no data for it.
- **Trend /min**: how fast the value is changing per minute; positive is rising.
"""

REVIEW = f"""
Every change the agent wants to make waits here for a person. Nothing is written to the tenant until someone
approves it{'' if config.ARMED else ', and while writes are disarmed an approval is only a dry run'}.{_AUTONOMY}

**Left: the evidence.** The signals that opened the incident (a KPI over its threshold, an alert, a data imbalance),
with the KPI values the agent was shown.

**Right: the proposal**
- **Diagnosis / Root cause**: what the agent thinks is wrong and which component causes it.
- **Rationale / Expected effect**: why this fix, and what should happen to the KPIs if it works.
- **Confidence**: the agent's own 0–1 estimate. It is not measured.
- **Playbooks cited**: the internal runbook sections (`playbooks/`) it relied on.
- **Past cases shown to the agent**: earlier incidents like this one and how their fixes turned out ("memory"). The
  agent is shown these and may learn from them.
- **Track record of these fixes**: how often this kind of fix worked on this process group before. It is shown to
  you only, not to the agent, to help you decide.
- **Action family / risk / writes**: which DataFlow API the fix uses (a new flow version, a parameter update, a
  resize), how risky it is, and what it changes.
- **Validation failed / Warnings**: the deterministic checks every proposal goes through (allowed actions, bounds,
  the target exists). An invalid proposal cannot be approved.

**Below**
- **Change**: each setting before and after. **Exact cdp commands**: what will run, word for word.
- **Steps**: the fix as JSON. Edit it and press **Submit edit** to re-validate your version before approving.
- **Approve** applies it. **Reject** closes the incident and needs a reason, which is kept. **Diagnose** asks the
  agent to look again.
"""

HISTORY = f"""
Every incident, newest first, with what was done and what it achieved.

- **Status**: VERIFYING (fix applied, being measured), RESOLVED (it worked), UNRESOLVED (it did not), REJECTED (a
  reviewer said no), NO_ACTION (the agent judged nothing needed fixing), DRY_RUN (approved while writes were off).
- **Verdict**: what the measurements say the fix did: IMPROVED, NO_CHANGE, WORSENED, or INCONCLUSIVE (too little
  data to tell).
- **Windows**: each fix is measured again over {_WINDOWS} after it went in, ignoring the first
  {window_label(config.VERIFY_SETTLE_SECONDS)} while the changed components restart. The {window_label(_FIRST)}
  window closes an incident when it is clear; the {window_label(_SECOND)} one decides it otherwise; later ones only
  catch a fix that turned bad.
- **Change**: the flow version or parameters the fix produced.

**Correct the outcome, or undo the fix**
- **Your call on the fix**: label it RIGHT_FIX, WRONG_FIX or UNCLEAR when you know better than the measurement (for
  example, the queue drained on its own). The Scorecard counts your label over the measured verdict; the measurement
  itself is kept.
- **Re-check**: measure a window again from another start time, such as after an unrelated fault cleared.
- **Propose a rollback**: opens a new incident that undoes the fix. It goes through the Review Queue like any other.
"""

PLAYBOOKS = """
The playbooks are the internal runbooks (`playbooks/*.md`) the agent searches before it proposes a fix. When a
reviewer rejects a proposal, or labels an applied fix a wrong fix in History, the LLM reads their reason and drafts an
edit to the playbook section the agent relied on. Nothing changes what the agent reads until a person approves it here.

**An edit**
- **Why it was drafted**: the incident, what the agent proposed and cited, and the reviewer's reason. A later draft of
  the same section takes over an open one and carries its reasons along, so a section has one open draft.
- **The change**: the section's text before and after. Struck lines go, marked lines are added.
- **Checks it fails**: what the agent must not read: the platform rules (`cdf-platform-rules.md`, never edited here),
  an action outside the catalog, newly advising to empty queues or restart a deployment, a CRN or an incident id.

**What you can do**
- **Approve** applies the text in the box (change it first if you like). The agent reads it from its next diagnosis.
- **Reject** drops the draft and needs a reason. **Revert** takes an applied edit back out. **Draft again** retries a
  draft that failed, or one whose section changed since.

**Status**
- **PROPOSED**: waiting for you. **SKIPPED**: the LLM judged the feedback to be about that incident only.
- **STALE**: the section changed after the draft (another edit, or a new version of the file in git).
- **MERGED**: the file in git already has the edit, so nothing is applied twice.

**Where approved edits live**: in the app's own database, laid over the files whenever the playbooks are indexed;
the files in git are not touched. **Make the applied edits permanent** gives them as a patch to commit. The Scorecard
names the agent with *playbooks edited* and a fingerprint of the approved edits, so before and after can be compared.
"""

MONITOR = """
The KPIs over time, with every change marked, so you can see what each change did.

**Fleet strip (top)**: one row per deployment.
- Its band is coloured by how close the KPIs were to their thresholds: fine, near (80% of the threshold or more),
  over, or no data.
- Markers show changes: the agent's (approved fixes) and an operator's (made outside this app).
- Incident marks show when incidents opened and how they ended.
- **KPIs now / Open / Last change / Verdict**: the current state, open incidents, the newest change and its
  measured result. Click a row to open that deployment below.

**One deployment (below)**
- The timeline is cut into **stretches** A, B, C... Each change starts a new one: a new flow version, a parameter
  update, a resize. A KPI rule update is only a tick on the rail; it changes the alert, not the flow.
- The **change ledger** has one column per stretch. A cell is the KPI's level at the end of that stretch (the mean of
  its last three buckets) and how it moved from the previous stretch. Green moved the good way, red the bad way.
- Pick a **Change** to see that change's before and after.

All times are UTC.
"""

SCORECARD = f"""
How good the agent is at fixing things, counted from the incidents it handled.

**Where the incidents come from**
- **Live**: the real incidents on the tenant, reviewed by people.
- **Replay**: an offline test. Every scenario is planted on a simulated tenant, the agent proposes a fix, and every
  valid proposal is approved automatically. The same faults can be replayed under different agent versions and
  compared fairly. It never touches the live tenant.

**Agent version: which agent handled it**
- It has three parts, shown as *instructions fa8c5675be* with *model · memory on* beneath it.
- **Instructions** (`prompt:` in the raw name): a fingerprint of the exact text the agent is given: its standing
  instructions, the question template, the repair and feedback messages, and the list of fixes it may choose from.
  The fingerprint changes only when that text changes. Most code commits leave it alone, so it is not a commit or
  release number. When it changes, the agent may behave differently, and this grouping shows whether it got better.
- **Model**: the LLM. It is part of the version because it is configurable (`LLM_MODEL_ID`), so the same instructions
  can be compared on two models. The replay's **oracle** has no LLM: it always applies the design fix, which shows
  the best result the measurements allow.
- **Memory on/off**: whether the agent was shown past cases. The replay turns it off to measure what memory adds.
  *Track records off* appears only on older runs, from when the agent was also shown fix success rates.
- **Playbooks edited**: the agent read playbook edits approved on the Playbooks tab; the fingerprint names the set
  of edits, so each approval or revert starts a new version.

**Tiles and columns**
- **Incidents**: how many, including those on the **healthy control** (S19, a group with nothing wrong, where the
  right answer is to do nothing).
- **Valid first try**: the agent's first proposal passed every check without needing a correction.
- **Picked the design fix** (*Design fix*): it chose the fix the scenario was built to need, or an acceptable
  alternative. The agent never sees this answer. The setting counts, the value does not: 4 or 6 concurrent tasks are
  the same pick.
- **Cited a case**: its proposal pointed to a past case from memory.
- **Approved as proposed** (*Review*): what reviewers did: approved as is, approved after editing, or rejected. A
  trusted fix that approved itself is shown as *auto-approved* and left out of these rates.
- **Faults detected** (Replay only): planted faults that opened an incident within 30 simulated minutes, and how
  often the healthy control stayed quiet.
- **Fix worked** (*Worked*): of the applied fixes with a clear result, the share that worked. A reviewer's label
  counts over the measured verdict.
- **Lift** (*Median lift*): one number for how much a fix helped, from **−100%** to **+100%**.
  - It reads the KPI that opened the incident, and compares where it stood when the fix went in with where it stood
    at the end of the window that decided the verdict. For a rate (good when high), it compares the averages on
    each side.
  - The change is divided by the larger of the two values, so the scale is the same for every KPI. **+100%**: a
    backlog cleared, or a stopped rate is flowing again. **+50%**: the backlog halved. **0%**: no change.
    **−50%**: it doubled.
  - When several KPIs decide an incident, the one that moved least counts, so one cleared queue cannot hide another
    still full.
  - **Measured on**, beside each incident's lift: the KPI it was read on (the component, such as *S01 Queue*, and
    the metric, such as *Flow Files Queued*), its two values, and the window.
  - A group shows the median and, beneath it, the **middle half** (where the middle 50% of its fixes fell) and which
    KPIs those lifts were measured on.
- **Regressions**: fixes that looked fine at first but were measured worse in a later window.
- **To resolve**: the median time from an incident opening to it being resolved.
"""

SYSTEM = f"""
How the app itself is configured and whether its parts are working.

- **DataFlow client**: mock (a simulated tenant) or live. **Writes**: ARMED means approvals change the tenant;
  disarmed means approvals only record the commands they would run. This is set in the project's environment
  variables, never from the page.
- **LLM / Embeddings**: the model endpoints and whether they answered. Embeddings index the playbooks and past cases
  so the agent can search them.
- **Poller**: how often each tier of KPIs is read, and when it last ran. **Poll now** reads everything at once.
- **Guardrails**: a deployment waits {window_label(config.COOLDOWN_SECONDS)} after a change before the next one, and
  takes at most {config.MAX_ACTIONS_PER_WINDOW} changes in {window_label(config.ACTION_WINDOW_SECONDS)}. Only `nfh-`
  deployments can ever be touched. Some actions are never
  allowed, such as emptying queues or restarting a deployment, because both hide a backlog instead of fixing it.
- **Verify**: the measuring windows each fix is judged over ({_WINDOWS}).
- **Autonomy**: whether trusted fixes approve themselves (`SELFHEAL_AUTONOMY`), and the rule. A fix is trusted on a
  process group once it has worked there {config.AUTONOMY_AFTER} times in a row with a person's approval; any miss (a
  rejection, a fix that didn't help or made things worse, a wrong-fix label) takes the trust away. Only low-risk flow
  edits qualify, at values a person approved. **Trusted fixes** lists them, even while autonomy is off. **Pause
  autonomy** sends every review back to a person until **Resume autonomy**.
- **Selftest** checks that both DataFlow APIs and the workload endpoint can be reached.
"""
