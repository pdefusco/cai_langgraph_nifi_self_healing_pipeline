---
title: CDF DataFlow platform rules
collection: nifi_best_practices
symptoms: [backlog, backpressure, failure_loop, data_loss, latency, stuck, misconfiguration, sizing_cpu, sizing_cost, healthy]
actions: []
---

# CDF DataFlow platform rules

Rules that apply to every remediation on Cloudera DataFlow, whatever the
symptom. Several of them were learned by measuring what went wrong on a
live tenant.

## Do not restart to clear a backlog
<!-- actions: NO_ACTION -->

Restarting a deployment or a process group does not make a slow consumer
faster. While the consumer is stopped, the source keeps producing, so the
backlog grows: one measured restart took a queue from 428 to 883 FlowFiles.
Restart is not a remediation and is not in the action catalog. Fix the
setting that limits throughput instead.

## Never empty queues to make a KPI look fixed
<!-- actions: NO_ACTION -->

Changing the flow version with the STOP_AND_EMPTY_QUEUES strategy, or
emptying a queue by hand, discards the queued data and resets Queued Count
to zero. The alert clears, but the cause is untouched, data is lost, and the
backlog comes back. Version changes always use
ONLY_RESTART_AFFECTED_COMPONENTS, which keeps queued data and restarts only
the components the change touches.

## Verify by the slope, not by the alert clearing
<!-- actions: NO_ACTION -->

An alert can clear for the wrong reason: a queue was emptied, a source
stalled, or the threshold changed. A fix is confirmed only by the KPI's
behaviour over a window after the change has settled. A queue must drain
or hold flat under its threshold, and a rate must rise or recover above its
floor. Compare a window before the change with a window after it, both
taken from the same chart response. Buckets from separate responses are
anchored differently and do not line up.

## Prefer parameter updates for parameterized values
<!-- actions: UPDATE_PARAMETER -->

If the wrong value comes from a parameter (`#{...}`), update the parameter
on the deployment. Do not edit the flow definition. A parameter update needs
no new catalog version and keeps the flow and its parameter context
consistent.

## Flow edits create a new catalog version
<!-- actions: SET_CONCURRENT_TASKS, SET_RUN_SCHEDULE, SET_BACKPRESSURE, SET_FLOWFILE_EXPIRATION, SET_PENALTY_YIELD, SET_RETRY_POLICY, SET_PROCESSOR_PROPERTY, ROUTE_RELATIONSHIP_TO_DLQ, SET_SCHEDULED_STATE -->

A change to a processor or connection setting is made by editing the flow
definition, importing it as a new version of the catalog flow, and then
moving the deployment to that version. Catalog versions cannot be deleted,
so every edit is permanent history. Make one coherent change per incident,
and put every step the fix needs into that one version. Do not edit the
deployment if it is already mid-update or runs a version other than the
one the incident was opened on: re-read its state first.

## KPIs are replaced as a whole list
<!-- actions: NO_ACTION -->

There is no command to add a single KPI. `update-flow-in-deployment --kpis`
replaces the deployment's whole KPI list, so any change must send the
existing KPIs back along with the new one. A version change must not carry
`--kpis`, or it would drop KPIs that are not in that call. Remediations do
not change KPIs.

## Choose the least disruptive lever

In order of preference: a parameter update, then a flow edit that restarts
only the affected components, then a deployment resize, which restarts
nodes. When the evidence is ambiguous, or the only fix would be outside the
flow (an endpoint that is down, an owner's decision), propose NO_ACTION and
say what a human should check. A healthy group with no breach also gets
NO_ACTION. Do not invent a change to show activity.
