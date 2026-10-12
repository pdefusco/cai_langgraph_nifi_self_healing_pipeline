---
title: Parameter and property misconfiguration
collection: nifi_playbooks
symptoms: [misconfiguration]
actions: [UPDATE_PARAMETER, SET_PROCESSOR_PROPERTY, NO_ACTION]
---

# Parameter and property misconfiguration

The flow is built right but configured with a wrong value: a URL, a JSON
path, a batch size or a copy count. The symptoms look like other problems
(failures, backlogs, floods), but threads, schedules and thresholds do not
fix them. Only the right value does. Before choosing a throughput or
retry lever, check the configured values of the processors involved.

## Values that come from parameters: update the parameter
<!-- actions: UPDATE_PARAMETER -->

When a property's value is a parameter reference such as
`#{target.url}`, the value lives in a parameter context, which CDF exposes
as a parameter group on the deployment. Fix it with a parameter update
(`update-flow-in-deployment --parameter-groups`). This needs no new flow
version and restarts only the components that reference the parameter.

Do not edit the property in the flow definition to replace the parameter
reference with a literal value. That forks the flow from its parameter
context: the next deployment of the flow with the correct context would
silently undo it, and other components that use the parameter keep the
wrong value. Timeouts and Concurrent Tasks do not fix a wrong endpoint.

## Runbook: correct values for the nfh-p5-parameters context
<!-- actions: UPDATE_PARAMETER -->

These are the production values. Any other value in a running deployment is
a misconfiguration.

| Parameter context | Parameter | Correct value | Notes |
|---|---|---|---|
| nfh-p5-parameters | `s17.target.url` | `http://localhost:9876/ingest` | The partner ingest endpoint. An address that does not answer, such as a 10.x host on port 8080, is a stale value from another environment. |
| nfh-p5-parameters | `s18.copies` | `1` | Copies per incoming FlowFile in production. Values such as 50 are load-test settings and flood the downstream queue. |

When a parameter differs from this table, propose UPDATE_PARAMETER with the
correct value from the table. When a parameter is not in the table, do not
guess a value: propose NO_ACTION and say which parameter needs an owner's
input.

## Processor properties set to the wrong value
<!-- actions: SET_PROCESSOR_PROPERTY -->

Properties set directly on the processor are fixed with a flow edit
(SET_PROCESSOR_PROPERTY), which creates a new flow version.

A common case is EvaluateJsonPath with a JSON path that does not match the
records. Every FlowFile then goes to unmatched or failure and nothing
reaches the success path. Compare the path with the field names in the
payload. When the upstream producer is a GenerateFlowFile, its Custom Text
in the flow inventory shows the payload. Watch for naming styles: a path of
`$.orderId` does not match a field named `order_id`. The fix is the
corrected path on the same dynamic property (for example `$.order_id`).

Do not speed up the unmatched or quarantine branch to make the queue
smaller. That moves the records out of sight without extracting them, and
the orders are effectively lost.

## Verifying the fix

The symptom the wrong value caused goes away: the failure queue stops
growing, the success path's FlowFiles Out recovers, or the flooded queue
drains to its normal level.
