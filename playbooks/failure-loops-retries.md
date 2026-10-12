---
title: Failure loops, retries and unreachable endpoints
collection: nifi_playbooks
symptoms: [failure_loop]
actions: [ROUTE_RELATIONSHIP_TO_DLQ, SET_RETRY_POLICY, SET_PROCESSOR_PROPERTY, SET_CONCURRENT_TASKS, SET_PENALTY_YIELD]
---

# Failure loops, retries and unreachable endpoints

FlowFiles that fail and are sent straight back to the same processor are
retried forever. A queue that grows on a failure or retry connection, while
the success path carries little or nothing, is the sign. The data is not
lost yet, but it is not moving, and every retry costs a thread.

## Unbounded failure self-loop
<!-- actions: ROUTE_RELATIONSHIP_TO_DLQ -->

A failure relationship connected back to its own processor, with no limit,
retries poison FlowFiles indefinitely. FlowFiles that will never succeed
pile up and slow down the good ones. Route the failure relationship to the
process group's dead-letter queue (the existing component whose name ends
in DLQ), so failed FlowFiles are kept for inspection instead of being
retried.

Raising Concurrent Tasks on a looping processor multiplies the retry storm
and makes it worse. Changing the retry policy alone does not help either:
a FlowFile that will never succeed still comes back.

## Unreachable endpoint (InvokeHTTP and other remote calls)
<!-- actions: ROUTE_RELATIONSHIP_TO_DLQ, SET_PROCESSOR_PROPERTY, SET_CONCURRENT_TASKS -->

When a remote host does not answer, each call holds a thread for the full
connect timeout (often 30 sec or more), and the Failure, No Retry and Retry
relationships fill up. The endpoint being down cannot be fixed from the
flow, so the remediation contains the damage:

1. Route Failure, No Retry and Retry to the DLQ so failed requests leave the
   loop and can be replayed later.
2. Lower Socket Connect Timeout (for example to 5 secs) so each failed call
   frees its thread quickly.
3. Raise Concurrent Tasks moderately (for example 4) so the healthy requests
   still get through while some calls wait.

These are one fix, applied together. If the endpoint is wrong rather than
down, and its URL comes from a parameter, see the parameter
misconfiguration playbook: correct the parameter instead.

## Processor-wide yield instead of per-FlowFile penalty
<!-- actions: SET_RETRY_POLICY, SET_PENALTY_YIELD -->

A retry policy with Backoff Mechanism YIELD_PROCESSOR stops the whole
processor after each failure. One transient error then blocks every
FlowFile behind it, and throughput collapses even though most FlowFiles
would succeed. Switch the backoff to PENALIZE_FLOWFILE so only the failing
FlowFile waits its penalty while the rest keep flowing. A Max Backoff
Period of about 1 min keeps a persistent failure from retrying too fast.

An over-long Penalty Duration or Yield Duration has a similar effect: lower
it so transient failures retry within seconds, not minutes. More Concurrent
Tasks do not help while the processor is yielding.

## Verifying the fix

The failure or retry queue stops growing and the success path's FlowFiles
Out recovers. A DLQ that receives FlowFiles after the change is expected:
that is where the poison data is now kept. Judge the fix by the main path's
throughput, not by the DLQ count.
