---
title: Silent data loss
collection: nifi_playbooks
symptoms: [data_loss]
actions: [SET_FLOWFILE_EXPIRATION, SET_CONCURRENT_TASKS, ROUTE_RELATIONSHIP_TO_DLQ]
---

# Silent data loss

Some misconfigurations drop data without raising any alert: no queue
grows, no threshold is crossed, and the flow looks healthy. The only sign
is that the numbers do not add up. A stage sends out fewer FlowFiles than it
takes in, or a downstream stage receives fewer than the upstream sends, and
no queue between them is growing to hold the difference. The imbalance is
the evidence. A flat queue is a symptom here, not a sign of health.

## FlowFile expiration on a connection
<!-- actions: SET_FLOWFILE_EXPIRATION, SET_CONCURRENT_TASKS -->

A connection with a FlowFile Expiration (for example 30 sec) deletes every
FlowFile that waits longer than that. Behind a slow consumer, the queue
never grows past what fits in the expiration window, so it looks flat while
most of the data is discarded. Look for a non-zero expiration in the flow
inventory on the connection between the imbalanced stages.

The fix has two parts, applied together:

1. Set the expiration to 0 sec (never expire), so no data is dropped.
2. Give the consumer enough capacity (Concurrent Tasks, for example 4) to
   keep up with the arrival rate.

Removing the expiration alone turns the silent loss into a growing backlog,
which is better (nothing is lost) but not fixed. Adding capacity alone
still loses whatever waits too long.

## Auto-terminated failure relationship
<!-- actions: ROUTE_RELATIONSHIP_TO_DLQ -->

A failure relationship marked auto-terminate discards every FlowFile that
fails, with no trace downstream. The processor's FlowFiles Out stays below
its FlowFiles In by the failure rate. Route the failure relationship to the
process group's DLQ (the existing component whose name ends in DLQ). This
keeps the failed data for inspection and replay, and removes the
auto-termination.

Do not loop the failure back to the same processor. FlowFiles that failed
validation fail again, and the loop becomes a retry storm (see the failure
loops playbook).

## Verifying the fix

The conservation check balances again: what leaves a stage (including what
goes to the DLQ) matches what enters it. For expiration, the consumer's
FlowFiles In rises to the producer's rate and the queue stays bounded. For
an auto-terminated failure, the DLQ starts receiving FlowFiles: that is the
data that was being lost.
