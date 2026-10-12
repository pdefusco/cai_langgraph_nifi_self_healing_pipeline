---
title: Batching latency and stuck flows
collection: nifi_playbooks
symptoms: [latency, stuck]
actions: [SET_PROCESSOR_PROPERTY, SET_SCHEDULED_STATE]
---

# Batching latency and stuck flows

A processor whose FlowFiles Out stays at zero, or far below its input, while
its input queue holds FlowFiles, is either waiting for a condition that
never comes or is not running at all. The queue may grow only slowly, so a
count threshold can take a long time to fire. A rate KPI that stays below
its floor is often the first sign.

## MergeContent bin that never fills
<!-- actions: SET_PROCESSOR_PROPERTY -->
<!-- symptoms: latency -->

MergeContent releases a bin when it reaches Minimum Number of Entries, or
when the bin is older than Max Bin Age. With no Max Bin Age and a minimum
the traffic rarely reaches, FlowFiles sit in the bin indefinitely: output
is zero and latency has no upper bound. Set Max Bin Age (for example 60
sec, or up to 5 min for a slow trickle). This bounds latency and still
lets full bins go out early.

Do not lower Minimum Number of Entries to 1. That releases every FlowFile
on its own and defeats the batching the flow was built for. Downstream
systems then receive many small files.

## Disabled processor
<!-- actions: SET_SCHEDULED_STATE -->
<!-- symptoms: stuck -->

A processor in the DISABLED state is never scheduled. Its input queue fills
and nothing comes out. The flow inventory shows the processor's scheduled
state. Enable it so it runs with its configured schedule (scheduled state
ENABLED in the flow version). Concurrency, run schedule and threshold
changes on a disabled processor do nothing, because it never runs.

Check that the processor is otherwise valid before enabling it: a processor
disabled on purpose because of a configuration problem will go invalid as
soon as it is enabled.

## Verifying the fix

The processor's FlowFiles Out rises above its floor and stays there, and
its input queue drains or holds a steady level. For batching, the output
arrives in regular batches no further apart than the Max Bin Age.
