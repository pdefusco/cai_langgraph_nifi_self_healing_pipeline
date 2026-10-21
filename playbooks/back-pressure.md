---
title: Back pressure
collection: nifi_playbooks
symptoms: [backpressure]
actions: [SET_BACKPRESSURE, SET_RUN_SCHEDULE, SET_CONCURRENT_TASKS]
---

# Back pressure

Every connection has two back-pressure thresholds: an object count and a
data size. When either is reached, Percent Full is at 100% and NiFi stops
scheduling the processor that feeds the connection. Upstream stalls, and
its own input queues start to fill. Back pressure is NiFi working as
designed. The question is whether the threshold is wrong, or the threshold
is right and something upstream or downstream is wrong.

## Diagnosis: which threshold, and why
<!-- actions: SET_BACKPRESSURE, SET_RUN_SCHEDULE, SET_CONCURRENT_TASKS -->

Read the connection's Queued Count, Queued Bytes and Percent Full together
with its thresholds in the flow inventory.

- Queued Count is at the object threshold and the threshold is small (tens
  of FlowFiles): the threshold is too low for normal traffic. Raise it.
- Queued Bytes is at the size threshold while the count is far below the
  object threshold: large files fill the size limit first. Only the size
  threshold matters here; raising the object count changes nothing.
- The queue sits at a sensible threshold (thousands of objects) and the
  producer emits far more than the consumer can take: the threshold is
  doing its job. Fix the rate on one side or the other (see below), not the
  threshold.

Back pressure travels upstream, so several queues in a row can be full. The
one to act on is the last of them: the stage it feeds is the bottleneck, and
the stages before it are back-pressured, not slow. Size the bottleneck's fix
by its ceiling (Concurrent Tasks / run schedule) against the intake, as the
backlog playbook describes.

## Raise a threshold that is too low for normal traffic
<!-- actions: SET_BACKPRESSURE -->

A threshold below the normal working set throttles a healthy flow. Raise the
object threshold to the NiFi default range (1000 to 10000). For a size
threshold, allow at least a few hundred normal-sized files: 100 MB to 1 GB
for megabyte-sized files. Keep the thresholds within the catalog bounds and
within what the node's content repository can hold.

Do not raise a threshold to absorb a growing backlog. If the queue would
still grow after the change, the threshold was never the cause.

## Fan-out flooding a slow sink
<!-- actions: SET_RUN_SCHEDULE, SET_CONCURRENT_TASKS -->

SplitText, DuplicateFlowFile and similar processors turn one FlowFile into
many. If the processor after the fan-out is scheduled slowly, the queue
between them fills and the splitter stalls. Speed up the consumer: a run
schedule of 0 sec (run whenever there is work) or more Concurrent Tasks on a
short schedule.

Size it against what the fan-out sends, not what it takes in: 2 FlowFiles/s
split 50 ways is 100/s. A consumer on 1 task every 20 ms has a ceiling of
50/s, and 2 tasks reach only 100/s, with no headroom; 4 tasks (200/s) or
0 sec clear it.

The fan-out factor (line count, Number of Copies) is business logic. Do
not reduce it to make the queue smaller, because that changes what the flow
outputs. Raising the threshold only delays the stall.

## Runaway producer on a 0 sec schedule
<!-- actions: SET_RUN_SCHEDULE -->

A source such as GenerateFlowFile on a run schedule of 0 sec produces as
fast as the thread allows, thousands per second, which no realistic
consumer matches. The queue pins at its threshold and Percent Full stays at
100%. Throttle the producer to its intended rate with a run schedule such
as 1 sec. Adding consumer threads cannot match an unbounded source.

## Verifying the fix

Percent Full falls below 100% and stays there, and the upstream
processor's FlowFiles Out recovers. For a fan-out or a runaway source, the
queue should hold a steady level well under the threshold.
