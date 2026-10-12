---
title: Backlog behind a slow consumer
collection: nifi_playbooks
symptoms: [backlog]
actions: [SET_CONCURRENT_TASKS, SET_RUN_SCHEDULE, NO_ACTION]
---

# Backlog behind a slow consumer

A queue whose Queued Count rises steadily, bucket after bucket, means the
processor it feeds takes FlowFiles out more slowly than they arrive. The fix
is always on that consumer: give it more threads or let it run more often.
The queue itself is only where the problem shows.

## Diagnosis: confirm it is the consumer
<!-- actions: SET_CONCURRENT_TASKS, SET_RUN_SCHEDULE -->

Compare the producer's FlowFiles Out with the consumer's FlowFiles In over
the same buckets. If the consumer handles less than the producer sends, and
the queue between them grows by about the difference, the consumer is the
bottleneck. Then look at the consumer's settings in the flow inventory:

- Concurrent Tasks of 1 with a short run schedule (0 sec to a few hundred
  ms): the consumer is busy all the time and needs more threads.
- A long run schedule (several seconds or more): the consumer sits idle
  between runs. Here threads do not help much, so the run schedule is the
  lever.

A rough capacity check is: maximum rate = Concurrent Tasks x (1 / run
schedule) x FlowFiles per run. If that ceiling is below the arrival rate,
no amount of waiting will clear the queue.

## Raise Concurrent Tasks on the bottleneck processor
<!-- actions: SET_CONCURRENT_TASKS -->

When the consumer is busy all the time on a short schedule, raise Concurrent
Tasks. Size it from the measured rates: needed tasks = current tasks x
(arrival rate / service rate), rounded up, plus one for headroom. Going from
1 to 4 is a typical first step. Stay within the catalog bound of 8, and do
not go higher on a small node: every task is a thread from the shared pool.

Change only the processor directly in front of the growing queue. In a
multi-stage pipeline, the stage after a healthy queue is not the bottleneck,
and speeding it up changes nothing. The growing queue points at the stage to
fix.

Do not raise concurrency when the deployment's CPU is already saturated.
The extra threads compete for the same cores and throughput can drop. See
the sizing playbook.

## Shorten the run schedule of a consumer that waits between runs
<!-- actions: SET_RUN_SCHEDULE -->

A consumer scheduled every 10 sec can handle at most one batch per 10
seconds per task. Even 8 tasks on that schedule give 0.8 runs per second,
so a source producing 1 per second still wins. Shorten the run schedule to
match the arrival rate: 1 sec for a one-per-second source, or 0 sec (run
whenever there is work) when the processor is cheap. Prefer the smallest
change that brings capacity above the arrival rate.

A shorter run schedule is also an acceptable alternative to more threads for
a busy consumer (for example 250 ms instead of 1 sec), but threads scale
better when each run is slow.

## Bursty arrivals: judge by peak and drain time
<!-- actions: SET_CONCURRENT_TASKS, NO_ACTION -->

Some sources arrive in bursts, and the queue follows a sawtooth: it climbs
during a burst and drains between them. A peak above the threshold is a
real problem only if the queue does not drain before the next burst, or the
peaks get higher each time. Then add drain capacity (Concurrent Tasks on the
consumer) so each burst clears faster. A sawtooth that returns to near zero
every cycle and stays under the threshold needs no action.

## What does not fix a backlog
<!-- actions: NO_ACTION -->

- Raising the back-pressure threshold only lets the queue hold more. The
  backlog keeps growing, now with more memory and disk at risk.
- Restarting the deployment or the processor does not drain anything. A
  measured restart made a backlog worse (428 -> 883 queued) because the
  source kept producing while the consumer was down.
- Emptying the queue clears the alert but discards data, and the growth
  comes straight back.

## Verifying the fix

The fix worked when the queue's slope turns negative (it drains) or holds
flat under its threshold, and the consumer's FlowFiles In rises to match
the producer. A level that is still rising, just more slowly, is only a
partial fix. Judge by the trend over the verify window, not by the alert
clearing.
