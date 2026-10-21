---
title: Backlog behind a slow consumer
collection: nifi_playbooks
symptoms: [backlog]
actions: [SET_CONCURRENT_TASKS, SET_RUN_SCHEDULE, NO_ACTION]
---

# Backlog behind a slow consumer

A queue whose Queued Count rises steadily, bucket after bucket, means the
processor it feeds takes FlowFiles out more slowly than they arrive. The fix
is on the bottleneck, the first stage that cannot keep up, sized so that its
new capacity is above the intake. The queue itself is only where the problem
shows, and in a long pipeline it may be several stages before the bottleneck.

## Diagnosis: confirm it is the consumer
<!-- actions: SET_CONCURRENT_TASKS, SET_RUN_SCHEDULE -->

Find the bottleneck first. Follow the group's queues in flow order: the
bottleneck is the stage whose input queue is full or growing and whose output
queue is not. A queue further upstream may be the one that alerted, but it is
full only because back pressure stops the stage that feeds it, and speeding
that stage up passes no more.

Then compare the bottleneck's measured rate in with its ceiling, from the
flow inventory:

    ceiling = Concurrent Tasks / run schedule

in FlowFiles per second, since most processors take one FlowFile per run. A
run schedule of 0 sec puts no ceiling on it.

- The rate is at the ceiling: the schedule is the limit. 1 task every 125 ms
  has a ceiling of 8/s; if it takes in 8/s, it is waiting between runs.
  Shorten the run schedule or add tasks.
- The rate is below the ceiling and a backlog waits in front: each FlowFile
  takes time to process and the tasks are busy all the time. Only more tasks
  help.
- The consumer takes in nothing at all: it is stopped, failing or
  misconfigured. That is not a capacity problem.
- The deployment's CPU is high: the node cannot run more threads. See the
  sizing playbook.

Then find the intake, what has to be handled per second: the consumer's rate
in, plus how fast the queue in front of it grows (a queue growing by 600 a
minute adds 10/s). A queue held at its back-pressure threshold cannot grow,
and its source is throttled to what the bottleneck takes, so use the
source's earlier peak rate instead. No fix works unless the new capacity is
above the intake.

## Raise Concurrent Tasks on the bottleneck processor
<!-- actions: SET_CONCURRENT_TASKS -->

When the consumer is below its ceiling with a backlog in front, raise
Concurrent Tasks. Size it from the measured rates: rate per task = current
rate / current tasks, and needed tasks = intake / rate per task, rounded up,
plus one for headroom. For example, 2 tasks on 0 sec taking in 6/s do 3/s
each; an intake of 15/s needs 5, so set 6. Stay within the catalog bound of
8, and do not go higher on a small node: every task is a thread from the
shared pool.

More tasks also raise the ceiling of a consumer that waits between runs, but
count it. 1 task every 125 ms gives 8/s, so 2 tasks give 16/s: against a
20/s intake the backlog still grows, only more slowly. 3 tasks (24/s) are
just enough and 4 leave headroom; a shorter schedule clears it too.

Change only the bottleneck. A stage after a healthy queue is not the
bottleneck, and neither is a stage that is back-pressured by a full queue
after it: speeding either up changes nothing.

Do not raise concurrency when the deployment's CPU is already saturated.
The extra threads compete for the same cores and throughput can drop. See
the sizing playbook.

## Shorten the run schedule of a consumer that waits between runs
<!-- actions: SET_RUN_SCHEDULE -->

A consumer scheduled every 10 sec can run at most once per 10 seconds per
task. Even 8 tasks on that schedule give 0.8/s, so a source producing 1/s
still wins. Shorten the run schedule until Concurrent Tasks / run schedule is
above the intake: 1 sec on those tasks gives 8/s, and 0 sec (run whenever
there is work) removes the ceiling when the processor is cheap. Prefer the
smallest change that brings the ceiling above the intake.

For a busy consumer at its ceiling on a short schedule, a shorter schedule
and more tasks both raise the ceiling: 1 task at 1 sec against an intake of
3/s clears with 4 tasks or with 250 ms (4/s either way). Shortening the
schedule of a consumer that is below its ceiling does nothing, because it is
not waiting between runs.

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
