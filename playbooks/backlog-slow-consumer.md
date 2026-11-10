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
  help, sized from the rate per task (below).
- The consumer takes in nothing at all: it is stopped, failing or
  misconfigured. That is not a capacity problem.
- Its FlowFiles go to failure or are dropped rather than queuing, by a
  FlowFile Expiration on its input connection for one: that is its routing
  or configuration, not only its capacity. See the failure-loop and
  data-loss playbooks before adding tasks.
- The deployment's CPU is high: the node cannot run more threads. See the
  sizing playbook.

Then find the intake, what has to be handled per second: the consumer's rate
in, plus how fast the queue in front of it grows (a queue growing by 600 a
minute adds 10/s). A queue held at its back-pressure threshold cannot grow,
and its source is throttled to what the bottleneck takes, so use the
source's earlier peak rate instead. No fix works unless the new capacity is
above the intake.

## Trace the symptom back to the originating stage
<!-- actions: SET_CONCURRENT_TASKS, SET_RUN_SCHEDULE, SET_FLOWFILE_EXPIRATION -->

When the stage the alert names keeps up with what it is sent, the cause is
elsewhere in the group. Walk the flow from the alert both ways, comparing
each stage's rate in with its rate out and with what arrives:

- Downstream, the last full queue in flow order marks the bottleneck: the
  stage it feeds is the one to change, and every stage before it is only
  back-pressured.
- Upstream, a source emitting far above its intended rate (a 0 sec
  schedule) floods everything after it: throttle it, as the back-pressure
  playbook's runaway producer section describes.
- Where the counts stop adding up and no queue grows to hold the
  difference, data is dropped there: see the data-loss playbook.

Change the stage where the readings change, not the one where they show.

## Raise Concurrent Tasks on the bottleneck processor
<!-- actions: SET_CONCURRENT_TASKS -->

When the consumer is below its ceiling with a backlog in front, raise
Concurrent Tasks. Size it from the measured rates: rate per task = current
rate / current tasks, and needed tasks = intake / rate per task, rounded up,
plus one for headroom. For example, 2 tasks on 0 sec taking in 6/s do 3/s
each; an intake of 15/s needs 5, so set 6. Stay within the catalog bound of
8, and do not go higher on a small node: every task is a thread from the
shared pool. Then add what it takes to drain the backlog: see "Size
concurrent tasks using intake and backlog".

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

## Size concurrent tasks using intake and backlog
<!-- actions: SET_CONCURRENT_TASKS, SET_RUN_SCHEDULE -->

A ceiling just above the intake stops the backlog growing but barely
drains it: what is already queued drains only at the capacity above the
intake. Against an intake of 15/s, 6 tasks at 3/s each (18/s) leave 3/s
spare, so 3,000 queued take about 17 minutes; 7 tasks (6/s spare) take
about 8. Size the fix for both, within the catalog bound of 8.

When the fix also makes each FlowFile cheaper, a shorter timeout on a slow
call for instance, still size the tasks from the intake: the cheaper
FlowFile alone rarely clears a backlog.

## Shorten the run schedule of a consumer that waits between runs
<!-- actions: SET_RUN_SCHEDULE -->

A consumer scheduled every 10 sec can run at most once per 10 seconds per
task. Even 8 tasks on that schedule give 0.8/s, so a source producing 1/s
still wins. Shorten the run schedule until Concurrent Tasks / run schedule is
above the intake: 1 sec on those tasks gives 8/s, and 0 sec (run whenever
there is work) removes the ceiling when the processor is cheap. Prefer the
smallest change that brings the ceiling above the intake, with enough to
spare to drain the backlog already waiting: the intake plus the backlog over
the time to clear it. An intake of 3/s with 1,200 queued, cleared in 10
minutes, needs 5/s: 1 task at 200 ms.

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
consumer), sized against the peak intake and the backlog a burst leaves, so
each burst clears before the next. A sawtooth that returns to near zero
every cycle and stays under the threshold needs no action.

## Unreachable endpoint slow consumer: route failures, lower timeout, increase capacity
<!-- actions: ROUTE_RELATIONSHIP_TO_DLQ, SET_PROCESSOR_PROPERTY, SET_CONCURRENT_TASKS -->

A consumer that calls a remote endpoint that does not answer (InvokeHTTP and
similar) is slow because each call waits out its connect timeout, not
because it has too few tasks. The fix has three parts, together: route its
failure relationships to the group's DLQ, lower the connect timeout, and
size Concurrent Tasks from the intake as above. The failure-loops
playbook's "Unreachable endpoint" section has the property names and
values. A lower timeout alone leaves the backlog; more tasks alone keep
every task waiting.

## Also consider connection expiration when consumer speed is increased
<!-- actions: SET_FLOWFILE_EXPIRATION, SET_CONCURRENT_TASKS -->

A consumer sped up behind a connection with a FlowFile Expiration still
loses whatever waits longer than the expiration while the backlog drains.
Check the input connection's expiration in the flow inventory: a non-zero
value on a queue behind a slow consumer drops data silently. Fix both
parts, as the data-loss playbook describes: no expiration, and capacity on
the consumer. Do not throttle a source that runs at its intended rate, or
change its batch size, to suit the consumer: that changes what the flow
takes in.

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
