---
title: Deployment sizing and cost
collection: nifi_playbooks
symptoms: [sizing_cpu, sizing_cost]
actions: [SCALE_DEPLOYMENT, SET_PROCESSOR_PROPERTY, NO_ACTION]
---

# Deployment sizing and cost

A DataFlow deployment runs on NiFi nodes of one cluster size (EXTRA_SMALL,
SMALL, MEDIUM, LARGE) with a static node count or an autoscaling range.
When the flow needs more than the nodes have, CPU stays high and queues grow
in front of the heavy processors, whatever their settings. When the nodes
far exceed what the flow needs, the deployment wastes money. Resizing
(`update-deployment --cluster-size / --static-node-count`) restarts the
NiFi nodes one by one, so it is a heavier change than a flow edit.

## CPU-bound flow: scale up or out
<!-- actions: SCALE_DEPLOYMENT, SET_PROCESSOR_PROPERTY -->
<!-- symptoms: sizing_cpu -->

Sustained CPU above about 85%, together with a growing queue in front of a
CPU-heavy processor (CompressContent at a high level, HashContent, record
transforms on large files), means the nodes are the bottleneck.

- Scale the deployment: a larger cluster size with 2 nodes (for example
  SMALL with 2 nodes), or the same size with 2 nodes. Scaling out helps only
  when the busy connection load-balances across nodes. A flow that keeps
  all its data on the primary node gains nothing from a second node.
- Or make the work cheaper when the setting allows it. For example,
  Compression Level 1 instead of 9 cuts CPU many times over, at a small
  cost in compression ratio. This is acceptable only if nothing downstream
  depends on the higher ratio.

Do not raise Concurrent Tasks on a CPU-saturated node. The extra threads
compete for the same cores, adding contention and context switching, and
throughput stays flat or drops.

## Over-provisioned deployment: downsize
<!-- actions: SCALE_DEPLOYMENT, NO_ACTION -->
<!-- symptoms: sizing_cost -->

A deployment whose CPU stays in the low single digits for its whole history,
with flat, near-empty queues, is paying for capacity it does not use. A
MEDIUM cluster with several nodes carrying a trickle is the typical case.
Propose a downsize to the smallest size that keeps headroom: EXTRA_SMALL with
1 node for a trickle, or SMALL with 1 node if the load has bursts.

A downsize is a cost optimization, not a fix, so it is never urgent. Make it
only when the history covers the flow's normal peaks. If CPU or queues show
regular peaks, keep enough headroom for them, or propose NO_ACTION.

## Verifying the fix

After scaling up or out, the queue in front of the heavy processor drains
and CPU falls below its threshold. A downsize succeeded only if the queues
stay flat and CPU stays well below its threshold for the whole verify
window afterwards. A queue that starts to grow after a downsize means the
deployment went too small, and the change should be rolled back.
