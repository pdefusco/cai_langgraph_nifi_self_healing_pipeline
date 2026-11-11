"""
The hand-written part of flows/scenarios/README.md: an introduction to each
pack and a walkthrough of each scenario, for people reviewing the scenarios.

scenario_docs.py adds what can be generated from the definitions (the
diagram, the grading summary and the settings tables), so the text here
explains what the flow is meant to be, what is wrong with it, how the numbers
work out, and why the tempting fixes don't work. Nothing here is shown to the
agent. Paragraphs are separated by blank lines; line breaks inside a
paragraph don't matter.

When a scenario's numbers change, change its walkthrough too.
"""

from flows.scenarios import p23_siblings, p24_healthy_500, p25_nested, scale

PACKS = {
    "p1-throughput": """
        The first pack is about the most common problem there is, a flow that can't keep up. Each scenario is a
        small flow where one processor is slower than what's arriving, and what differs is the reason it's slow.
        That reason decides which setting is the right one to change.
    """,
    "p2-backpressure": """
        These scenarios are all about back pressure. In two of them the threshold is simply set too low for the
        traffic. In the other two the threshold is fine and something is producing far more than it should, so
        raising the threshold would only hide the problem.
    """,
    "p3-errors": """
        This pack is about error handling. A failure relationship that loops straight back, an endpoint that
        never answers, a JSON path that matches nothing and a retry policy that stalls the whole processor all
        look like a slow or stuck flow from the outside. Each one needs a different fix.
    """,
    "p4-dataloss-latency": """
        The faults in this pack are the quiet ones. Two of them lose data without any queue growing, one holds
        data back for over an hour, and one is just a processor that somebody forgot to turn back on.
    """,
    "p5-parameters": """
        This pack's deployment has a parameter context, `nfh-p5-parameters`, and two of its scenarios are wrong
        because of a parameter value rather than anything in the flow itself. The right fix is to change the
        parameter, not the processor property that refers to it. The third scenario is a healthy control.
    """,
    "p6-sizing-cpu": """
        A flow that is configured correctly but runs on a node that is too small for it. The only real fixes
        are a bigger deployment or less work per FlowFile.
    """,
    "p7-sizing-overprov": """
        The opposite of p6. Nothing is broken here, but the deployment is much bigger than the flow needs, so
        the problem is cost. It's the only scenario whose alert is on CPU being too low.
    """,
    "p8-orders": """
        p8 is the first of the cascade packs, where the alert fires several stages away from the actual cause.
        Both scenarios are order pipelines of about ten processors. In S22 back pressure from a merge works its
        way back to ingest, and in S23 a throttle lets only a third of the shipments through.
    """,
    "p9-clickstream": """
        Two clickstream pipelines. In S24 a split multiplies the traffic in front of a single-task sessionizer.
        S25 has the same kind of slow sessionizer, but its buffer expires FlowFiles, so the data is lost instead
        of queuing up.
    """,
    "p10-iot": """
        Two IoT pipelines for sensor readings. In S26 an enrichment call that is always refused retries forever
        until the whole flow is blocked, and in S27 a validation step quietly drops 15% of the readings.
    """,
    "p11-payments": """
        Two payment pipelines. In S28 one of three load-balanced lanes has stopped and the other two cover for
        it, so the totals look normal. In S29 the compression level is the bottleneck, and the backlog it causes
        reaches ingest.
    """,
    "p12-warehouse": """
        p12 starts the compound scenarios, which have more than one fault and aren't fixed until all of them
        are. In S30 two stages are both too slow. In S31 a stalled stage hides a second fault further down that
        only shows up once data is flowing again.
    """,
    "p13-billing": """
        Two more compound scenarios, in a billing flow with its own parameter context, `nfh-p13-billing`. Each
        one mixes a parameter fault with a flow fault, so it takes at least two separate proposals to fix.
    """,
    "p14-capacity": """
        One compound scenario that mixes sizing with a flow fault. Part of the group needs a bigger node, and a
        different part has a slow processor that no amount of hardware will speed up.
    """,
    "p15-bursts": """
        Intermittent faults. These flows are fine most of the time and only go wrong on a schedule, driven by
        CRON generators, so a fix can't be judged on what the flow looks like at an arbitrary moment.
    """,
    "p16-slow-burn": """
        Slow burns. Both flows look healthy for hours after they're deployed, and each takes two to three hours
        to reach its alert. They check whether the agent can spot a trend before it becomes an outage.
    """,
    "p17-nightly-batch": """
        A sizing fault that only shows up for 40 minutes a night. The rest of the day the node is nearly idle,
        so the alert, and the proof that a fix worked, both depend on the time of day.
    """,
    "p18-noisy-bursts": """
        The first of the control packs. Nothing here is broken, but both flows have behaviour that looks
        alarming at first glance: a queue that climbs steeply twice an hour, and a dead-letter queue that fills
        up every hour.
    """,
    "p19-daily-cycle": """
        Two more controls, both with load that follows the clock. One rises and falls over the working day, and
        the other is idle for 23 hours and busy for one.
    """,
    "p20-big-healthy": """
        The last controls are the biggest and busiest flows in the set. S44 is a 30-processor pipeline that uses
        most of the patterns from the other packs, and S45 keeps the CPU busy all day without being undersized.
    """,
    "p21-wide-100": """
        The first of the customer-sized packs. Each scenario is a fault we already have, copied from a small pack
        into a process group of 100 processors, where the rest of the group is healthy flows of its own that never
        touch the fault. The fault, the fix and the traps are the small scenario's, so the only thing that changes
        is how much flow the agent has to read past to find it, and comparing each one with its twin shows what
        size alone does to the diagnosis and to how long it takes.
    """,
    "p22-wide-250": """
        Two faults we already have, each in a group of 250 processors, and each beside one more healthy flow, a
        decoy, that looks like the fault: a merge that batches beside a merge that deadlocks, and a validator that
        retries its own failures and keeps up beside one that doesn't. The decoy's queue is charted for the agent,
        so the readings show both, and changing the decoy is one of the wrong fixes the tests check.
    """,
    "p23-siblings": """
        Many small process groups in one deployment, as customers organize flows by team or source. Three are
        faults we already have, each in a group about the size of its neighbours, and the other 24 are healthy
        controls of 8 to 12 processors. Each fault has to be found in its own group, and none of the siblings may
        alert or be changed, however many groups there are to poll and read.
    """,
    "p24-healthy-500": """
        The largest control: 500 healthy processors in 20 groups of 25, with no fault anywhere. It measures false
        alarms, and the poll and prompt load, at the biggest size we test. The controls are generated, so their
        walkthroughs are too.
    """,
    "p25-nested": """
        Customer flows nest process groups two or three levels deep and pass FlowFiles between them through input
        and output ports. Here three faults we already have sit whole one or two groups down, in "Region" and in
        it "Site", with healthy filler on every level and a crossing into each group: a source above, the input
        port, a few stages, the output port and a sink above again. The agent's inventory shows the groups inside
        by their path, its edits find a processor wherever it is in them, and a route to a DLQ must stay inside
        the processor's own group, as NiFi requires. S101 and S102 are nested controls.
    """,
    "p26-holdout-a": """
        The first of two holdout packs: faults of kinds the catalog fixes and the playbooks cover, in domains,
        names and numbers that no prompt, playbook or replay was tuned on, here carrier billing, parcel tracking
        and pharmacy. The default replay leaves the holdouts out and reviews of their incidents draft no playbook
        edits, so they keep measuring what the agent does on a fault it hasn't seen. Every fault is fixed with a
        low-risk flow edit, and S106 is the control.
    """,
    "p27-holdout-b": """
        The second holdout pack, in identity checks, onboarding and ad auctions, held out in the same way as
        p26-holdout-a. Three of its faults are fixed with low-risk flow edits; S108's fix is a DLQ route, which is
        medium risk and always goes to a person. S110 is the control.
    """,
}

WALKTHROUGHS = {
    "S01": """
        This is the simplest scenario we have, and the others build on it. A source creates three small
        FlowFiles every second and hands them to S01 Consumer, an UpdateAttribute that stands in for some real
        processing step. The consumer has a single concurrent task and runs once a second, so it moves one
        FlowFile a second while three arrive.

        That leaves two FlowFiles a second behind, 120 a minute, and `s01-backlog` passes the alert threshold of
        500 roughly four minutes after the flow starts. It never levels off, because nothing about the
        imbalance changes.

        The fix is to give the consumer more capacity. Four concurrent tasks gives it four a second, and a
        300 ms run schedule gives it 3.3 a second with a single task, so we accept either one. Raising the back
        pressure threshold on `s01-backlog` is the tempting wrong answer, since it makes the alert go away for a
        while, but the queue just grows bigger. Restarting the deployment doesn't drain anything either.
    """,
    "S02": """
        Here the drain already has four concurrent tasks, which looks like plenty, but it's scheduled to run
        only every ten seconds. Each task wakes up, takes one FlowFile and goes back to sleep, so the drain
        manages four FlowFiles every ten seconds, 0.4 a second, against one a second arriving.

        Concurrency can't fix this one. Even at eight tasks, the most the agent is
        allowed to set, the drain would reach 0.8 a second and the queue would still grow. The setting that's
        actually wrong is the run schedule. Changing it to 1 sec gives four a second. We accept 3 sec too, 1.3
        a second, the slowest schedule that kept up in every test round (3.6 sec didn't), though it takes
        longer to drain.
    """,
    "S03": """
        A three-stage pipeline with a weak middle. Two FlowFiles arrive each second. S03 Enrich and S03 Publish
        each have four tasks on a one second schedule, so they can do four a second, but S03 Transform, between
        them, has only one task and does one a second.

        All three connections have an alert at 300, but only `s03-to-transform`, the queue in front of the slow
        stage, ever grows. The queue in front of Enrich stays empty, and so does the one in front of Publish,
        which only ever sees what Transform lets through.

        The test is whether the agent finds the right stage. Four tasks on Transform, or a 250 ms schedule,
        fixes it. Adding tasks to Enrich or Publish changes nothing, because neither of them has a backlog.
    """,
    "S04": """
        This one comes from an oscillator pattern used in some of the SIB examples. Every ten minutes the
        source drops a burst of 600 FlowFiles, and the drain takes them at one a second. A burst takes the whole
        ten minutes to clear, so the queue saws between 600 and zero and sits above the alert threshold of 300
        for about half of every cycle.

        The fix is a faster drain. Four tasks or a 250 ms schedule clears a burst in two and a half minutes.
        What makes it worth testing is the verdict afterwards. A sawtooth isn't a leak, and a single good or bad
        sample doesn't tell you much, so a fix has to be judged on how high the peaks go and how fast they
        drain.
    """,
    "S05": """
        Every 30 seconds the source sends a burst of 50 FlowFiles into `s05-buffer`, and S05 Writer takes them
        at two a second. On average the writer has more than enough capacity, since a burst is gone in 25
        seconds. The problem is that the buffer back-pressures at just ten FlowFiles, so it can't even hold one
        burst. The source gets blocked for most of each cycle and the queue keeps hitting 100% of its limit.

        The fix is a sensible threshold. We grade 1,000 objects as the design fix and accept the NiFi default
        of 10,000 as well. More writer tasks would drain each burst faster, but the next burst would still run
        into a ten-FlowFile limit.
    """,
    "S06": """
        This is the size-based version of S05. Every ten seconds the source produces ten files of 1 MB each,
        and the queue in front of the consumer is limited to 2 MB, so two files fill it. The consumer handles
        one file a second, which matches the average rate, but the queue is pinned at its limit and the source
        is blocked again and again.

        The fix is to raise the data size threshold, to 1 GB (the NiFi default) or at least 100 MB. Raising the
        object threshold does nothing here, because the limit being hit is the size, not the count, and the
        scenario checks that the agent notices the difference.
    """,
    "S07": """
        A source sends one FlowFile every ten seconds to S07 Fan Out, a DuplicateFlowFile that makes 100 copies
        of each one. That's 101 FlowFiles every ten seconds, 10.1 a second, landing on S07 Sink, which runs once
        a second. The queue between them, `s07-fanout`, passes 1,000 in under two minutes.

        It's tempting to treat the fan-out as the problem and lower Number of Copies, but in this flow the
        copies are the business logic, and changing them changes what the flow produces. Raising the threshold
        on `s07-fanout` just stores the flood. The design fix is to let the sink run as fast as it can, with a
        0 sec schedule.

        A 20 ms schedule, 50 a second, is accepted too. Settings that only just keep up, like 50 or 100 ms or
        more tasks, don't help: the FlowFiles waiting behind the fan-out keep `s07-fanout` at its limit until
        they're through, and only a sink well ahead of 10.1 a second clears them in time.
    """,
    "S08": """
        S08 Runaway Source is a GenerateFlowFile with a run schedule of 0 sec, so it produces FlowFiles as fast
        as NiFi will let it, hundreds a second. The consumer has two tasks on a one second schedule. The queue
        between them is full within moments and stays at its limit.

        No consumer setting the agent is allowed to make could keep up with that, since the most it can set is
        eight tasks. The fix is on the producer. Putting the source back on a one second schedule brings it down
        to one FlowFile a second, which the consumer handles easily.
    """,
    "S09": """
        S09 Source sends one order record a second, and one in ten is malformed: `{"id": }`, with no value.
        S09 Validator is an EvaluateJsonPath that reads `$.id` from each one. A malformed record can't be parsed,
        so it goes to failure, and failure goes straight back into the validator through `s09-failure-loop`, with
        no limit on how many times a FlowFile can go around. It fails again every time, because nothing about it
        has changed.

        The validator runs four times a second, so it keeps up with the input and retries the loop with what is
        left. The good records go through, `s09-input` stays near empty, and the loop grows by one record every
        ten seconds, passing 100 after about 17 minutes. Left alone, it fills to its limit and back pressure stops
        the validator, and then the input backs up too. There's an S09 DLQ on the canvas, but it's disabled and
        nothing is connected to it.

        The fix is to route failure to the DLQ instead of back into the validator. The agent's DLQ edit repoints
        the self-loop at S09 DLQ and enables it, and the loop empties into it. More validator tasks or a 0 sec
        schedule are the obvious mistakes: they retry the same malformed records faster, and none of them will
        ever parse. The tests check that both leave the loop growing. Setting a retry policy on its own isn't
        enough either, because once retries are exhausted the FlowFiles still land back on the self-loop.
    """,
    "S10": """
        S10 Partner Call is an InvokeHTTP that posts orders to a partner at `10.255.255.1`. That address isn't
        routable, so instead of being refused, every connection just hangs until the 60 second connect timeout
        runs out. All three of the processor's failure relationships, Failure, No Retry and Retry, go to
        `s10-retry-loop`, which feeds straight back in. With one task and a request arriving every two seconds,
        the processor gets through one call a minute while thirty new requests arrive.

        We grade the design fix as three changes together. Route the failure relationships to S10 DLQ so failed
        calls stop coming back, cut the connect timeout to 5 seconds so a dead call fails fast, and give the
        processor four tasks. That's 0.8 calls a second against 0.5 arriving.

        Concurrency on its own doesn't come close. Even eight tasks at one call a minute each is about 0.13 a
        second. And none of this makes the partner answer, because the endpoint really is down. The aim is a
        flow that fails quickly and keeps the failed orders somewhere they can be replayed later.
    """,
    "S11": """
        The source generates small order records like `{"order_id": 42, "amount": 10.5}`. S11 Extract Order,
        an EvaluateJsonPath, is supposed to pull out the order id, but its `orderId` property asks for
        `$.orderId`, in camel case, and the field is called `order_id`. Not a single record matches.

        Everything goes down the unmatched route to S11 Quarantine, a LogAttribute that only runs every ten
        seconds. So `s11-unmatched` grows by almost one FlowFile a second and crosses 100 within a couple of
        minutes, while S11 Publish receives nothing at all.

        The quarantine looks like the bottleneck, and that's the trap. If the agent speeds it up, the queue
        clears and the alert goes away, but every order is still being quarantined. The real fix is the path.
        Setting `orderId` to `$.order_id` sends the records to Publish where they belong, which is why FlowFiles
        into S11 Publish is one of the KPIs that decide the outcome.
    """,
    "S12": """
        S12 Transient Service is a DebugFlow that fails three times in ten, standing in for a service with
        occasional transient errors. It uses NiFi's built-in retry on the failure relationship, which is a good
        idea, but the backoff mechanism is YIELD_PROCESSOR with a ten minute ceiling. With that setting a retry
        doesn't just delay the FlowFile that failed, it yields the whole processor. Both tasks wait, starting at
        ten seconds and doubling each time the same FlowFile fails again, up to ten minutes.

        That works out at about 7.5 seconds of stall per FlowFile on average, while a new FlowFile arrives every
        second, so `s12-requests` keeps growing even though most requests would succeed on their first try.

        The fix is to switch the backoff to PENALIZE_FLOWFILE, which only holds back the FlowFile that failed
        and lets the processor carry on with the others. We also accept that change with a lower ceiling of one
        minute. More concurrent tasks is the instinctive response and doesn't help, because a yielded processor
        holds all of its tasks.
    """,
    "S13": """
        This one is about data that disappears. The source sends three FlowFiles a second to S13 Consumer, which
        only handles one a second. On its own that would be a growing backlog like S01, but `s13-expiring` has a
        FlowFile expiration of 30 seconds, and anything that has waited longer than that is thrown away. So the
        queue doesn't grow. It levels off at around 90 FlowFiles, far below anything that would look worrying,
        and two of every three FlowFiles quietly expire.

        No KPI alerts here, and nothing would alert on a queue that looks this healthy. The way to see the
        problem is to compare what the source sends out with what the consumer takes in. NiFi records expired
        FlowFiles in provenance but doesn't raise a bulletin.

        The fix needs two changes together. Remove the expiration, and give the consumer four tasks so it can
        keep up. Removing the expiration on its own turns the silent loss into an ordinary backlog growing by two
        a second, which at least is visible, but it isn't fixed.
    """,
    "S14": """
        S14 Validator is a DebugFlow that fails one record in five. Its success relationship goes on to S14
        Publish, but failure is auto-terminated, so the 20% that fail are simply dropped. There's no queue, no
        bulletin and nothing in a dead-letter queue. There is an S14 DLQ on the canvas, but it's disabled and
        nothing is connected to it.

        The only sign in the metrics is that the validator sends out fewer FlowFiles than it takes in, and those
        two numbers are what the KPIs chart. The fix is to route failure to S14 DLQ, so rejected records are kept
        somewhere a person can look at them.

        The tempting alternative is to loop failure back into the validator so the records get another go. That
        trades silent loss for S09's loop, which never empties.
    """,
    "S15": """
        S15 Batcher is a MergeContent that waits for 1,000 entries before it sends a batch, and it has no Max
        Bin Age. The source produces one FlowFile every five seconds. At that rate the first batch would leave
        after 5,000 seconds, about 83 minutes, and until then nothing comes out of the merge at all.

        The alert is on the merge's output rather than on a queue. If the batcher sends nothing for fifteen
        minutes, it fires. The queue in front of it is a poor signal here, since it only grows by twelve
        FlowFiles a minute.

        The fix is to give the bin an age limit. With Max Bin Age at 60 seconds (or 5 minutes, which we also
        accept), MergeContent sends whatever it has when the time runs out. Setting Minimum Number of Entries to
        1 would also get data moving, but it ends batching entirely, and the batch size is presumably what the
        flow's owner wanted.
    """,
    "S16": """
        Somebody disabled S16 Enricher during maintenance and never turned it back on. FlowFiles keep arriving at
        one a second and wait in `s16-to-enrich`, which crosses its threshold of 500 a little over eight minutes
        in.

        The fix is just to enable the processor. A disabled processor doesn't run at all, so changing its
        concurrency or run schedule has no effect, and that's the mistake this scenario is looking for.
    """,
    "S17": """
        S17 Partner Call is an InvokeHTTP whose HTTP URL is `#{s17.target.url}`, a reference to the pack's
        parameter context. The parameter is set to `http://10.255.255.1:8080/ingest`, an address nothing listens
        on. The real partner is S17 Partner Stub, a ListenHTTP in the same group on port 9876, standing in for
        the partner's service.

        A request arrives every five seconds and each one waits out a 30 second connect timeout before it fails
        over to S17 Failed Requests, so about five pile up in `s17-requests` every 30 seconds. Meanwhile the
        partner stub receives nothing, which raises a second alert after ten minutes.

        The fix is to set the parameter to `http://localhost:9876/ingest`. It's also possible to type the right
        URL straight into the InvokeHTTP property, and the flow would work, but that replaces the parameter
        reference and the flow would no longer follow its context. Shortening the timeout or adding tasks only
        makes the failures happen faster.
    """,
    "S18": """
        S18 Fan Out is a DuplicateFlowFile whose Number of Copies comes from the parameter `s18.copies`. In
        production it should be 1, but it's still set to 50 from a load test. The source sends one record every
        ten seconds, so the sink receives about five a second instead of one every ten seconds, and it only
        handles one a second. `s18-copies` passes 1,000 a few minutes in.

        The fix is to set the parameter back to 1. Speeding up S18 Sink would clear the queue too, but every
        record downstream would then be processed 50 times.
    """,
    "S19": """
        Nothing is wrong with this flow, on purpose. One FlowFile a second passes through two stages, Transform
        and Publish, each with two tasks on a 500 ms schedule, so each can handle four a second and both queues
        stay close to empty. S19 is here to check that the agent leaves a healthy flow alone.
    """,
    "S20": """
        This flow is CPU-bound by design. Every two seconds the source creates three 10 MB files of random
        bytes, about 15 MB a second. S20 Compress gzips them at level 9, which on random data manages roughly
        8 MB a second per core, and S20 Hash then takes a SHA-256 of each one. Together that's about two cores
        of work, and an EXTRA_SMALL node has two. CPU sits above 85% and the queue in front of the compressor
        starts to build.

        One detail matters for the fix. The source runs on the primary node only, so adding nodes doesn't add
        load, and the first connection, `s20-to-compress`, uses round robin load balancing, which spreads the
        files over however many nodes there are. That's why scaling out works here. The design fix is SMALL
        with two nodes, and EXTRA_SMALL with two nodes is accepted too. Dropping the compression level to 1 is
        also accepted, since that removes most of the CPU cost.

        What doesn't work is giving the compressor more tasks. The node is already saturated, and more threads
        just compete for the same two cores.
    """,
    "S21": """
        This flow barely does anything. One small FlowFile every ten seconds goes through two pass-through
        stages, and it's deployed on a MEDIUM two-node cluster, far more than it needs. The alert is CPU staying
        under 5% for half an hour.

        The fix is to scale down to EXTRA_SMALL with one node, and SMALL with one node is accepted too. A
        downsize only counts as a success if the flow still keeps up afterwards, so the input queue has an alert
        of its own at 100 FlowFiles.
    """,
    "S22": """
        A ten-processor order pipeline. Orders arrive at 100 a second, have their region extracted with an
        EvaluateJsonPath, get tagged, have their currency normalised and their card numbers masked by a
        ReplaceText, and are then batched by S22 Batch Orders, a MergeContent, before being compressed, named and
        published.

        The merge is where it goes wrong. It waits for at least 20,000 orders per batch, but the connection in
        front of it, `s22-to-batch`, back-pressures at the default 10,000, and the FlowFiles sitting in the
        merge's bin still count against that connection. The bin can never reach 20,000, so the merge only sends
        a batch when it hits its 30 minute Max Bin Age. In between, back pressure works its way upstream one
        queue at a time, about 100 seconds per queue, and roughly 500 seconds after each flush the ingest queue
        is full. That's where the alert is, five hops away from the cause.

        The design fix is to bring Minimum Number of Entries down to 5,000, so a bin can fill well within the
        connection's limit. Raising the back pressure threshold on `s22-to-batch` to 30,000, or setting a one
        minute bin age, also clears it. Anything aimed at the alert itself doesn't work. S22 Extract Order isn't
        slow, it's back-pressured, so more tasks there do nothing, and a higher threshold on ingest only lets a
        bigger backlog build up.
    """,
    "S23": """
        Shipments come in at 30 a second and go through carrier extraction, address normalisation and pricing
        before reaching S23 Carrier Throttle, a ControlRate set to 600 FlowFiles a minute. That's ten a second, a
        third of what arrives. Everything after the throttle runs at ten a second, so confirmations at the end of
        the pipeline sit well below the 20 a second the alert expects.

        The backlog builds in front of the throttle. Its input connection holds 2,000, which fills in about 100
        seconds at the surplus of 20 a second. After that each queue upstream fills in turn, roughly 500 seconds
        apiece at the default 10,000, and the ingest queue reaches its own alert at 8,000 about 25 minutes in.
        The low confirmation rate fires well before that.

        The fix is to raise Maximum Rate to 3,000 a minute, or to keep 600 and shorten Time Duration to ten
        seconds, which comes to the same thing. Two mistakes are tempting. ControlRate's limit applies per node,
        not per task, so giving the throttle more concurrent tasks lets no more through. And the stages after the
        throttle are starved rather than slow, so speeding them up changes nothing.
    """,
    "S24": """
        Click batches arrive two a second, each with 50 clicks as lines of CSV. S24 Split Clicks, a SplitText,
        breaks them into one FlowFile per click, so 100 clicks a second flow on through parsing, bot filtering
        and geo enrichment to S24 Sessionize. The sessionizer has one task on a 20 ms schedule, which is 50 a
        second, half of what arrives.

        The surplus of 50 a second fills the sessionizer's input in about 200 seconds, then the queues before it
        fill one by one, about 200 seconds each. The alert is on `s24-splits`, the queue straight after the
        split, which is full about 15 minutes in and two hops away from the actual bottleneck.

        Four concurrent tasks on the sessionizer, or a 0 sec schedule, fixes it. Adding tasks to S24 Parse Click,
        where the alert points, does nothing because that stage is back-pressured rather than slow. Splitting
        into fewer, larger FlowFiles would relieve the pressure too, but every stage after the split expects one
        click per FlowFile, so it changes what the flow means.
    """,
    "S25": """
        Page views arrive at 20 a second. They're parsed, filtered for bots, have their IP masked and get
        geo-enriched, then wait in `s25-session-buffer` for S25 Sessionize. The sessionizer has one task every
        100 ms, so it does ten a second, half of the input. In S24 a gap like that makes a growing queue. Here
        the buffer has a one minute FlowFile expiration, so it levels off at around 1,300 FlowFiles and ten page
        views a second expire without a trace.

        The buffer's alert at 5,000 is never reached. What gives it away is that S25 Score Engagement, the stage
        after the sessionizer, takes in half as many FlowFiles as the source sends out, and Score Engagement's
        input is what decides the outcome.

        The fix is to give the sessionizer four tasks, three, or a schedule of 40 ms or shorter, so it keeps
        up and nothing waits long enough to expire. Removing the expiration on its own isn't enough. It just
        turns the silent loss into a backlog growing by ten a second.
    """,
    "S26": """
        Sensor readings arrive at 50 a second and are parsed, converted and tagged with their site before S26
        Enrich Device, an InvokeHTTP, posts each one to a device lookup service at `localhost:9999`. Nothing
        listens on that port, so every call is refused immediately. The processor's Failure, Retry and No Retry
        relationships all go to `s26-retry-loop`, which leads straight back into the same processor.

        Since nothing ever succeeds, everything that arrives ends up in the loop, and the loop is full after
        about 200 seconds. From then on the lookup is back-pressured by its own output and stops taking new
        readings. The queues upstream fill one after another, about 200 seconds each, and ingest is full roughly
        17 minutes in, which is where the alert fires.

        The fix is to route those three relationships to S26 Device DLQ. Nothing reaches the store until the
        lookup service comes back, but the readings are kept in the DLQ and the rest of the flow is no longer
        jammed. More tasks on the lookup don't help, because it's blocked by its own full loop, and a higher
        threshold on the loop only buys a little time against an endpoint that refuses everything.
    """,
    "S27": """
        Meter readings arrive at ten a second and pass through five stages before S27 Schema Check, a DebugFlow
        that rejects three in every 20, or 15%. Its failure relationship is auto-terminated, so rejected readings
        are dropped on the spot. An S27 Reject DLQ is on the canvas, disabled, with nothing routed to it. The
        store at the end receives 8.5 readings a second instead of ten.

        No queue grows anywhere, and the stages between the check and the store are perfectly healthy. The loss
        only shows when you compare what the source sends out with what the store takes in, three stages after
        the cause. The KPI that decides the outcome is FlowFiles into the reject DLQ, which should go from zero
        to about 1.5 a second once the fix is in.

        The fix is to route failure to S27 Reject DLQ. More tasks on the schema check wouldn't change anything,
        since it rejects the same share however fast it runs.
    """,
    "S28": """
        Payments arrive at twelve a second and, after parsing and fraud scoring, S28 Balance Lanes, a
        DistributeLoad in round robin mode, spreads them over three lanes. Each lane has an authorize step and a
        tokenize step, both running one task every 100 ms, so each lane can do ten a second. The lanes join
        again at S28 Capture Payment before the ledger write. Lane 3's tokenizer is disabled.

        With lane 3 stopped, its authorizer's output fills at four a second and reaches its limit of 2,000 in
        about 500 seconds. Then the authorizer itself is blocked, and the balancer's queue for lane 3 fills up
        over another 500 seconds, so the alert on `s28-to-lane-3` comes about 17 minutes in. After that
        DistributeLoad skips the full lane, and lanes 1 and 2 carry six a second each, which they handle easily.
        The ledger rate looks completely normal, which is why this one is easy to miss, but several thousand
        payments are stuck in lane 3.

        The fix is to enable S28 Tokenize Lane 3. The alert is on the balancer's queue, and the processor right
        after it, S28 Authorize Lane 3, is the obvious suspect, but it's back-pressured rather than slow. The
        stopped processor is one stage further on.
    """,
    "S29": """
        Bank statements of 200 KB each arrive at 100 a second, which is 20 MB a second, and go through
        validation, account tagging and rendering before S29 Compress Statement gzips them at level 9. At that
        level gzip manages about 8 MB a second on one core, or 40 statements a second, so 60 a second pile up.
        Each queue upstream hits its 1 GB size limit, about 5,000 statements, in under 90 seconds, and the
        backlog reaches ingest within six minutes.

        At level 1 gzip does about 60 MB a second, or 300 statements, which is plenty, and that's the design
        fix. Level 3 is accepted too.

        A second compress task is tempting, but it only doubles the compressor to 16 MB a second, still short of
        20, and it takes the node's other core. The stages before the compressor are back-pressured, not slow,
        so they don't need anything.
    """,
    "S30": """
        A warehouse order flow with 20 orders a second coming in and two stages that are too slow. S30 Pick
        Items runs one task every 125 ms, which is eight a second, and S30 Label Parcel, three stages later, runs
        one task every 200 ms, five a second. The queue in front of picking fills at twelve a second and reaches
        its limit of 2,000 in under three minutes. The labeller only receives eight a second from picking, so
        its queue fills more slowly, at three a second, and is full in about eleven minutes.

        Fixing one stage only moves the problem. If picking runs at full speed, the labeller gets all 20 orders a
        second and falls further behind. If labelling is fixed first, picking is still as slow as it was. Both
        stages have to speed up before the KPIs clear, and the replay checks that the agent gets there
        over two rounds. Three tasks on picking and five on labelling, or 40 ms on either, are accepted too.

        The stages in between are back-pressured, not slow, and a bigger queue in front of picking only holds a
        bigger backlog.
    """,
    "S31": """
        Shipping manifests arrive at ten a second. The first processor they reach, S31 Validate Manifest, is
        disabled, so its queue fills in about 200 seconds and the feed stops. Nothing gets through to the rest
        of the flow, and that's what raises the alert.

        Hidden behind it is a second fault. Further down, S31 Check Customs, a DebugFlow, rejects three
        manifests in 20 and auto-terminates them. While the validator is disabled nothing reaches the customs
        check, so this loss doesn't exist yet. Once the validator is enabled the manifests flow again, and 15% of
        them disappear at the customs check, just as in S27. Its Customs DLQ is there but disabled and not
        connected.

        So the scenario needs two fixes, enabling the validator and then routing the customs check's failure to
        S31 Customs DLQ. More tasks, or a bigger queue in front of the disabled processor, move nothing, and more
        tasks on the customs check reject just as many.
    """,
    "S32": """
        Invoices arrive at ten a second. After parsing, tax and currency conversion they reach S32 Billing
        Throttle, a ControlRate whose Maximum Rate is the parameter `s32.max.rate`. It's set to 5 a second, where
        production uses 50. After numbering, S32 Post Invoice runs one task every 500 ms, two a second. So there
        are two bottlenecks of different kinds. The throttle's queue fills at five a second and reaches 2,000 in
        under seven minutes, and the poster's queue fills at three a second and is full in about eleven.

        One proposal can't fix both, because the throttle's rate is a parameter and the poster's schedule is a
        flow edit, and the agent proposes those separately. Raising the rate alone sends all ten a second at the
        poster, and speeding up the poster alone leaves the throttle where it was.

        Editing Maximum Rate in the flow would replace the parameter reference, and more tasks on a ControlRate
        let no more through, since its limit is per node.
    """,
    "S33": """
        Two usage records arrive each second. The first stage, S33 Meter Gate, is disabled, so its queue fills
        to 2,000 in a little under 17 minutes and the feed stops there. That's the first alert.

        Behind the gate is a parameter left at its load-test value. S33 Fan Out Usage copies each record
        `#{s33.copies}` times, and `s33.copies` is still 50 when production expects 1. While the gate is
        disabled that doesn't matter, because nothing reaches the fan-out. Once it's enabled, 100 records a
        second land on S33 Rate Usage, which handles 20 a second, and its queue fills in well under a minute.

        It takes both fixes, enabling the gate and setting the parameter back to 1. Speeding up the rater is the
        wrong answer, since it would process 50 times as many records as it should.
    """,
    "S34": """
        This group holds two separate flows. One is an archive flow like S20. Three 10 MB files of random bytes
        arrive every two seconds, are gzipped at level 9 and then hashed with SHA-256, which is about two cores
        of work, enough to saturate an EXTRA_SMALL node and keep a SMALL one near 80%. The other is a notice
        flow, where requests come in at four a second and S34 Send Notice runs once a second, so its queue grows
        by three a second and is full at 2,000 in about eleven minutes, whatever the CPU is doing.

        The two faults need separate fixes. A bigger node takes care of the CPU, and the sender needs a faster
        schedule or more tasks, which is a flow edit that no amount of hardware replaces.

        Two nodes don't help, unlike in S20, because nothing load-balances the archives across them. More
        compress tasks on a saturated node just add contention.
    """,
    "S35": """
        Web events trickle in at two a second all day. For the first 20 minutes of every second hour, a CRON
        driven campaign generator (`* 0-19 */2 * * ?`) adds 20 a second on top. S35 Load Events, one task every
        100 ms, handles ten a second, so during a burst its queue grows by twelve a second. It crosses the alert
        at 8,000 about eleven minutes into the burst and is full at fourteen.

        When the burst ends the loader catches up at eight a second. The queue falls back under the threshold
        about four minutes later and is empty well before the next burst. Anyone who looks during the quiet part
        of the cycle sees a perfectly healthy flow.

        The fix is four tasks on the loader, or a 0 sec schedule, which keeps up even at the peak. Two things
        make this one tricky to judge. An empty queue an hour after a change proves nothing, because it would
        have been empty anyway, so the verdict has to wait for the next burst. And raising the threshold just
        holds a bigger backlog for longer.
    """,
    "S36": """
        Store orders arrive at five a second around the clock. For the first ten minutes of every hour a partner
        export adds ten a second through its own branch, where S36 Validate Partner Order fails one record in
        four. Its failure relationship is auto-terminated, so while the export runs, 2.5 records a second vanish.
        For the other 50 minutes of the hour nothing is lost at all. As in S27, the reject DLQ is on the canvas,
        disabled, with nothing routed to it.

        This is the silent drop from S14 and S27 with a schedule on top. Compare the validator's input and
        output at the wrong time of the hour and they agree. The fix is to route failure to S36 Reject DLQ, and
        what decides it is the validator's output and the DLQ's input, read while the export is running. More
        tasks on the validator don't help, since it rejects the same share at any speed.
    """,
    "S37": """
        The trade feed sends 41 trades every two seconds, 20.5 a second, through parsing, validation, account
        enrichment and currency conversion to S37 Write Ledger, which runs one task every 50 ms, so 20 a second.
        The writer is busy all the time and almost keeps up. Half a trade a second, 30 a minute, gets left
        behind.

        Nothing else looks wrong. The writer's output rate is normal, no processor is stalled, and the queue in
        front of it grows so slowly that it takes nearly three hours to cross 5,000. A trend on that queue is the
        only early warning, and the scenario is here to see whether the agent catches it before the breach.

        The fix is a second task on the writer, or a 0 sec schedule. Raising the threshold only moves the breach
        a few hours further out, because the queue never stops growing.
    """,
    "S38": """
        Claims arrive at ten a second. S38 Validate Claim, a DebugFlow, passes 49 in 50 and sends the rest to
        failure, which goes to S38 Retry Claim. That retry step is disabled, so the one claim in 50 that fails,
        0.2 a second, piles up in `s38-retry`.

        For a long time this doesn't affect anything. The retry queue crosses 1,500 after about two hours and is
        full at 2,000 some 40 minutes later. At that point the validator has nowhere to send its failures, NiFi
        stops scheduling it, and the whole flow backs up behind it.

        The fix is to enable S38 Retry Claim. The validator itself is healthy, and a higher threshold on the
        retry queue only postpones the stall, since nothing ever drains it while the retry step is disabled.
    """,
    "S39": """
        From 02:00 to 02:39 UTC every night, a CRON driven export (`*/2 0-39 2 * * ?`) produces three 10 MB files
        every two seconds, which are gzipped at level 9 and hashed, the same two cores of work as in S20. That
        pins an EXTRA_SMALL node for 40 minutes. The rest of the day only a small audit trickle runs, two events
        a second, and the node is nearly idle.

        A SMALL node would sit around 70% during the export, which is why that's the fix. The hard part is
        timing. A fix judged at noon shows nothing either way, so the verdict has to come from the next night's
        run, and the scenario's detection window is a full day.

        As in S34, more compress tasks only add contention, and two nodes don't help because nothing
        load-balances the export across them.
    """,
    "S40": """
        Nothing is broken here. Web events trickle in at two a second, and twice an hour, in minutes 0 to 2 and
        30 to 32, a flash sale generator adds 30 a second. S40 Load Events handles ten a second, so during the
        three minute burst its queue grows by 22 a second to about 4,000, against a threshold of 5,000. Then it
        drains at eight a second and is empty about eight minutes later, long before the next sale.

        A queue climbing that steeply will set off a trend warning, and this scenario checks that the agent
        waits to see it level off instead of acting on the slope. More tasks on the loader would only be used for
        a few minutes an hour.
    """,
    "S41": """
        This is the healthy twin of S36. Store orders arrive at five a second, and for ten minutes each hour a
        partner export adds ten a second. Its validator fails three records in ten, but here failure is connected
        to S41 Reject DLQ, as designed. While the export runs, three records a second go to the DLQ, and every
        FlowFile that enters the validator leaves by one of its relationships.

        The DLQ filling up every hour can look like a problem, and the sink takes in less than the sources send
        while the export runs. Both are expected. The difference is sitting in the DLQ, accounted for.
    """,
    "S42": """
        Meters report at one a second around the clock. During business hours, 08:00 to 19:59 UTC, a second CRON
        generator adds six a second, and through the lunch hour a third adds five more, so the load runs from one
        a second at night to twelve at lunch. S42 Load Readings handles 20 a second and its queue stays near
        empty all day.

        What could fool an agent here is the shape of the rate. Throughput drops sevenfold at 20:00 UTC every
        evening, which looks as if something upstream broke but is just the end of the working day, and the
        lunch peak is the busiest hour, which the loader handles easily.
    """,
    "S43": """
        The nightly bank file arrives between 01:00 and 01:59 UTC as 20 statements every five seconds, four a
        second. It goes through decryption, parsing, a totals check, payment matching and reconciliation before
        S43 Post Ledger, which can do ten a second. For the other 23 hours nothing flows at all.

        A rate of zero for most of the day is the schedule, not a stalled feed. The agent shouldn't treat the
        silence as a fault, and starting or speeding up the source would only post statements that don't exist
        yet.
    """,
    "S44": """
        The biggest flow in the set, with 30 processors, and nothing wrong with it. Sensor batches of 20 CSV
        readings arrive two a second and are split into single readings, 40 a second, with the original batch
        archived. A validator sends one reading in 20 to a connected DLQ. A ControlRate limits the stream to
        6,000 a minute, which is 100 a second and far above the load, and a DistributeLoad spreads it over three
        regional lanes of about 13 a second each, where each scoring stage can do 20. The lanes join and pass
        through windowing, anomaly detection and formatting, and are merged into batches of 500 to 1,000, about
        one every 13 seconds, which are compressed, checksummed and stored. The merge's originals go to an audit
        sink. A small heartbeat flow runs alongside at one a second.

        It uses most of the processor types from the other scenarios, so it tests whether their warning signs
        get mistaken for faults in a flow where they're harmless. The merge sends out one FlowFile for every 500
        or more it takes in, which is batching, not loss. The rate limit and the lanes are comfortably within
        capacity.
    """,
    "S45": """
        Devices upload 14 MB a second of random data all day, and 20 MB a second during the lunch hour, 12:00 to
        12:59 UTC. S45 Compress Upload gzips it at level 6, about 25 MB a second per core, with two tasks, and
        the uploads are then hashed with SHA-256. Together with the rest of the pack that's about 1.1 of the
        EXTRA_SMALL node's two cores most of the day and 1.5 at lunch, so CPU runs around 60% and rises to about
        75% at lunch, with no backlog and headroom to spare.

        High CPU on its own isn't a fault. A bigger node would cost more and change nothing, and a lower
        compression level would cut CPU at the price of bigger archives that nobody asked for. The alert is at
        85% for five minutes, and it shouldn't fire.
    """,
    "S46": """
        S01's slow consumer, renamed: S46 Source makes three FlowFiles a second and S46 Consumer, with one task on
        a one second schedule, moves one, so `s46-backlog` grows by two a second and passes 500 about four
        minutes in. Around it are 98 healthy processors in 17 small flows: chains of pass-through stages, lanes
        that split and join, validations that send one FlowFile in 20 to a DLQ, and rate limits far above their
        load. Each runs at one to three FlowFiles a second and none of them has a backlog.

        The fix is S01's: four tasks on the consumer, or a 300 ms schedule. The filler adds tempting distractions,
        the DLQs that receive a steady trickle above all, but nothing in it needs changing.
    """,
    "S47": """
        S03's pipeline bottleneck, renamed, in a group of 100. Two FlowFiles a second go through S47 Enrich and
        S47 Publish, which do four a second each, and S47 Transform between them, which does one, so only
        `s47-to-transform` grows. The other 96 processors are healthy filler, as in S46.

        Three or more tasks on Transform, or a 250 ms schedule, fixes it, and adding tasks to Enrich or Publish
        changes nothing. In a group this size there are many more stages the agent could pick, so this is the
        twin that tests most directly whether it still finds the one with the backlog in front of it.
    """,
    "S48": """
        S24's fan-out into one task, renamed, in a group of 100. Click batches split into 100 clicks a second for
        a sessionizer that does 50, and the backlog fills the queues back to `s48-splits`, two hops before the
        cause, where the alert is. The other 90 processors, in 16 flows, are healthy filler, as in S46.

        The fix is S24's: three tasks on S48 Sessionize, or a 0 sec schedule. Adding tasks to S48 Parse Click,
        where the alert points, does nothing. With a cascade, the agent has to follow the backlog through the
        group, so this one shows whether a larger flow makes it lose the trail.
    """,
    "S49": """
        S22's merge deadlock, renamed, in a group of 250. A hundred orders a second go to S49 Batch Orders, which
        waits for 20,000 while its input connection holds only 10,000, so it empties only at its 30 minute bin
        age and the back pressure reaches `s49-ingest`, five hops upstream, where the alert is.

        Beside it is the decoy, S49 Exports: ten FlowFiles a second into S49 Exports Batch, a merge of 5,000 to
        6,000. Its bin fills in 500 s and goes, so `s49-exports-2` climbs to 5,000 and drops, again and again.
        On a chart it looks like the start of the same backlog, but it never reaches its threshold. The other 236
        processors are healthy filler, as in S46.

        The fix is S22's: a minimum of 5,000 entries on S49 Batch Orders, or a 30,000 threshold on
        `s49-to-batch`, or a one minute bin age. Lowering the decoy's minimum instead is a wrong fix, and the tests
        check that it leaves the deadlock in place.
    """,
    "S50": """
        S09's failure self-loop, renamed, in a group of 250. One record in ten is malformed, fails S50 Validator
        and goes straight back into it, failing again on every run, so `s50-failure-loop` grows by one record
        every ten seconds and passes 100.

        The decoy, S50 Exports Send, also loops its failures back into itself, but its failures are transient:
        one attempt in 20 fails, a retry gets through, and it runs on a 0 sec schedule, so it has room for the
        retries and `s50-exports-2` stays near empty. The other
        243 processors are healthy filler, as in S46.

        The fix is S09's: route S50 Validator's failures to S50 DLQ. Routing the decoy's failures there instead
        is a wrong fix, and the tests check that it leaves the self-loop growing.
    """,
    "S51": """
        S02's slow run schedule, renamed, in a group of 12 among 24 healthy siblings: S51 Drain runs every 10
        seconds with four tasks, 0.4 FlowFiles a second against one coming in. The fix is S02's, a 1 sec
        schedule, and more tasks still fall short.
    """,
    "S52": """
        S14's auto-terminated failures, renamed, in a group of 14 among its siblings: one record in five fails
        S52 Validator and vanishes. The fix is S14's, routing the failures to S52 DLQ. Nothing queues, so the
        only sign is the gap between what goes into the validator and what comes out.
    """,
    "S53": """
        S28's stopped lane, renamed, in a group of 24 among its siblings: S53 Tokenize Lane 3 is disabled, and
        the balancer's queue for lane 3 fills behind it while lanes 1 and 2 carry the rest. The fix is S28's,
        enabling the tokenizer, and the traps are too: more tasks on the authorizer, or a higher threshold, change
        nothing.
    """,
    "S98": """
        S01's slow consumer, renamed, one group down: S98 Source and S98 Consumer are in S98 Region, inside the
        scenario's group. The source makes three FlowFiles a second and the consumer, with one task on a one second
        schedule, moves one, so `s98-backlog` grows by two a second and passes 500 about four minutes in. A healthy
        crossing goes down into S98 Region through S98 In Region and back up through S98 Out Region, beside the
        fault but never through it.

        The fix is S01's: four tasks on S98 Consumer, or a 300 ms schedule, made where it is, in S98 Region. The
        ports carry FlowFiles and have nothing to change.
    """,
    "S99": """
        S09's failure self-loop, renamed, two groups down in S99 Region / S99 Site. One record in ten is
        malformed, fails S99 Validator and goes straight back into it through `s99-failure-loop`, failing again
        on every run, so the loop passes 100. S99 DLQ is beside the validator in S99 Site, disabled and connected to nothing.

        The fix is S09's: route the validator's failures to S99 DLQ. The filler has DLQs of its own higher up, and
        a route to one of them is refused, because a connection can only join processors in one group.
    """,
    "S100": """
        S14's auto-terminated failures, renamed, two groups down in S100 Region / S100 Site: one record in five
        fails S100 Validator and vanishes. Nothing queues, so the only sign is the gap between what goes into the
        validator and what comes out, three levels below the group the incident names.

        The fix is S14's: route the failures to S100 DLQ, which is beside the validator in S100 Site.
    """,
    "S103": """
        Call records come from the switches at five a second and from roaming partners at one more, and join at
        S103 Decode CDR. Every stage runs on a 0 sec schedule and keeps up except S103 Rate Call, which has one
        task every 250 ms and does four a second. `s103-to-rate` grows by two a second, 120 a minute, and passes
        3,000 about 25 minutes in. The queues before it stay empty, so the alert is one stage from the cause.

        The fix is three tasks on the rater, twelve a second, or a 0 sec schedule. Two tasks would only just
        match the input and take over half an hour to drain a full queue. More tasks on S103 Lookup Subscriber,
        which feeds the queue, or a higher threshold on `s103-to-rate`, change nothing.
    """,
    "S104": """
        Parcel scans arrive three a second and pass through parsing, barcode validation and depot lookup to S104
        Geofence Check, which has two tasks but runs only every five seconds: 0.4 a second. `s104-to-geofence`
        grows by 2.6 a second and passes 2,000 about 13 minutes in. A small heartbeat flow runs beside it.

        The schedule is the fault, so the fix is a 0 sec schedule, or 100 ms. Adding tasks looks like the answer
        but isn't: even eight, the catalog's maximum, do only 1.6 a second on a five second schedule. More tasks
        on S104 Resolve Depot, which feeds the queue, do nothing.
    """,
    "S105": """
        Prescriptions arrive 41 every four seconds, 10.25 a second, and S105 Write Dispense Record, with two tasks
        every 200 ms, does ten. It is busy and nearly keeps up, so its output looks normal, but a quarter of a
        FlowFile a second stays behind, 15 a minute, and `s105-to-write` passes 2,500 nearly three hours in. Only
        the trend of the queue shows it early.

        The fix is a third task on the writer, or a 100 ms schedule. A higher threshold on `s105-to-write` moves
        the breach out by hours without stopping the growth, and more tasks on S105 Price Claim do nothing.
    """,
    "S106": """
        Nothing is wrong here. Port manifests arrive two a second all day, and for minutes 15 to 19 of every hour
        a backfill adds 15 a second more. S106 Load Manifest, with one task every 100 ms, does ten, so for those
        five minutes `s106-to-load` grows by seven a second, to about 2,000 against an alert at 3,000, and then
        drains at eight a second in under five minutes.

        A queue that climbs every hour looks like a growing backlog, but each one is gone well before the next.
        More tasks on the loader would sit idle for 50 minutes of the hour, and the alert shouldn't fire.
    """,
    "S107": """
        Identity checks arrive two a second and reach S107 Verify Identity, which calls a service that fails two
        requests in ten. The failures are transient and retried in place, but with YIELD_PROCESSOR, which backs
        off the whole processor rather than the one FlowFile: five seconds, doubling with each retry up to five
        minutes, and both tasks wait it out together. The verifier sits idle with work in front of it, and
        `s107-to-verify` passes 500 within ten minutes.

        The fix is PENALIZE_FLOWFILE, so only the failed FlowFile waits and the rest go on, optionally with a
        shorter maximum backoff of 30 seconds. More tasks on the verifier change nothing while it is yielded, and
        more tasks on S107 Resolve Country Rules, which feeds the queue, do nothing either.
    """,
    "S108": """
        Onboarding applications arrive two a second, and one in 20 is malformed: its applicant has no value, so
        it isn't JSON. S108 Parse KYC Document fails it and sends it straight back to itself through
        `s108-failure-loop`, where it fails again on every run, for good. The parser keeps up with the good
        applications five times over, so the loop just grows, by one every ten seconds, and passes 100 about 17
        minutes in.

        The fix is to route the parser's failures to S108 KYC Review DLQ, which is in the group, disabled and
        connected to nothing. That is a medium-risk change, so it always goes to a person. More tasks or a
        shorter schedule on the parser only retry the same applications faster; none of them will ever parse.
    """,
    "S109": """
        Bid requests arrive three a second all day, and for minutes 0 to 14 of every third hour an auction burst
        adds 15 a second more. S109 Record Impression, with two tasks every 250 ms, does eight, so during a burst
        `s109-to-record` grows by ten a second, passes 6,000 ten minutes in and reaches about 9,000. After it, the
        queue drains at five a second and is back under 6,000 ten minutes later, until the next burst.

        The fix is six tasks on the recorder, or a 0 sec schedule. Because the queue drains between bursts, an
        empty queue an hour later says nothing, and the fix is judged on the next burst. A higher threshold on
        `s109-to-record` or more tasks on S109 Enrich Segment change nothing.
    """,
    "S110": """
        Nothing is wrong here. A door sensor alert arrives every two seconds and S110 Copy Per Subscriber copies
        it once per subscriber, 20 FlowFiles out for each one in, so ten a second flow on to addressing,
        rendering and dispatch, all on 0 sec schedules that keep up with ease.

        A processor that puts out twenty times what it takes in looks like a flood, but it is the fan-out doing
        its job, and lowering Number of Copies would drop messages people should get. The alert on
        `s110-to-dispatch` shouldn't fire.
    """,
}

# The controls of p23 to p25 are filler alone, so their walkthroughs are generated.
WALKTHROUGHS.update({s.id: scale.walkthrough(s) for pack in (p23_siblings.PACK, p24_healthy_500.PACK, p25_nested.PACK)
                     for s in pack.scenarios if s.kind == "control"})
