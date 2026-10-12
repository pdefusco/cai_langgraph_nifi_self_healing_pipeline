# Provisioning by hand: the raw `cdp` commands

Each script in this directory runs exactly these commands, through the same
`cdp` CLI (cdpcli 0.9.164). Run them yourself to see what a step does, or to
recover when a script stops partway through. Each script also has `--dry-run`,
which prints its commands with the real CRNs filled in.

```bash
export SVC=crn:cdp:df:...:service:...          # SELFHEAL_SERVICE_CRN
export ENV=crn:cdp:environments:...            # SELFHEAL_ENVIRONMENT_CRN
NIFI=2.6.0.4.12.0.2-1                          # SELFHEAL_NIFI_VERSION
```

**Which plane each command uses:**
- `cdp df ...` goes to the control plane, so it works from anywhere.
- `cdp dfworkload ...` goes to the environment's private workload endpoint, so
  it works only from inside the environment's VPC, for example a CAI session in
  the same environment. Every `dfworkload` command needs `--environment-crn $ENV`.

**Paging:** list commands pass `--max-items 500`. Without it the CLI fetches
pages of 20 and prints a plain-text paging warning before the JSON.

---

## 00 selftest

```bash
cdp df list-deployments --max-items 500           # control plane
cdp dfworkload list-nifi-versions --environment-crn $ENV   # workload plane; must list $NIFI
```

## 01 import flows

```bash
python flows/build_all.py --check      # writes flows/out/<pack>.json and checks it

cdp df list-flow-definitions --search-term nfh-p1-throughput --max-items 500
    # The search is a substring match. The scripts take only the exact name.
cdp df import-flow-definition --file flows/out/p1-throughput.json \
    --name nfh-p1-throughput --description "Self-healing test pack p1-throughput." \
    --comments "baseline: the faulty version every reset returns to"
    # The response has .crn (the flow) and .versions[0].crn (the baseline version).
```

`--canary` does this under a throwaway name, then reads the flow back:

```bash
cdp df get-flow-version --flow-version-crn $FV
    # prints the flow JSON itself (.flowContents at the top level): the CLI
    # base64-decodes the API's .flowDefinition before printing
cdp df delete-flow --flow-crn $CANARY_FLOW
```

## 02 create deployments

```bash
cdp df create-deployment --service-crn $SVC --flow-version-crn $FV \
    --deployment-name nfh-p1-throughput \
    --cluster-size '{"name":"EXTRA_SMALL"}' --static-node-count 1 \
    --cfm-nifi-version $NIFI --auto-start-flow
    # p5 (Listen* processor) also needs --ignore-deployment-inbound-configuration-checks
    # p7 is MEDIUM x2, oversized on purpose

cdp df describe-deployment --deployment-crn $D        # poll .deployment.status.state
cdp df list-flows-in-deployment --deployment-crn $D   # .deployedFlows[0].crn is $F
```

`create-deployment` runs on the client side. It calls `initiate-deployment`
first and then `dfworkload create-deployment`, so it needs the workload plane
too. Billing starts here.

## 03 discover KPI metadata

```bash
cdp dfworkload get-flow-configuration-metadata-in-deployment \
    --deployment-crn $D --deployed-flow-crn $F --environment-crn $ENV \
    > provision/out/kpi_metadata_p1-throughput.json
```

The response has two parts:
- `kpiMetaData.kpiScopes[].metricTypes[].id` lists the metric ids each scope
  offers.
- `kpiScopes[].contextGroups[].scopeComponents[]` lists the components. Each
  component's `id` is the semicolon-joined ancestry (`<pg-id>;<component-id>`)
  that a KPI's `componentId` must use.

## 04 configure KPIs

No command creates a single KPI. `update-flow-in-deployment --kpis` replaces
the whole list. The steps are:
1. Read the current list and its configuration version.
2. Merge ours into it, keeping everyone else's KPIs and reusing the ids of
   ours.
3. Send the whole list back.

```bash
cdp dfworkload get-flow-configuration-in-deployment \
    --deployment-crn $D --deployed-flow-crn $F --environment-crn $ENV
    # .deployedFlowConfiguration.configurationVersion -> $V, .kpis -> existing

cdp dfworkload update-flow-in-deployment \
    --deployment-crn $D --deployed-flow-crn $F --environment-crn $ENV \
    --configuration-version $V --kpis '[
      {"metricId": "connectionAmountQueued", "metricComponentType": "NIFI_CONNECTION",
       "componentId": "<pg-id>;<connection-id>",
       "alert": {"thresholdMoreThan": {"unitId": "count", "value": 500},
                 "frequencyTolerance": {"value": 1, "unit": {"id": "MINUTES"}}}},
      {"metricId": "cpuUtilization", "metricComponentType": "SYSTEM",
       "alert": {"thresholdMoreThan": {"unitId": "percent", "value": 85},
                 "frequencyTolerance": {"value": 5, "unit": {"id": "MINUTES"}}}}
    ]'
    # A stale $V fails with CONFLICT. Re-read and resend.
```

Then wait until `get-flow-configuration-in-deployment` reports
`kpisDirty: false`, and find the charts:

```bash
cdp df list-deployment-kpis --deployment-crn $D --metrics-time-period LAST_THIRTY_MINUTES
cdp df list-flow-kpis-in-deployment --deployment-crn $D --deployed-flow-crn $F \
    --metrics-time-period LAST_THIRTY_MINUTES
cdp df list-deployment-system-metrics --deployment-crn $D --metrics-time-period LAST_THIRTY_MINUTES
```

A chart has no KPI id or component id. The only thing that identifies it is
the triple (`componentType`, `componentName`, chart `name`), which is why
every component with a KPI has a unique name.

## 05 register fleet

This step only reads:

```bash
cdp df describe-deployment --deployment-crn $D          # name, service, flowCrn
cdp df list-flows-in-deployment --deployment-crn $D     # deployed-flow CRN
cdp df describe-flow-in-deployment --deployment-crn $D --deployed-flow-crn $F   # flowVersionCrn
cdp df list-deployment-active-alerts --deployment-crn $D
```

`--adopt` finds the flow and the deployment by exact name, using
`list-flow-definitions`, `list-flow-definition-versions --flow-crn` (the
baseline is the lowest version) and `list-deployments`.

## 06 reset pack

These are the same three writes the agent's actions use. Each one is sent only
if the pack has drifted from its baseline:

```bash
# 1. flow version back to the baseline
cdp df change-flow-version-in-deployment --service-crn $SVC \
    --deployment-crn $D --deployed-flow-crn $F --flow-version-crn $BASELINE_FV \
    --strategy ONLY_RESTART_AFFECTED_COMPONENTS
    # p5: also --ignore-deployment-inbound-configuration-checks
    # Never STOP_AND_EMPTY_QUEUES. Never --kpis (it would replace them all).

# 2. parameters back to the scenario values
cdp dfworkload update-flow-in-deployment \
    --deployment-crn $D --deployed-flow-crn $F --environment-crn $ENV \
    --configuration-version $V --parameter-groups '[{"name": "nfh-p5-parameters",
      "parameters": [{"name": "s17.target.url", "value": "http://10.255.255.1:8080/ingest"},
                     {"name": "s18.copies", "value": "50"}]}]'

# 3. sizing back to the pack's
cdp dfworkload get-deployment-configuration --deployment-crn $D --environment-crn $ENV
    # .deploymentConfiguration.configurationVersion -> $DV
cdp dfworkload update-deployment --deployment-crn $D --environment-crn $ENV \
    --configuration-version $DV --cluster-size '{"name":"EXTRA_SMALL"}' --static-node-count 1
```

Wait for the deployment and the deployed flow to settle (`describe-deployment`
and `describe-flow-in-deployment` are no longer `UPDATING`) before the next
write.

The agent's flow edits go through the same version swap, with a new catalog
version in between:

```bash
cdp df get-flow-version --flow-version-crn $CURRENT_FV > current.json   # the flow JSON itself
# edit the JSON
cdp df import-flow-definition-version --flow-crn $FLOW --file edited.json --comments "..."
cdp df change-flow-version-in-deployment ... --flow-version-crn $NEW_FV \
    --strategy ONLY_RESTART_AFFECTED_COMPONENTS
```

Catalog versions cannot be deleted one at a time. Every fix stays in the
flow's history until the flow itself is deleted.

## teardown

Do these in this order. A flow can't be deleted while a deployment still
references it.

```bash
cdp dfworkload terminate-deployment --deployment-crn $D --environment-crn $ENV
cdp df describe-deployment --deployment-crn $D     # until it fails / is no longer listed
cdp df delete-flow --flow-crn $FLOW                # skipped with --keep-flow
```

Before running these, confirm that `$D` and `$FLOW` are yours. The script
takes them from the fleet file and checks the `nfh-` prefix, the service and
the denylist. A name search can match someone else's deployment.
