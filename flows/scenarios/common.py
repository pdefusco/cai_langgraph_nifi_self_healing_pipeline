"""
Small building blocks the scenarios share.

Rates in the comments are what the simulator computes and what a live
NiFi does with the same settings: a timer-driven processor runs each
concurrent task once per run schedule, so one that handles a FlowFile
per run moves at most `tasks / period` FlowFiles per second.
"""

from flows.builder.lib import ProcessGroup


def generator(pg: ProcessGroup, name: str, *, period: str = "1 sec", batch: int = 1,
              size: str = "1 KB", text: str | None = None, binary: bool = False,
              key: str = "gen", execution_node: str = "ALL") -> dict:
    """GenerateFlowFile: `batch` FlowFiles every `period`, on each node it runs on."""

    properties = {
        "Batch Size": str(batch),
        "Data Format": "Binary" if binary else "Text",
        "File Size": size,
        "Unique FlowFiles": "true" if binary else "false",
    }

    if text is not None:
        properties["generate-ff-custom-text"] = text

    return pg.processor(key, name, "GenerateFlowFile", properties, period=period,
                        execution_node=execution_node)


def stage(pg: ProcessGroup, key: str, name: str, *, period: str = "1 sec", tasks: int = 1,
          terminal: bool = False, scheduled_state: str = "ENABLED") -> dict:
    """
    A cheap pass-through stage (UpdateAttribute). With `terminal` it is a
    sink: success is auto-terminated, so FlowFiles leave the flow here.
    """

    return pg.processor(
        key, name, "UpdateAttribute", {"Store State": "Do not store state"},
        period=period, tasks=tasks, scheduled_state=scheduled_state,
        auto_terminate=("success",) if terminal else (),
    )


def sink(pg: ProcessGroup, key: str, name: str, *, period: str = "1 sec", tasks: int = 1,
         scheduled_state: str = "ENABLED") -> dict:
    return stage(pg, key, name, period=period, tasks=tasks, terminal=True,
                 scheduled_state=scheduled_state)


def dlq(pg: ProcessGroup, name: str, *, connected: bool = True, key: str = "dlq") -> dict:
    """
    A dead-letter sink: LogAttribute, so a dead-lettered FlowFile leaves a
    log line with its attributes and does not fill a queue forever.

    `ROUTE_RELATIONSHIP_TO_DLQ` only routes to a component like this, in
    the same group, with "DLQ" in its name. An unconnected DLQ is
    DISABLED, because NiFi marks an enabled processor with no upstream
    connection invalid; the action enables it when it connects it.
    """

    return pg.processor(
        key, name, "LogAttribute", {"Log Level": "warn"},
        period="0 sec", tasks=1, auto_terminate=("success",),
        scheduled_state="ENABLED" if connected else "DISABLED",
    )


def debug_flow(pg: ProcessGroup, key: str, name: str, *, successes: int, failures: int,
               **options) -> dict:
    """
    DebugFlow cycles deterministically: `successes` FlowFiles to success,
    then `failures` to failure, and repeats, so the failure fraction is
    failures / (successes + failures).
    """

    return pg.processor(key, name, "DebugFlow", {
        "FlowFile Success Iterations": str(successes),
        "FlowFile Failure Iterations": str(failures),
        "FlowFile Rollback Iterations": "0",
        "FlowFile Exception Iterations": "0",
    }, **options)


def invoke_http(pg: ProcessGroup, key: str, name: str, url: str, *,
                connect_timeout: str, read_timeout: str = "15 secs", **options) -> dict:
    return pg.processor(key, name, "InvokeHTTP", {
        "HTTP Method": "POST",
        "HTTP URL": url,
        "Socket Connect Timeout": connect_timeout,
        "Socket Read Timeout": read_timeout,
    }, **options)
