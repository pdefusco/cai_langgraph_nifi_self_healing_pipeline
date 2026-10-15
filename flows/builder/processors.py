"""
The processors the scenarios use, with what the builder needs to know.

`properties` maps each property key the builder may set to whether that
key has been confirmed against a live CDF NiFi 2.6 (SIB verified the
GenerateFlowFile and UpdateAttribute keys). The rest are the NiFi
descriptor names as documented; NiFi 2.x also runs each processor's
`migrateProperties` on import, which renames the 1.x spellings, so a
wrong guess here shows up as a dynamic property on the canary import
rather than a silent default. `build_all.py --check` lists every
unverified key; provision/01 --canary confirms them.

`editable` is the allow-list for SET_PROCESSOR_PROPERTY: the agent may
change these keys and no others.

Only standard NARs are used, so nothing needs a custom NAR upload.
"""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Spec:
    name: str
    type: str
    artifact: str
    relationships: tuple
    properties: dict
    dynamic: bool = False
    input_required: bool = True
    verified: bool = False
    editable: tuple = field(default_factory=tuple)


STANDARD = "nifi-standard-nar"


def _standard(name: str) -> str:
    return f"org.apache.nifi.processors.standard.{name}"


PROCESSORS = {
    "GenerateFlowFile": Spec(
        name="GenerateFlowFile",
        type=_standard("GenerateFlowFile"),
        artifact=STANDARD,
        relationships=("success",),
        properties={
            "Batch Size": True,
            "File Size": True,
            "Unique FlowFiles": True,
            "Data Format": True,
            "character-set": True,
            "generate-ff-custom-text": False,
        },
        dynamic=True,
        input_required=False,
        verified=True,
        editable=("Batch Size",),
    ),
    "UpdateAttribute": Spec(
        name="UpdateAttribute",
        type="org.apache.nifi.processors.attributes.UpdateAttribute",
        artifact="nifi-update-attribute-nar",
        relationships=("success",),
        properties={"Store State": True, "canonical-value-lookup-cache-size": True},
        dynamic=True,
        verified=True,
    ),
    "LogAttribute": Spec(
        name="LogAttribute",
        type=_standard("LogAttribute"),
        artifact=STANDARD,
        relationships=("success",),
        properties={"Log Level": False},
    ),
    "DebugFlow": Spec(
        name="DebugFlow",
        type=_standard("DebugFlow"),
        artifact=STANDARD,
        relationships=("success", "failure"),
        properties={
            "FlowFile Success Iterations": False,
            "FlowFile Failure Iterations": False,
            "FlowFile Rollback Iterations": False,
            "FlowFile Exception Iterations": False,
        },
        input_required=False,
    ),
    "InvokeHTTP": Spec(
        name="InvokeHTTP",
        type=_standard("InvokeHTTP"),
        artifact=STANDARD,
        relationships=("Original", "Response", "Retry", "No Retry", "Failure"),
        # The connect timeout's key is "Connection Timeout" (the docs title it
        # "Socket Connect Timeout"). InvokeHTTP sends dynamic properties as
        # request headers, so a wrong key fails every request on its space.
        properties={
            "HTTP Method": False,
            "HTTP URL": True,
            "Connection Timeout": False,
            "Socket Read Timeout": False,
        },
        dynamic=True,
        input_required=False,
        editable=("Connection Timeout", "Socket Read Timeout"),
    ),
    "ListenHTTP": Spec(
        name="ListenHTTP",
        type=_standard("ListenHTTP"),
        artifact=STANDARD,
        relationships=("success",),
        properties={"Listening Port": False, "Base Path": False},
        input_required=False,
    ),
    "EvaluateJsonPath": Spec(
        name="EvaluateJsonPath",
        type=_standard("EvaluateJsonPath"),
        artifact=STANDARD,
        relationships=("matched", "unmatched", "failure"),
        properties={
            "Destination": False,
            "Return Type": False,
            "Path Not Found Behavior": False,
            "Null Value Representation": False,
        },
        # The JSON paths themselves are dynamic properties, which the
        # agent may edit (S11); see actions/catalog.py.
        dynamic=True,
    ),
    "MergeContent": Spec(
        name="MergeContent",
        type=_standard("MergeContent"),
        artifact=STANDARD,
        relationships=("merged", "original", "failure"),
        properties={
            "Merge Strategy": False,
            "Merge Format": False,
            "Minimum Number of Entries": False,
            "Maximum Number of Entries": False,
            "Max Bin Age": False,
            "Maximum number of Bins": False,
        },
        editable=("Max Bin Age", "Minimum Number of Entries", "Maximum Number of Entries"),
    ),
    "DuplicateFlowFile": Spec(
        name="DuplicateFlowFile",
        type=_standard("DuplicateFlowFile"),
        artifact=STANDARD,
        relationships=("success",),
        properties={"Number of Copies": False},
        editable=("Number of Copies",),
    ),
    "CompressContent": Spec(
        name="CompressContent",
        type=_standard("CompressContent"),
        artifact=STANDARD,
        relationships=("success", "failure"),
        properties={"Mode": False, "Compression Format": False, "Compression Level": False},
        editable=("Compression Level",),
    ),
    "CryptographicHashContent": Spec(
        name="CryptographicHashContent",
        type=_standard("CryptographicHashContent"),
        artifact=STANDARD,
        relationships=("success", "failure"),
        properties={"Hash Algorithm": False},
    ),
    "DistributeLoad": Spec(
        name="DistributeLoad",
        type=_standard("DistributeLoad"),
        artifact=STANDARD,
        # "1".."N", from Number of Relationships: see relationships().
        relationships=("1",),
        # A dynamic property named for a relationship ("2") is its weight
        # under round robin.
        properties={"Number of Relationships": False, "Distribution Strategy": False},
        dynamic=True,
    ),
    "ControlRate": Spec(
        name="ControlRate",
        type=_standard("ControlRate"),
        artifact=STANDARD,
        relationships=("success", "failure"),
        properties={
            "Rate Control Criteria": False,
            "Maximum Rate": False,
            "Maximum Data Rate": False,
            "Maximum FlowFile Rate": False,
            "Time Duration": False,
        },
        editable=("Maximum Rate", "Maximum Data Rate", "Maximum FlowFile Rate", "Time Duration"),
    ),
    "SplitText": Spec(
        name="SplitText",
        type=_standard("SplitText"),
        artifact=STANDARD,
        relationships=("original", "splits", "failure"),
        properties={
            "Line Split Count": False,
            "Maximum Fragment Size": False,
            "Header Line Count": False,
            "Remove Trailing Newlines": False,
        },
        editable=("Line Split Count",),
    ),
    "ReplaceText": Spec(
        name="ReplaceText",
        type=_standard("ReplaceText"),
        artifact=STANDARD,
        relationships=("success", "failure"),
        # "Regular Expression" is the key; the UI titles it "Search Value".
        properties={
            "Regular Expression": False,
            "Replacement Value": False,
            "Replacement Strategy": False,
            "Evaluation Mode": False,
            "Maximum Buffer Size": False,
            "Character Set": False,
        },
        editable=("Maximum Buffer Size",),
    ),
}


def relationships(kind: str, properties: dict | None = None) -> tuple:
    """A processor's relationships. DistributeLoad's are "1".."N", N from its properties."""

    spec = PROCESSORS.get(kind)

    if spec is None:
        return ()

    if kind == "DistributeLoad":
        try:
            count = int(str((properties or {}).get("Number of Relationships") or 1))

        except ValueError:
            count = 1

        return tuple(str(n) for n in range(1, max(1, count) + 1))

    return spec.relationships


def spec_for(kind: str) -> Spec:
    try:
        return PROCESSORS[kind]

    except KeyError:
        raise ValueError(f"no processor spec for {kind!r}; known: {sorted(PROCESSORS)}") from None


def kind_of(processor: dict) -> str:
    """The short kind ("InvokeHTTP") of a processor dict from a definition."""

    return (processor.get("type") or "").rsplit(".", 1)[-1]
