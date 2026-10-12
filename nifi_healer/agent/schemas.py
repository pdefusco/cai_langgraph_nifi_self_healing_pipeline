"""
The shape of the LLM's reply. Pydantic checks types and field names; the
catalog checks bounds and edits.py checks the flow, so a reply that
parses here can still be invalid.
"""

from pydantic import BaseModel, ConfigDict, Field


class Step(BaseModel):
    # A misspelled field ("tasks" for "value") is an error the model is
    # told about, not a field silently dropped.
    model_config = ConfigDict(extra="forbid")

    action: str
    target: str = ""
    value: str | int | float | None = None
    property: str | None = None
    relationship: str | list[str] | None = None
    dlq: str | None = None
    object_threshold: int | None = None
    size_threshold: str | None = None
    parameter: str | None = None
    cluster_size: str | None = None
    nodes: int | None = None
    backoff: str | None = None
    max_backoff: str | None = None
    retry_count: int | None = None
    penalty: str | None = None
    yield_duration: str | None = None


class Proposal(BaseModel):
    model_config = ConfigDict(extra="ignore")

    diagnosis: str
    root_cause_component: str = ""
    steps: list[Step] = Field(min_length=1)
    rationale: str = ""
    citations: list[str] = Field(default_factory=list)
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    expected_effect: str = ""

    def as_dict(self) -> dict:
        data = self.model_dump()
        data["steps"] = [s.model_dump(exclude_none=True) for s in self.steps]

        return data
