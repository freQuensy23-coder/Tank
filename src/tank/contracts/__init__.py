"""Domain contracts independent of storage, orchestration and network adapters."""

from pydantic import BaseModel, ConfigDict


class DomainModel(BaseModel):
    """Reject coercion and unknown fields at explicit domain construction boundaries."""

    model_config = ConfigDict(
        strict=True,
        extra="forbid",
        frozen=True,
        validate_default=True,
        allow_inf_nan=False,
        revalidate_instances="always",
        hide_input_in_errors=True,
    )
