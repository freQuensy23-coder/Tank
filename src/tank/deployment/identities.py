"""Scalar publisher artifact identities shared by inward records and outer runtime."""

from typing import Annotated

from pydantic import Field

type CommitSHA = Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]
type SHA256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
