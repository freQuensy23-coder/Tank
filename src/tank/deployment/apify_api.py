"""Apify HTTP adapter projecting foreign JSON into consumed publisher records."""

import json
import urllib.error
import urllib.request
from http.client import HTTPMessage
from typing import IO, Literal, Self
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, JsonValue, SecretStr, model_validator

from tank.contracts import DomainModel
from tank.contracts.json import decode_unique_json
from tank.deployment.api_contracts import (
    ActorSnapshot,
    ActorTagUpdate,
    ActorVersionsSnapshot,
    APIFailureReason,
    BuildObservation,
    CompletedBuildSnapshot,
    HistoricalBuildSnapshot,
    ProviderErrorEnvelope,
    ReportedAPIReason,
    UnreportedAPIReason,
    UploadedVersion,
    VersionUpload,
)
from tank.deployment.contracts import BuildID
from tank.deployment.identities import CommitSHA


class ApifyAPIError(RuntimeError):
    pass


class ApifyEndpoint(DomainModel):
    base_url: str

    @model_validator(mode="after")
    def validate_base(self) -> Self:
        url = urlsplit(self.base_url)
        if (
            url.scheme not in {"https", "http"}
            or url.hostname is None
            or url.username is not None
            or url.password is not None
            or url.query
            or url.fragment
            or url.path != "/v2"
        ):
            raise ValueError("Invalid Apify API endpoint")
        return self


class ApiEnvelope(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore", hide_input_in_errors=True)
    data: dict[str, JsonValue]


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: IO[bytes],
        code: int,
        msg: str,
        headers: HTTPMessage,
        newurl: str,
    ) -> None:
        return None


type Method = Literal["GET", "POST", "PUT"]
type Upload = VersionUpload | ActorTagUpdate


class ApifyHTTPFailure(ApifyAPIError):
    def __init__(self, method: Method, status_code: int, diagnostic: APIFailureReason) -> None:
        self.status_code = status_code
        self.diagnostic = diagnostic
        message = f"Apify {method} failed with HTTP {status_code}"
        if isinstance(diagnostic, ReportedAPIReason):
            message += ": " + diagnostic.text
        super().__init__(message)


class ApifyAPI:
    def __init__(self, token: SecretStr, endpoint: ApifyEndpoint) -> None:
        if not token.get_secret_value():
            raise ApifyAPIError("Apify API credential is empty")
        self.token = token
        self.endpoint = endpoint
        self.opener = urllib.request.build_opener(NoRedirect, urllib.request.ProxyHandler({}))

    def _reason(self, error: urllib.error.HTTPError) -> APIFailureReason:
        try:
            try:
                document = error.read()
            finally:
                error.close()
        except (OSError, ValueError):
            return UnreportedAPIReason(state="UNREPORTED", reason="READ_FAILED")
        if not document:
            return UnreportedAPIReason(state="UNREPORTED", reason="EMPTY_OR_MISSING")
        try:
            decode_unique_json(document)
            envelope = ProviderErrorEnvelope.model_validate_json(document)
        except ValueError:
            return UnreportedAPIReason(state="UNREPORTED", reason="INVALID_JSON_OR_FIELDS")
        if envelope.error is None:
            return UnreportedAPIReason(state="UNREPORTED", reason="EMPTY_OR_MISSING")
        parts: list[str] = []
        if envelope.error.type is not None:
            parts.append(envelope.error.type)
        if envelope.error.message is not None:
            parts.append(envelope.error.message)
        text = " ".join(parts).strip()
        if not text:
            return UnreportedAPIReason(state="UNREPORTED", reason="EMPTY_OR_MISSING")
        return ReportedAPIReason(
            state="REPORTED", text=text.replace(self.token.get_secret_value(), "[token]")[:300]
        )

    def _request(self, method: Method, path: str, body: Upload | None) -> ApiEnvelope:
        payload = (
            None
            if body is None
            else json.dumps(body.model_dump(mode="json"), sort_keys=True, separators=(",", ":")).encode()
        )
        request = urllib.request.Request(
            self.endpoint.base_url + path,
            data=payload,
            method=method,
            headers={
                "Authorization": "Bearer " + self.token.get_secret_value(),
                "Content-Type": "application/json",
            },
        )
        try:
            with self.opener.open(request, timeout=75) as response:
                document = response.read()
        except urllib.error.HTTPError as error:
            raise ApifyHTTPFailure(method, error.code, self._reason(error)) from None
        except OSError:
            raise ApifyAPIError(f"Apify {method} transport failure") from None
        try:
            decode_unique_json(document)
            return ApiEnvelope.model_validate_json(document)
        except ValueError:
            raise ApifyAPIError(f"Apify {method} invalid JSON response envelope") from None

    def _project[T: BaseModel](self, method: Method, path: str, body: Upload | None, schema: type[T]) -> T:
        return schema.model_validate(self._request(method, path, body).data)

    def actor(self) -> ActorSnapshot:
        return self._project("GET", "/actors/rbf5CQKQgKNVl4SfO", None, ActorSnapshot)

    def actor_versions(self) -> ActorVersionsSnapshot:
        return self._project("GET", "/actors/rbf5CQKQgKNVl4SfO", None, ActorVersionsSnapshot)

    def completed_build(self, build_id: BuildID) -> CompletedBuildSnapshot:
        record = self._project("GET", "/actor-builds/" + build_id, None, CompletedBuildSnapshot)
        if record.id != build_id:
            raise ApifyAPIError("Build response identity mismatch")
        return record

    def historical_build(self) -> HistoricalBuildSnapshot:
        return self._project("GET", "/actor-builds/c99YRMlKXube5V0gV", None, HistoricalBuildSnapshot)

    def upload_version(self, version: VersionUpload, *, replace: bool) -> None:
        path = "/actors/rbf5CQKQgKNVl4SfO/versions"
        if replace:
            path += "/0.3"
        self._request("PUT" if replace else "POST", path, version)

    def uploaded_version(self) -> UploadedVersion:
        return self._project("GET", "/actors/rbf5CQKQgKNVl4SfO/versions/0.3", None, UploadedVersion)

    def start_build(self, release_sha: CommitSHA) -> BuildObservation:
        return self._project(
            "POST",
            "/actors/rbf5CQKQgKNVl4SfO/builds?version=0.3&tag=tank-" + release_sha[:12],
            None,
            BuildObservation,
        )

    def wait_build(self, build_id: BuildID) -> BuildObservation:
        record = self._project(
            "GET", "/actor-builds/" + build_id + "?waitForFinish=60", None, BuildObservation
        )
        if record.id != build_id:
            raise ApifyAPIError("Build response identity mismatch")
        return record

    def update_direct_tag(self, update: ActorTagUpdate) -> None:
        self._request("PUT", "/actors/rbf5CQKQgKNVl4SfO", update)
