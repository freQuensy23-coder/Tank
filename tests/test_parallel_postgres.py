"""Real libpq parsing and routing policy; no connection or mocked parser."""

import pytest
from parallel_postgres import (
    DATABASE_VARIABLES,
    configure_environment,
    parallel_requested,
    resolve_test_databases,
)
from psycopg.conninfo import conninfo_to_dict


@pytest.mark.parametrize(
    "dsn",
    [
        "dbname=tank_test_case_{worker} host=/tmp/socket user=postgres options='-c statement_timeout=1000'",
        "postgresql://postgres:synthetic-password@localhost:5432/tank_test_case_{worker}?application_name=audit",
    ],
)
def test_routes_only_dbname_and_preserves_other_libpq_fields(dsn) -> None:
    original = conninfo_to_dict(dsn)
    environment = {DATABASE_VARIABLES[0]: dsn}
    routed = resolve_test_databases(environment, "gw12")
    actual = conninfo_to_dict(routed[DATABASE_VARIABLES[0]])
    assert actual == {**original, "dbname": "tank_test_case_gw12"}
    assert environment == {DATABASE_VARIABLES[0]: dsn}


def test_controller_keeps_exact_templates_for_worker_inheritance() -> None:
    environment = {
        name: f"postgresql:///tank_test_case{index}_{{worker}}?host=%2Ftmp%2Fsocket"
        for index, name in enumerate(DATABASE_VARIABLES)
    }
    assert resolve_test_databases(environment, None) == environment


def test_independent_workers_receive_different_precreated_databases() -> None:
    original = {DATABASE_VARIABLES[0]: "dbname=tank_test_case_{worker} host=/tmp/socket"}
    first = conninfo_to_dict(resolve_test_databases(original, "gw0")[DATABASE_VARIABLES[0]])
    second = conninfo_to_dict(resolve_test_databases(original, "gw1")[DATABASE_VARIABLES[0]])
    assert first["dbname"] == "tank_test_case_gw0"
    assert second["dbname"] == "tank_test_case_gw1"
    assert first["host"] == second["host"] == "/tmp/socket"


@pytest.mark.parametrize("worker", ["", "master", "gw", "gw-1", "gw0/escape", "GW0", "gw1\n"])
def test_rejects_bad_worker_identity(worker) -> None:
    with pytest.raises(ValueError, match="worker ID"):
        resolve_test_databases({}, worker)


@pytest.mark.parametrize(
    "dbname",
    [
        "tank",
        "production_{worker}",
        "tank_test_shared",
        "tank_test_{{worker}}",
        "tank_test_{worker}_{worker}",
        "tank_test_{other}",
        "tank_test_{worker}/escape",
    ],
)
def test_rejects_shared_non_test_nested_and_unsafe_database_templates(dbname) -> None:
    with pytest.raises(ValueError, match="dbname"):
        resolve_test_databases({DATABASE_VARIABLES[0]: f"dbname='{dbname}'"}, "gw0")


@pytest.mark.parametrize("field", ["host", "user", "password", "options", "application_name"])
def test_rejects_placeholders_outside_database(field) -> None:
    dsn = f"dbname=tank_test_case_{{worker}} {field}='unsafe_{{worker}}'"
    with pytest.raises(ValueError, match="only in dbname"):
        resolve_test_databases({DATABASE_VARIABLES[0]: dsn}, "gw0")


def test_invalid_last_dsn_cannot_partially_rewrite_environment() -> None:
    environment = {
        DATABASE_VARIABLES[0]: "dbname=tank_test_first_{worker}",
        DATABASE_VARIABLES[1]: "dbname=tank_test_second_{worker}",
        DATABASE_VARIABLES[2]: "dbname=production",
        "UNRELATED": "unchanged",
    }
    original = dict(environment)
    with pytest.raises(ValueError):
        configure_environment(environment, "gw0")
    assert environment == original


def test_absent_database_routes_are_allowed_and_unrelated_environment_is_unchanged() -> None:
    environment = {"UNRELATED": "unchanged"}
    configure_environment(environment, "gw3")
    assert environment == {"UNRELATED": "unchanged"}
    assert resolve_test_databases({}, None) == {}


def test_success_updates_all_three_routes_only_after_validation() -> None:
    environment = {name: "dbname=tank_test_case_{worker}" for name in DATABASE_VARIABLES}
    configure_environment(environment, "gw2")
    assert all(
        conninfo_to_dict(environment[name])["dbname"] == "tank_test_case_gw2" for name in DATABASE_VARIABLES
    )


@pytest.mark.parametrize(
    "args,expected",
    [
        (["-n", "auto"], True),
        (["-n", "4"], True),
        (["-n2"], True),
        (["--numprocesses=2"], True),
        (["-n", "0"], False),
        ([], False),
        (["-n", "-1"], False),
        (["-n", "logical"], False),
        (["-n"], False),
    ],
)
def test_opt_in_parallel_argument_policy(args, expected) -> None:
    assert parallel_requested(args) is expected


def test_parser_error_does_not_echo_dsn_credentials() -> None:
    with pytest.raises(ValueError) as error:
        resolve_test_databases({DATABASE_VARIABLES[0]: "password='synthetic-do-not-echo"}, "gw0")
    assert "synthetic-do-not-echo" not in str(error.value)
