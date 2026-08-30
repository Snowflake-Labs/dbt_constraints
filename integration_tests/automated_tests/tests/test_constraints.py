"""
General integration tests for dbt_constraints package.

These tests verify that primary keys, unique keys, foreign keys, and not null
constraints are correctly created across different database platforms.

## Why most tests here run no dbt command

The session prepares each matrix cell with `dbt seed --full-refresh` followed by
`dbt build --full-refresh`. That one build creates every model and runs every constraint
the package can create. A test that only needs to confirm "the package created constraint
X" therefore reads that build's log through the `baseline_build` fixture.

Each dbt invocation costs far more than the work it does. An in-database cell pays a CLI
start, a dbt runtime start inside Snowflake, and a full project parse before anything
useful happens: 60 to 90 seconds to rebuild one model and grep its log, which the full
build had already shown.

## The rule for reading baseline_build

Assert on the SPECIFIC constraint name, never on a bare word.

`baseline_build` covers every model. `"primary_key" in log` is therefore true because of
some OTHER model, and such an assertion passes even when the model under test failed. The
earlier version of this file made exactly that mistake once the build was shared.

A test that changes data, or needs its own selection or vars, must still call `run_dbt`.

## Constraint naming

`{adapter}__create_constraints.sql` builds each name as
`{table_identifier}_{columns joined by _}_{PK|UK|FK}`, then upper-cases it and replaces
every character outside [A-Z0-9_$] with `_` (`sanitize_constraint_name`).

Two details the names below depend on:
  - `dim_customers` sets `alias: dim_customer`, and the macro uses the relation
    IDENTIFIER, so its constraints are named DIM_CUSTOMER_*, not DIM_CUSTOMERS_*.
  - A foreign key is named after the CHILD table and the child columns.
"""

# type: ignore
import pytest

# Constraint names the full build must report creating.
#
# Derived from models/schema.yml and the naming macro. DIM_PART_SUPPLIER_PS_SUPPKEY_FK is
# independently confirmed by tests/assert_source_constraints.sql, which checks the same
# name in the database catalog.
#
# Lower case, because baseline_build returns lower-cased text.
PK_DIM_PART = "dim_part_p_partkey_pk"
PK_DIM_CUSTOMERS = "dim_customer_c_custkey_pk"
PK_MULTI_COLUMN = "fact_order_line_l_orderkey_l_linenumber_pk"
PK_DIM_PART_SUPPLIER = "dim_part_supplier_ps_partkey_ps_suppkey_pk"

UK_DIM_PART_SEQ = "dim_part_p_partkey_seq_uk"
UK_DIM_CUSTOMERS_SEQ = "dim_customer_c_custkey_seq_uk"
UK_DIM_ORDERS = "dim_orders_o_orderkey_uk"

FK_DIM_ORDERS = "dim_orders_o_custkey_fk"
FK_MULTI_COLUMN = "fact_order_line_l_partkey_l_suppkey_fk"

# always_create_constraint: true on dim_orders_null_keys. The valid unique key and the
# valid foreign key must still be created even though its primary key has null values.
UK_ALWAYS_CREATE = "dim_orders_null_keys_o_orderkey_seq_uk"
FK_ALWAYS_CREATE = "dim_orders_null_keys_o_custkey_fk"


# Cells whose target database is Snowflake. The dpos_* cells already report "snowflake"
# through the target fixture; these three report their own key.
#
# Snowflake supports the RELY/NORELY flag on a constraint, and
# create_constraints.sql only skips a constraint from a failed test when the adapter does
# NOT support that flag. So these cells record a failed test as NORELY, while postgres,
# oracle and sqlserver create nothing.
SNOWFLAKE_TARGETS = ("snowflake", "fusion", "core2")

# Oracle replaces a constraint name longer than this with PK_/UK_/FK_ || ora_hash(name).
# See oracle__create_constraints.sql. The hashed name cannot be predicted from the model
# name, so an exact-name assertion cannot apply to it.
ORACLE_MAX_IDENTIFIER = 30


def _creating_line(constraint_name: str) -> str:
    """
    Return the exact log line the package writes when it CREATES this constraint.

    A bare `name in log` check does not work in either direction. The package also logs
    `Skipping <name> because ...` when it declines to create a constraint, and dbt logs
    the test node name too, so the constraint name is present in the log whether or not
    the constraint was created. Matching the whole `creating <kind>: <name>` line is the
    only reliable form.

    The kind comes from the name suffix, which the naming macro always appends.
    """
    suffix = constraint_name.rsplit("_", 1)[-1]
    kinds = {"pk": "primary key", "uk": "unique key", "fk": "foreign key"}
    if suffix not in kinds:
        raise ValueError(
            f"{constraint_name!r} does not end in _pk, _uk or _fk, so the constraint "
            "kind cannot be determined."
        )
    return f"creating {kinds[suffix]}: {constraint_name}"


def _skip_if_oracle_hashes(target: str, constraint_name: str) -> None:
    """Skip when Oracle would store this constraint under a hashed name."""
    if target == "oracle" and len(constraint_name) > ORACLE_MAX_IDENTIFIER:
        pytest.skip(
            f"Oracle hashes {constraint_name!r} because it exceeds "
            f"{ORACLE_MAX_IDENTIFIER} characters, so the stored name is "
            "PK_/UK_/FK_ || ora_hash(...) and cannot be matched by name"
        )


def assert_created(baseline_build: str, constraint_name: str, target: str = "") -> None:
    """Fail unless the build log reports creating this exact constraint."""
    _skip_if_oracle_hashes(target, constraint_name)
    line = _creating_line(constraint_name)
    assert line in baseline_build, (
        f"The full build never logged {line!r}. Either the package skipped this "
        "constraint, or the expected name in this module is wrong. Lines the build did "
        "log:\n"
        + "\n".join(
            log_line
            for log_line in baseline_build.splitlines()
            if "creating primary key:" in log_line
            or "creating unique key:" in log_line
            or "creating foreign key:" in log_line
        )
    )


def assert_not_created(
    baseline_build: str, constraint_name: str, target: str = ""
) -> None:
    """Fail if the build log reports creating this constraint."""
    _skip_if_oracle_hashes(target, constraint_name)
    line = _creating_line(constraint_name)
    assert line not in baseline_build, (
        f"The build logged {line!r}, and it must not. The package must skip a "
        "constraint on a view, and must skip a constraint whose test failed."
    )


@pytest.mark.parametrize(
    "constraint_name",
    [
        # dim_orders is absent on purpose. It declares unique and not_null on
        # o_orderkey, not dbt_constraints.primary_key. The package creates no PK.
        PK_DIM_PART,
        PK_DIM_CUSTOMERS,
    ],
)
def test_primary_key_creation(baseline_build, constraint_name, target):
    """Test that primary key constraints are created for models."""
    assert_created(baseline_build, constraint_name, target)


@pytest.mark.parametrize(
    "constraint_name",
    [
        UK_DIM_PART_SEQ,
        UK_DIM_CUSTOMERS_SEQ,
        # dim_orders declares unique on o_orderkey, so it belongs in this list.
        UK_DIM_ORDERS,
    ],
)
def test_unique_key_creation(baseline_build, constraint_name, target):
    """Test that unique key constraints are created for models."""
    assert_created(baseline_build, constraint_name, target)


def test_foreign_key_creation(baseline_build, target):
    """Test that foreign key constraints are created.

    dim_orders.o_custkey references dim_customers.c_custkey. The build creates the
    parent before the child, so a single full build proves the ordering as well.
    """
    assert_created(baseline_build, FK_DIM_ORDERS, target)


def test_multi_column_primary_key(baseline_build, target):
    """Test that multi-column primary keys are created."""
    assert_created(baseline_build, PK_MULTI_COLUMN, target)
    assert_created(baseline_build, PK_DIM_PART_SUPPLIER, target)


def test_multi_column_foreign_key(baseline_build, target):
    """Test that multi-column foreign keys are created."""
    assert_created(baseline_build, FK_MULTI_COLUMN, target)


def test_constraints_not_created_on_views(baseline_build, target):
    """Test that constraints are not created on views.

    dim_customers_view declares a primary key and a unique key. It materializes as a
    view, so the package must run the tests and create neither constraint.
    """
    assert_not_created(baseline_build, "dim_customers_view_c_custkey_pk", target)
    assert_not_created(baseline_build, "dim_customers_view_c_custkey_seq_uk", target)


def test_failed_test_no_constraint(baseline_build, target):
    """Test that constraints are not created when tests fail.

    dim_duplicate_orders holds duplicate values, so its primary key and unique key tests
    fail and produce a NORELY rely_clause.

    create_constraints.sql requires `rely_clause == 'RELY'` OR the adapter to support
    RELY/NORELY. On an adapter without that support, a failed test therefore creates no
    constraint at all. Snowflake does support it and records the constraint as NORELY
    instead, which test_failed_test_creates_norely covers.
    """
    if target in SNOWFLAKE_TARGETS:
        pytest.skip("Snowflake records a failed test as NORELY, see the norely test")

    assert_not_created(baseline_build, "dim_duplicate_orders_o_orderkey_pk", target)
    assert_not_created(baseline_build, "dim_duplicate_orders_o_orderkey_uk", target)
    assert_not_created(baseline_build, "dim_duplicate_orders_o_orderkey_seq_uk", target)


@pytest.mark.snowflake
def test_failed_test_creates_norely(baseline_build, target):
    """A failed test on Snowflake creates the constraint as NORELY.

    Snowflake supports the rely flag, so the package records the constraint and marks it
    NORELY rather than skipping it. The flag is the assertion here: a constraint created
    from a failed test must never be RELY.
    """
    if target not in SNOWFLAKE_TARGETS:
        pytest.skip("Only Snowflake supports RELY/NORELY")

    assert (
        "creating primary key: dim_duplicate_orders_o_orderkey_pk norely"
        in baseline_build
    ), (
        "Expected the failed primary key on dim_duplicate_orders to be created NORELY. "
        "Lines the build did log:\n"
        + "\n".join(
            line
            for line in baseline_build.splitlines()
            if "dim_duplicate_orders" in line and "creating" in line
        )
    )


def test_always_create_constraint_config(baseline_build, target):
    """Test that always_create_constraint config forces constraint creation.

    dim_orders_null_keys sets always_create_constraint on the model. Its primary key has
    null values and stays absent, but the valid unique key and foreign key must still be
    created.
    """
    assert_created(baseline_build, UK_ALWAYS_CREATE, target)
    assert_created(baseline_build, FK_ALWAYS_CREATE, target)


@pytest.mark.postgres
def test_postgres_specific(baseline_build, target):
    """Test PostgreSQL-specific constraint behavior."""
    if target != "postgres":
        pytest.skip("PostgreSQL-specific test")

    # The full build completed, which baseline_build guarantees. Confirm the package
    # created constraints on this adapter rather than skipping every one.
    assert_created(baseline_build, PK_DIM_PART, target)


@pytest.mark.snowflake
def test_snowflake_specific(baseline_build, target):
    """Test Snowflake-specific constraint behavior (RELY/NORELY)."""
    if target != "snowflake":
        pytest.skip("Snowflake-specific test")

    # Snowflake is the only adapter that carries a RELY clause on a constraint.
    assert_created(baseline_build, PK_DIM_PART, target)
    assert "rely" in baseline_build


@pytest.mark.oracle
def test_oracle_specific(baseline_build, target):
    """Test Oracle-specific constraint behavior."""
    if target != "oracle":
        pytest.skip("Oracle-specific test")

    assert_created(baseline_build, PK_DIM_PART, target)


@pytest.mark.sqlserver
def test_sqlserver_specific(baseline_build, target):
    """Test SQL Server-specific constraint behavior."""
    if target != "sqlserver":
        pytest.skip("SQL Server-specific test")

    assert_created(baseline_build, PK_DIM_PART, target)


def test_incremental_rebuild(baseline_build, run_dbt):
    """
    A second build over already-built models must succeed.

    This is the one test here that runs its own command. baseline_build supplies the
    seed and the full refresh, so this only adds the incremental pass. An incremental
    build meets every constraint again on tables that already carry them, which is where
    a constraint-management bug shows up as a duplicate-object or lock error.
    """
    rebuild_result = run_dbt("dbt build", check=False)
    assert rebuild_result.returncode == 0, (
        f"Incremental rebuild failed:\n{rebuild_result.stdout}\n{rebuild_result.stderr}"
    )
