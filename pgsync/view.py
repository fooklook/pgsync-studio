"""PGSync views."""

import logging
import typing as t
import warnings

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import array
from sqlalchemy.dialects.postgresql.base import PGDDLCompiler
from sqlalchemy.ext import compiler
from sqlalchemy.schema import DDLElement
from sqlalchemy.sql.selectable import Select

from .constants import DEFAULT_SCHEMA, MATERIALIZED_VIEW

logger = logging.getLogger(__name__)

VIEW_LOCK_TIMEOUT = "5s"


class CreateView(DDLElement):
    """
    A class representing a CREATE VIEW statement in PostgreSQL.

    Attributes:
        schema (str): The name of the schema that the view belongs to.
        name (str): The name of the view.
        selectable (Select): The SELECT statement that defines the view.
        materialized (bool): Whether the view is materialized or not. Defaults to True.
    """

    def __init__(
        self,
        schema: str,
        name: str,
        selectable: Select,
        materialized: bool = True,
    ):
        self.schema: str = schema
        self.name: str = name
        self.selectable: Select = selectable
        self.materialized: bool = materialized


@compiler.compiles(CreateView)
def compile_create_view(
    element: CreateView, compiler: PGDDLCompiler, **kwargs
) -> str:
    """
    Compiles a CREATE VIEW statement for PostgreSQL.

    Args:
        element (CreateView): The CreateView object to compile.
        compiler (PGDDLCompiler): The compiler to use.
        **kwargs: Additional keyword arguments.

    Returns:
        str: The compiled CREATE VIEW statement.
    """
    statement: str = compiler.sql_compiler.process(
        element.selectable,
        literal_binds=True,
    )
    materialized: str = "MATERIALIZED" if element.materialized else ""
    return (
        f'CREATE {materialized} VIEW "{element.schema}"."{element.name}" AS '
        f"{statement}"
    )


class DropView(DDLElement):
    """
    A class representing a SQL statement to drop a view.

    Attributes:
    - schema (str): The name of the schema containing the view to be dropped.
    - name (str): The name of the view to be dropped.
    - materialized (bool): Whether the view to be dropped is a materialized view. Defaults to True.
    - cascade (bool): Whether to drop objects that depend on the view to be dropped. Defaults to True.
    """

    def __init__(
        self,
        schema: str,
        name: str,
        materialized: bool = True,
        cascade: bool = True,
    ):
        self.schema: str = schema
        self.name: str = name
        self.materialized: bool = materialized
        self.cascade: bool = cascade


@compiler.compiles(DropView)
def compile_drop_view(
    element: DropView, compiler: PGDDLCompiler, **kwargs
) -> str:
    """
    Compiles a DROP VIEW statement for PostgreSQL.

    Args:
        element (DropView): The DropView object to compile.
        compiler (PGDDLCompiler): The compiler to use.
        **kwargs: Additional keyword arguments.

    Returns:
        str: The compiled DROP VIEW statement.
    """
    cascade: str = "CASCADE" if element.cascade else ""
    materialized: str = "MATERIALIZED" if element.materialized else ""
    return (
        f"DROP {materialized} VIEW IF EXISTS "
        f'"{element.schema}"."{element.name}" {cascade}'
    )


class RefreshView(DDLElement):
    """
    A class representing a view refresh operation in PostgreSQL.

    Attributes:
    -----------
    schema : str
        The schema of the view to be refreshed.
    name : str
        The name of the view to be refreshed.
    concurrently : bool, optional
        Whether or not to refresh the view concurrently. Default is False.
    """

    def __init__(
        self,
        schema: str,
        name: str,
        concurrently: bool = False,
    ):
        self.schema: str = schema
        self.name: str = name
        self.concurrently: bool = concurrently


@compiler.compiles(RefreshView)
def compile_refresh_view(
    element: RefreshView, compiler: PGDDLCompiler, **kwargs
) -> str:
    """
    Compiles a `RefreshView` object into a SQL string that can be executed against a PostgreSQL database.

    Args:
        element (RefreshView): The `RefreshView` object to compile.
        compiler (PGDDLCompiler): The compiler to use for generating the SQL string.
        **kwargs: Additional keyword arguments to pass to the compiler.

    Returns:
        str: The compiled SQL string.
    """
    concurrently: str = "CONCURRENTLY" if element.concurrently else ""
    return (
        f"REFRESH MATERIALIZED VIEW {concurrently} "
        f'"{element.schema}"."{element.name}"'
    )


class CreateIndex(DDLElement):
    """
    A class representing a CREATE INDEX statement in SQL.

    Attributes:
    - name (str): The name of the index.
    - schema (str): The name of the schema that the index belongs to.
    - entity (str): The name of the table or view that the index is created on.
    - columns (list): A list of column names that the index is created on.
    """

    def __init__(self, name: str, schema: str, entity: str, columns: list):
        self.schema: str = schema
        self.name: str = name
        self.entity: str = entity
        self.columns: list = columns


@compiler.compiles(CreateIndex)
def compile_create_index(
    element: CreateIndex, compiler: PGDDLCompiler, **kwargs
) -> str:
    """
    Compiles a CreateIndex object into a SQL statement.

    Args:
        element (CreateIndex): The CreateIndex object to compile.
        compiler (PGDDLCompiler): The compiler to use for compilation.
        **kwargs: Additional keyword arguments.

    Returns:
        str: The compiled SQL statement.
    """
    return (
        f"CREATE UNIQUE INDEX {element.name} ON "
        f'"{element.schema}"."{element.entity}" ({", ".join(element.columns)})'
    )


class DropIndex(DDLElement):
    """
    A class representing a DROP INDEX statement in SQL.

    Attributes:
    - name (str): The name of the index to be dropped.
    """

    def __init__(self, name: str):
        self.name: str = name


@compiler.compiles(DropIndex)
def compile_drop_index(
    element: DropIndex, compiler: PGDDLCompiler, **kwargs
) -> str:
    """
    Compiles a DropIndex object into a SQL string that drops the index if it exists.

    Args:
        element (DropIndex): The DropIndex object to compile.
        compiler (PGDDLCompiler): The compiler to use for compilation.
        **kwargs: Additional keyword arguments.

    Returns:
        str: The compiled SQL string.
    """
    return f"DROP INDEX IF EXISTS {element.name}"


def _get_constraints(
    models: t.Callable,
    schema: str,
    tables: t.Set[str],
    label: str,
    constraint_type: str,
) -> sa.sql.Select:
    """
    Returns a SQLAlchemy Select object that selects the table name and an array of column names for each table in the given set of tables that has a constraint of the given constraint type.

    Args:
        models (Callable): A callable that returns a SQLAlchemy Table object for the given table name and schema.
        schema (str): The schema to search for constraints.
        tables (Set[str]): The set of table names to search for constraints.
        label (str): The label to give to the array of column names in the result set.
        constraint_type (str): The type of constraint to search for.

    Returns:
        sa.sql.Select: A SQLAlchemy Select object that selects the table name and an array of column names for each table in the given set of tables that has a constraint of the given constraint type.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=sa.exc.SAWarning)
        table_constraints = models("table_constraints", "information_schema")
        key_column_usage = models("key_column_usage", "information_schema")
    return (
        sa.select(
            *[
                table_constraints.c.table_name,
                sa.func.ARRAY_AGG(
                    sa.cast(
                        key_column_usage.c.column_name,
                        sa.TEXT,
                    )
                ).label(label),
            ]
        )
        .join(
            key_column_usage,
            sa.and_(
                key_column_usage.c.constraint_name
                == table_constraints.c.constraint_name,
                key_column_usage.c.table_schema
                == table_constraints.c.table_schema,
                key_column_usage.c.table_schema == schema,
            ),
        )
        .where(
            *[
                table_constraints.c.table_name.in_(tables),
                table_constraints.c.constraint_type == constraint_type,
            ]
        )
        .group_by(table_constraints.c.table_name)
    )


def _primary_keys(
    models: t.Callable, schema: str, tables: t.Set[str]
) -> sa.sql.Select:
    """
    Returns a SQLAlchemy Select object that represents the primary keys of the specified tables in the given schema.

    Args:
        models (Callable): A callable that returns a SQLAlchemy MetaData object.
        schema (str): The name of the schema containing the tables.
        tables (Set[str]): A set of table names to retrieve primary keys for.

    Returns:
        sa.sql.Select: A SQLAlchemy Select object representing the primary keys of the specified tables.
    """
    return _get_constraints(
        models,
        schema,
        tables,
        label="primary_keys",
        constraint_type="PRIMARY KEY",
    )


def _foreign_keys(
    models: t.Callable, schema: str, tables: t.Set[str]
) -> sa.sql.Select:
    """
    Returns a SQLAlchemy SELECT statement that retrieves foreign key constraints for the specified tables in the given schema.

    Args:
        models (Callable): A callable that returns a SQLAlchemy metadata object.
        schema (str): The name of the schema to retrieve foreign key constraints from.
        tables (Set[str]): A set of table names to retrieve foreign key constraints for.

    Returns:
        sa.sql.Select: A SQLAlchemy SELECT statement that retrieves foreign key constraints for the specified tables in the given schema.
    """
    return _get_constraints(
        models,
        schema,
        tables,
        label="foreign_keys",
        constraint_type="FOREIGN KEY",
    )


def _view_values_statement(rows: dict[str, dict[str, t.Any]]) -> Select:
    """Build the VALUES query used by the trigger metadata view."""

    normalized_rows: dict[t.Optional[str], dict[str, t.Any]] = dict(rows)
    if not normalized_rows:
        normalized_rows[None] = {
            "primary_keys": set(),
            "foreign_keys": set(),
            "indices": set(),
            "columns": set(),
            "payload_columns": set(),
        }

    def fields_array(fields: dict[str, t.Any], name: str) -> t.Any:
        values: list[str] = sorted(
            {str(item) for item in (fields.get(name) or []) if item}
        )
        return array(values) if values else None

    return sa.select(
        sa.sql.Values(
            sa.column("table_name"),
            sa.column("primary_keys"),
            sa.column("foreign_keys"),
            sa.column("indices"),
            sa.column("columns"),
            sa.column("payload_columns"),
        )
        .data(
            [
                (
                    table_name,
                    fields_array(fields, "primary_keys"),
                    fields_array(fields, "foreign_keys"),
                    fields_array(fields, "indices"),
                    fields_array(fields, "columns"),
                    fields_array(fields, "payload_columns"),
                )
                for table_name, fields in sorted(
                    normalized_rows.items(),
                    key=lambda item: str(item[0] or ""),
                )
            ]
        )
        .alias("t")
    )


def replace_view_rows(
    conn: sa.engine.Connection,
    schema: str,
    rows: dict[str, dict[str, t.Any]],
    *,
    has_materialized_view: t.Optional[bool] = None,
    has_plain_view: t.Optional[bool] = None,
) -> None:
    """Replace the trigger metadata view from an explicit, trusted snapshot.

    The caller owns the transaction and the PGSync view advisory lock.  This
    is intentionally separate from :func:`create_view`, whose public contract
    is to merge metadata for one job during bootstrap.
    """

    if has_materialized_view is None:
        has_materialized_view = _is_view(
            conn,
            schema,
            MATERIALIZED_VIEW,
            materialized=True,
        )
    if has_plain_view is None:
        has_plain_view = False
        if not has_materialized_view:
            has_plain_view = _is_view(
                conn,
                schema,
                MATERIALIZED_VIEW,
                materialized=False,
            )
    if has_materialized_view or has_plain_view:
        conn.execute(
            DropView(
                schema,
                MATERIALIZED_VIEW,
                materialized=bool(has_materialized_view),
            )
        )
    conn.execute(
        CreateView(
            schema,
            MATERIALIZED_VIEW,
            _view_values_statement(rows),
        )
    )
    conn.execute(sa.text(f'DROP INDEX IF EXISTS "{schema}"."_idx"'))
    conn.execute(
        CreateIndex(
            "_idx",
            schema,
            MATERIALIZED_VIEW,
            ["table_name"],
        )
    )


def _create_view(
    conn: sa.engine.Connection,
    models: t.Callable,
    index: str,
    schema: str,
    tables: t.Set,
    user_defined_fkey_tables: dict,
    node_columns: dict,
    payload_columns: t.Optional[dict] = None,
) -> None:
    """
    This module defines a function `create_view` that creates a view describing primary_keys and foreign_keys for each table
    with an index on table_name. The view is used within the trigger function to determine what payload values to send to pg_notify.

    Args:
        conn (sa.engine.Connection): SQLAlchemy transaction connection.
        models (Callable): A callable that returns a list of SQLAlchemy models.
        index (str): The name of the index.
        schema (str): The name of the schema.
        tables (Set): A set of table names.
        user_defined_fkey_tables (dict): A dictionary containing user-defined foreign key tables.
        node_columns (dict): A dictionary containing node columns.

    Returns:
        None

    Raises:
        None

        This is only called once on bootstrap.
        It is used within the trigger function to determine what payload
        values to send to pg_notify.

        Since views cannot be modified, we query the existing view for exiting
        rows and union this to the next query.

        So if 'specie' was the only row before, and the next query returns
        'unit' and 'structure', we want to end up with the result below.

         table_name | primary_keys | foreign_keys     | indices    | columns
        ------------+--------------+------------------+------------+--------------
         specie     | {id}         | {id, user_id}    | {foo, bar} | {name, age}
         unit       | {id}         | {id, profile_id} | {foo, bar} | {name, description}
         structure  | {id}         | {id}             | {foo, bar} | {name, type}
         unit       | {id}         | {id, profile_id} | {foo, bar} | {name, description}
         structure  | {id}         | {id}             | {foo, bar} | {name, type}
    """

    rows: dict = {}
    has_materialized_view: bool = _is_view(
        conn,
        schema,
        MATERIALIZED_VIEW,
        materialized=True,
    )
    has_plain_view: bool = False
    if not has_materialized_view:
        has_plain_view = _is_view(
            conn,
            schema,
            MATERIALIZED_VIEW,
            materialized=False,
        )
    if has_materialized_view or has_plain_view:
        existing_rows = conn.execute(
            sa.select("*").select_from(
                sa.text(f"{schema}.{MATERIALIZED_VIEW}")
            )
        ).fetchall()
        for row in existing_rows:
            mapping = getattr(row, "_mapping", {})
            table_name = mapping.get("table_name")
            primary_keys = mapping.get("primary_keys")
            foreign_keys = mapping.get("foreign_keys")
            indices = mapping.get("indices")
            columns = mapping.get("columns")
            existing_payload_columns = mapping.get("payload_columns")
            if table_name is None and len(row) >= 1:
                table_name = row[0]
            if primary_keys is None and len(row) >= 2:
                primary_keys = row[1]
            if foreign_keys is None and len(row) >= 3:
                foreign_keys = row[2]
            if indices is None and len(row) >= 4:
                indices = row[3]
            if columns is None and len(row) >= 5:
                columns = row[4]
            if existing_payload_columns is None and len(row) >= 6:
                existing_payload_columns = row[5]
            rows.setdefault(
                table_name,
                {
                    "primary_keys": set(),
                    "foreign_keys": set(),
                    "indices": set(),
                    "columns": set(),
                    "payload_columns": set(),
                },
            )
            if primary_keys:
                rows[table_name]["primary_keys"] = set(primary_keys)
            if foreign_keys:
                rows[table_name]["foreign_keys"] = set(foreign_keys)
            if indices:
                rows[table_name]["indices"] = set(indices)
            if columns:
                rows[table_name]["columns"] = set(columns)
            if existing_payload_columns:
                rows[table_name]["payload_columns"] = set(
                    existing_payload_columns
                )

    if schema != DEFAULT_SCHEMA:
        for table in set(tables):
            tables.add(f"{schema}.{table}")

    for table_name, columns in conn.execute(
        _primary_keys(models, schema, tables)
    ).fetchall():
        rows.setdefault(
            table_name,
            {
                "primary_keys": set(),
                "foreign_keys": set(),
                "indices": set(),
                "columns": set(),
                "payload_columns": set(),
            },
        )
        if columns:
            rows[table_name]["primary_keys"] |= set(columns)
            rows[table_name]["indices"] |= set([index])
            # NB: add the primary key columns to the columns
            # set as well in case its a through table
            rows[table_name]["columns"] |= set(columns)

    for table_name, columns in conn.execute(
        _foreign_keys(models, schema, tables)
    ).fetchall():
        rows.setdefault(
            table_name,
            {
                "primary_keys": set(),
                "foreign_keys": set(),
                "indices": set(),
                "columns": set(),
                "payload_columns": set(),
            },
        )
        if columns:
            rows[table_name]["foreign_keys"] |= set(columns)
            rows[table_name]["indices"] |= set([index])
            # NB: add the foreign key columns to the columns
            # set as well in case its a through table
            rows[table_name]["columns"] |= set(columns)

    if user_defined_fkey_tables:
        for table_name, columns in user_defined_fkey_tables.items():
            rows.setdefault(
                table_name,
                {
                    "primary_keys": set(),
                    "foreign_keys": set(),
                    "indices": set(),
                    "columns": set(),
                    "payload_columns": set(),
                },
            )
            if columns:
                rows[table_name]["foreign_keys"] |= set(columns)
                rows[table_name]["indices"] |= set([index])

    if node_columns:
        for table_name, columns in node_columns.items():
            rows.setdefault(
                table_name,
                {
                    "primary_keys": set(),
                    "foreign_keys": set(),
                    "indices": set(),
                    "columns": set(),
                    "payload_columns": set(),
                },
            )
            if columns:
                rows[table_name]["columns"] |= set(columns)
                rows[table_name]["indices"] |= set([index])

    if payload_columns:
        for table_name, columns in payload_columns.items():
            rows.setdefault(
                table_name,
                {
                    "primary_keys": set(),
                    "foreign_keys": set(),
                    "indices": set(),
                    "columns": set(),
                    "payload_columns": set(),
                },
            )
            if columns:
                rows[table_name]["payload_columns"] |= set(columns)
                rows[table_name]["indices"] |= set([index])

    if not rows:
        rows.setdefault(
            None,
            {
                "primary_keys": set(),
                "foreign_keys": set(),
                "indices": set(),
                "columns": set(),
                "payload_columns": set(),
            },
        )

    statement = sa.select(
        sa.sql.Values(
            sa.column("table_name"),
            sa.column("primary_keys"),
            sa.column("foreign_keys"),
            sa.column("indices"),
            sa.column("columns"),
            sa.column("payload_columns"),
        )
        .data(
            [
                (
                    table_name,
                    (
                        array(fields["primary_keys"])
                        if fields.get("primary_keys")
                        else None
                    ),
                    (
                        array(fields.get("foreign_keys"))
                        if fields.get("foreign_keys")
                        else None
                    ),
                    (
                        array(fields.get("indices"))
                        if fields.get("indices")
                        else None
                    ),
                    (
                        array(fields.get("columns"))
                        if fields.get("columns")
                        else None
                    ),
                    (
                        array(fields.get("payload_columns"))
                        if fields.get("payload_columns")
                        else None
                    ),
                )
                for table_name, fields in rows.items()
            ]
        )
        .alias("t")
    )
    logger.debug(f"Creating view: {schema}.{MATERIALIZED_VIEW}")
    if has_materialized_view or has_plain_view:
        conn.execute(
            DropView(
                schema,
                MATERIALIZED_VIEW,
                materialized=has_materialized_view,
            )
        )
    conn.execute(CreateView(schema, MATERIALIZED_VIEW, statement))
    conn.execute(DropIndex("_idx"))
    conn.execute(
        CreateIndex(
            "_idx",
            schema,
            MATERIALIZED_VIEW,
            ["table_name"],
        )
    )


def create_view(
    engine: sa.engine.Engine,
    models: t.Callable,
    fetchall: t.Callable,
    index: str,
    schema: str,
    tables: t.Set,
    user_defined_fkey_tables: dict,
    views: t.List[str],
    node_columns: dict,
    payload_columns: t.Optional[dict] = None,
) -> None:
    """Atomically merge metadata into the trigger helper view.

    ``fetchall`` and ``views`` remain in the public signature for backwards
    compatibility. All reads use the transaction connection so the advisory
    lock covers the complete read/modify/write cycle.
    """

    with engine.begin() as conn:
        conn.execute(
            sa.text(f"SET LOCAL lock_timeout = '{VIEW_LOCK_TIMEOUT}'")
        )
        conn.execute(
            sa.text(
                "SELECT pg_advisory_xact_lock("
                "hashtext(:namespace), hashtext(:resource))"
            ),
            {
                "namespace": "pgsync",
                "resource": f"{schema}.{MATERIALIZED_VIEW}",
            },
        )
        _create_view(
            conn,
            models,
            index,
            schema,
            tables,
            user_defined_fkey_tables,
            node_columns,
            payload_columns,
        )
    logger.debug(f"Created view: {schema}.{MATERIALIZED_VIEW}")


def _is_view(
    conn: sa.engine.Connection,
    schema: str,
    table: str,
    materialized: bool = True,
) -> bool:
    """Check view existence using an existing transaction connection."""
    column: str = "matviewname" if materialized else "viewname"
    pg_table: str = "pg_matviews" if materialized else "pg_views"
    return (
        conn.execute(
            sa.select(*[sa.column(column)])
            .select_from(sa.text(pg_table))
            .where(
                sa.and_(
                    *[
                        sa.column(column) == table,
                        sa.column("schemaname") == schema,
                    ]
                )
            )
            .with_only_columns(*[sa.func.COUNT()])
            .order_by(None)
        ).scalar()
        > 0
    )


def is_view(
    engine: sa.engine.Engine,
    schema: str,
    table: str,
    materialized: bool = True,
) -> bool:
    """
    Check if a given table is a view in the specified schema.

    Args:
        engine (sa.engine.Engine): SQLAlchemy engine to use for the database connection.
        schema (str): Name of the schema to check for the table.
        table (str): Name of the table to check.
        materialized (bool, optional): Whether to check for a materialized view or a regular view. Defaults to True.

    Returns:
        bool: True if the table is a view, False otherwise.
    """
    with engine.connect() as conn:
        return _is_view(conn, schema, table, materialized=materialized)
