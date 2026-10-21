"""View tests."""

import threading
from concurrent.futures import (
    ThreadPoolExecutor,
    TimeoutError as FutureTimeoutError,
)

import pytest
import sqlalchemy as sa
from mock import call, MagicMock, patch

from pgsync.base import Base, create_schema, subtransactions
from pgsync.constants import DEFAULT_SCHEMA
from pgsync.settings import IS_MYSQL_COMPAT
from pgsync.view import (
    _foreign_keys,
    _primary_keys,
    create_view,
    CreateIndex,
    CreateView,
    DropIndex,
    DropView,
    is_view,
    RefreshView,
)


@pytest.mark.skipif(
    IS_MYSQL_COMPAT,
    reason="Skipped because IS_MYSQL_COMPAT env var is set",
)
class TestView(object):
    """View tests."""

    @pytest.fixture(scope="function")
    def data(self, sync, book_cls):
        session = sync.session
        books = [
            book_cls(
                isbn="abc",
                title="The world is yours",
                description="Tigers are mystical creatures",
            ),
        ]
        with subtransactions(session):
            session.add_all(books)
        yield books

        with subtransactions(session):
            session.query(book_cls).delete()

        session.connection().engine.dispose(close=True)
        sync.search_client.close()

    def test_create_materialized_view(self, connection):
        """Test create materialized view."""
        view = "test_mat_view"
        with connection.engine.connect() as conn:
            conn.execute(
                CreateView(
                    DEFAULT_SCHEMA, view, sa.select(1), materialized=True
                )
            )
            conn.commit()
        assert (
            is_view(connection.engine, DEFAULT_SCHEMA, view, materialized=True)
            is True
        )
        assert (
            is_view(
                connection.engine, DEFAULT_SCHEMA, view, materialized=False
            )
            is False
        )
        with connection.engine.connect() as conn:
            conn.execute(DropView(DEFAULT_SCHEMA, view, materialized=True))
            conn.commit()

    def test_create_view(self, connection):
        """Test create non-materialized view."""
        view = "test_view"
        with connection.engine.connect() as conn:
            conn.execute(
                CreateView(
                    DEFAULT_SCHEMA, view, sa.select(1), materialized=False
                )
            )
            conn.commit()
        assert (
            is_view(
                connection.engine, DEFAULT_SCHEMA, view, materialized=False
            )
            is True
        )
        assert (
            is_view(connection.engine, DEFAULT_SCHEMA, view, materialized=True)
            is False
        )
        with connection.engine.connect() as conn:
            conn.execute(DropView(DEFAULT_SCHEMA, view, materialized=False))
            conn.commit()

    def test_drop_materialized_view(self, connection):
        """Test drop materialized view."""
        view = "test_view_drop"
        with connection.engine.connect() as conn:
            conn.execute(
                CreateView(
                    DEFAULT_SCHEMA, view, sa.select(1), materialized=True
                )
            )
            conn.commit()
        assert (
            is_view(connection.engine, DEFAULT_SCHEMA, view, materialized=True)
            is True
        )
        with connection.engine.connect() as conn:
            conn.execute(DropView(DEFAULT_SCHEMA, view, materialized=True))
            conn.commit()
        assert (
            is_view(connection.engine, DEFAULT_SCHEMA, view, materialized=True)
            is False
        )
        assert (
            is_view(
                connection.engine, DEFAULT_SCHEMA, view, materialized=False
            )
            is False
        )

    def test_drop_view(self, connection):
        """Test drop non-materialized view."""
        view = "test_view_drop"
        with connection.engine.connect() as conn:
            conn.execute(
                CreateView(
                    DEFAULT_SCHEMA, view, sa.select(1), materialized=False
                )
            )
            conn.commit()
        assert (
            is_view(
                connection.engine, DEFAULT_SCHEMA, view, materialized=False
            )
            is True
        )
        with connection.engine.connect() as conn:
            conn.execute(DropView(DEFAULT_SCHEMA, view, materialized=False))
            conn.commit()
        assert (
            is_view(connection.engine, DEFAULT_SCHEMA, view, materialized=True)
            is False
        )
        assert (
            is_view(
                connection.engine, DEFAULT_SCHEMA, view, materialized=False
            )
            is False
        )

    @pytest.mark.usefixtures("table_creator")
    def test_refresh_view(self, connection, sync, book_cls, data):
        """Test refresh materialized view."""
        view = "test_view_refresh"
        pg_base = Base(connection.engine.url.database)

        model = pg_base.models("book", "public")
        statement = sa.select(*[model.c.isbn]).select_from(model)
        with connection.engine.connect() as conn:
            conn.execute(
                CreateView(DEFAULT_SCHEMA, view, statement, materialized=True)
            )
            conn.commit()

        with connection.engine.connect() as conn:
            assert [
                result.isbn
                for result in conn.execute(sa.text(f"SELECT * FROM {view}"))
            ][0] == "abc"

        session = sync.session
        with subtransactions(session):
            session.execute(
                book_cls.__table__.update()
                .where(book_cls.__table__.c.isbn == "abc")
                .values(isbn="xyz")
            )

        with connection.engine.connect() as conn:
            # the value should still be abc
            assert [
                result.isbn
                for result in conn.execute(sa.text(f"SELECT * FROM {view}"))
            ][0] == "abc"

        with connection.engine.connect() as conn:
            conn.execute(RefreshView(DEFAULT_SCHEMA, view))
            conn.commit()

        with connection.engine.connect() as conn:
            # the value should now be xyz
            assert [
                result.isbn
                for result in conn.execute(sa.text(f"SELECT * FROM {view}"))
            ][0] == "xyz"

        with connection.engine.connect() as conn:
            conn.execute(DropView(DEFAULT_SCHEMA, view, materialized=True))
            conn.commit()

    @pytest.mark.usefixtures("table_creator")
    def test_index(self, connection, sync, book_cls, data):
        """Test create/drop an index."""
        indices = sa.inspect(connection.engine).get_indexes(
            "book", schema=DEFAULT_SCHEMA
        )
        assert indices == []
        with connection.engine.connect() as conn:
            conn.execute(
                CreateIndex("my_index", DEFAULT_SCHEMA, "book", ["isbn"])
            )
            conn.commit()
        indices = sa.inspect(connection.engine).get_indexes(
            "book", schema=DEFAULT_SCHEMA
        )
        assert len(indices) == 1
        assert indices[0] == {
            "name": "my_index",
            "unique": True,
            "column_names": ["isbn"],
            "include_columns": [],
            "dialect_options": {
                "postgresql_include": [],
            },
        }
        with connection.engine.connect() as conn:
            conn.execute(DropIndex("my_index"))
            conn.commit()
        indices = sa.inspect(connection.engine).get_indexes(
            "book", schema=DEFAULT_SCHEMA
        )
        assert indices == []

    @pytest.mark.usefixtures("table_creator")
    def test_primary_keys(self, connection, book_cls):
        pg_base = Base(connection.engine.url.database)
        statement = _primary_keys(pg_base.models, DEFAULT_SCHEMA, ["book"])
        rows = connection.execute(statement).fetchall()
        assert rows == [("book", ["isbn"])]

    @pytest.mark.usefixtures("table_creator")
    def test_foreign_keys(self, connection, book_cls):
        pg_base = Base(connection.engine.url.database)
        statement = _foreign_keys(
            pg_base.models, DEFAULT_SCHEMA, ["book", "publisher"]
        )
        rows = connection.execute(statement).fetchall()
        assert rows[0][0] == "book"
        assert sorted(rows[0][1]) == sorted(
            ["publisher_id", "buyer_id", "seller_id"]
        )

    @pytest.mark.usefixtures("table_creator")
    def test__create_view(self, connection, book_cls):
        pg_base = Base(connection.engine.url.database)

        def fetchall(statement):
            return connection.execute(statement).fetchall()

        with patch("pgsync.view.logger") as mock_logger:
            create_view(
                connection.engine,
                pg_base.models,
                fetchall,
                "testdb",
                DEFAULT_SCHEMA,
                ["book", "publisher"],
                user_defined_fkey_tables={},
                views=[],
                node_columns={"book": ["node_id"]},
            )
            assert mock_logger.debug.call_count == 2
            assert mock_logger.debug.call_args_list == [
                call("Creating view: public._view"),
                call("Created view: public._view"),
            ]

        user_defined_fkey_tables = {"publisher": ["publisher_id"]}
        create_schema(connection.engine.url.database, "myschema")

        try:
            with patch("pgsync.view.logger") as mock_logger:
                create_view(
                    connection.engine,
                    pg_base.models,
                    fetchall,
                    "testdb",
                    "myschema",
                    set(["book", "publisher"]),
                    user_defined_fkey_tables=user_defined_fkey_tables,
                    views=[],
                    node_columns={"book": ["node_id"]},
                )
                assert mock_logger.debug.call_count == 2
                assert mock_logger.debug.call_args_list == [
                    call("Creating view: myschema._view"),
                    call("Created view: myschema._view"),
                ]
        finally:
            connection.execute(
                sa.text("DROP SCHEMA IF EXISTS myschema CASCADE")
            )
            connection.commit()

    @pytest.mark.usefixtures("table_creator")
    def test__create_view_replaces_existing_materialized_view_not_seen_in_cache(
        self, connection, book_cls
    ):
        pg_base = Base(connection.engine.url.database)

        def fetchall(statement):
            return connection.execute(statement).fetchall()

        create_view(
            connection.engine,
            pg_base.models,
            fetchall,
            "testdb",
            DEFAULT_SCHEMA,
            {"book", "publisher"},
            user_defined_fkey_tables={},
            views=[],
            node_columns={"book": ["node_id"]},
        )
        assert is_view(
            connection.engine,
            DEFAULT_SCHEMA,
            "_view",
            materialized=True,
        )

        create_view(
            connection.engine,
            pg_base.models,
            fetchall,
            "testdb",
            DEFAULT_SCHEMA,
            {"book", "publisher"},
            user_defined_fkey_tables={},
            views=[],
            node_columns={"book": ["node_id"]},
        )

        assert is_view(
            connection.engine,
            DEFAULT_SCHEMA,
            "_view",
            materialized=True,
        )

    @pytest.mark.usefixtures("table_creator")
    def test__create_view_postgresql_rollback_preserves_existing_view(
        self, connection, book_cls
    ):
        pg_base = Base(connection.engine.url.database)
        create_view(
            connection.engine,
            pg_base.models,
            pg_base.fetchall,
            "rollback-baseline",
            DEFAULT_SCHEMA,
            {"book", "publisher"},
            user_defined_fkey_tables={},
            views=[],
            node_columns={"book": ["node_id"]},
        )

        statement = sa.text(
            'SELECT * FROM "public"."_view" ORDER BY table_name'
        )
        with connection.engine.connect() as conn:
            before = conn.execute(statement).fetchall()

        invalid_index = sa.text(
            'CREATE UNIQUE INDEX "_idx" ON '
            '"public"."_view" ("missing_column")'
        )
        with (
            patch("pgsync.view.CreateIndex", return_value=invalid_index),
            pytest.raises(sa.exc.DBAPIError),
        ):
            create_view(
                connection.engine,
                pg_base.models,
                pg_base.fetchall,
                "rollback-new",
                DEFAULT_SCHEMA,
                {"book", "publisher"},
                user_defined_fkey_tables={},
                views=[],
                node_columns={"book": ["node_id"]},
            )

        with connection.engine.connect() as conn:
            after = conn.execute(statement).fetchall()
            index_exists = conn.execute(
                sa.text(
                    "SELECT EXISTS ("
                    "SELECT 1 FROM pg_indexes "
                    "WHERE schemaname = 'public' "
                    "AND tablename = '_view' "
                    "AND indexname = '_idx'"
                    ")"
                )
            ).scalar()

        assert after == before
        assert index_exists is True

    @pytest.mark.usefixtures("table_creator")
    def test__create_view_postgresql_replacement_never_exposes_missing_view(
        self, connection, book_cls
    ):
        pg_base = Base(connection.engine.url.database)
        create_view(
            connection.engine,
            pg_base.models,
            pg_base.fetchall,
            "concurrency-baseline",
            DEFAULT_SCHEMA,
            {"book", "publisher"},
            user_defined_fkey_tables={},
            views=[],
            node_columns={"book": ["node_id"]},
        )

        create_paused = threading.Event()
        release_create = threading.Event()
        observer_started = threading.Event()

        def pause_after_drop(
            conn,
            cursor,
            statement,
            parameters,
            context,
            executemany,
        ):
            if statement.startswith(
                'CREATE MATERIALIZED VIEW "public"."_view"'
            ):
                create_paused.set()
                assert release_create.wait(timeout=5)

        sa.event.listen(
            connection.engine,
            "before_cursor_execute",
            pause_after_drop,
        )

        def rebuild():
            create_view(
                connection.engine,
                pg_base.models,
                pg_base.fetchall,
                "concurrency-new",
                DEFAULT_SCHEMA,
                {"book", "publisher"},
                user_defined_fkey_tables={},
                views=[],
                node_columns={"book": ["node_id"]},
            )

        def observe():
            observer_started.set()
            with connection.engine.connect() as conn:
                return conn.execute(
                    sa.text('SELECT COUNT(*) FROM "public"."_view"')
                ).scalar()

        try:
            with ThreadPoolExecutor(max_workers=2) as executor:
                rebuild_future = executor.submit(rebuild)
                assert create_paused.wait(timeout=5)
                observer_future = executor.submit(observe)
                assert observer_started.wait(timeout=5)

                try:
                    with pytest.raises(FutureTimeoutError):
                        observer_future.result(timeout=0.25)
                finally:
                    release_create.set()
                rebuild_future.result(timeout=5)
                assert observer_future.result(timeout=5) > 0
        finally:
            release_create.set()
            sa.event.remove(
                connection.engine,
                "before_cursor_execute",
                pause_after_drop,
            )

    @pytest.mark.usefixtures("table_creator")
    def test__create_view_postgresql_concurrent_rebuilds_merge_both_indices(
        self, connection, book_cls
    ):
        pg_base = Base(connection.engine.url.database)
        create_view(
            connection.engine,
            pg_base.models,
            pg_base.fetchall,
            "concurrency-baseline",
            DEFAULT_SCHEMA,
            {"book", "publisher"},
            user_defined_fkey_tables={},
            views=[],
            node_columns={"book": ["node_id"]},
        )

        first_lock_acquired = threading.Event()
        release_first = threading.Event()
        listener_guard = threading.Lock()
        pause_first = {"value": True}

        def pause_after_advisory_lock(
            conn,
            cursor,
            statement,
            parameters,
            context,
            executemany,
        ):
            if not statement.startswith(
                "SELECT pg_advisory_xact_lock("
            ):
                return
            with listener_guard:
                should_pause = pause_first["value"]
                pause_first["value"] = False
            if should_pause:
                first_lock_acquired.set()
                assert release_first.wait(timeout=5)

        sa.event.listen(
            connection.engine,
            "after_cursor_execute",
            pause_after_advisory_lock,
        )

        def rebuild(index):
            create_view(
                connection.engine,
                pg_base.models,
                pg_base.fetchall,
                index,
                DEFAULT_SCHEMA,
                {"book", "publisher"},
                user_defined_fkey_tables={},
                views=[],
                node_columns={"book": ["node_id"]},
            )

        try:
            with ThreadPoolExecutor(max_workers=2) as executor:
                first = executor.submit(rebuild, "concurrent-index-a")
                assert first_lock_acquired.wait(timeout=5)
                second = executor.submit(rebuild, "concurrent-index-b")
                try:
                    with pytest.raises(FutureTimeoutError):
                        second.result(timeout=0.25)
                finally:
                    release_first.set()
                first.result(timeout=5)
                second.result(timeout=5)
        finally:
            release_first.set()
            sa.event.remove(
                connection.engine,
                "after_cursor_execute",
                pause_after_advisory_lock,
            )

        with connection.engine.connect() as conn:
            indices = conn.execute(
                sa.text(
                    'SELECT indices FROM "public"."_view" '
                    "WHERE table_name = 'book'"
                )
            ).scalar_one()

        assert {"concurrent-index-a", "concurrent-index-b"} <= set(indices)

    @pytest.mark.usefixtures("table_creator")
    def test__create_view_postgresql_advisory_lock_timeout_preserves_view(
        self, connection, book_cls
    ):
        pg_base = Base(connection.engine.url.database)
        create_view(
            connection.engine,
            pg_base.models,
            pg_base.fetchall,
            "lock-timeout-baseline",
            DEFAULT_SCHEMA,
            {"book", "publisher"},
            user_defined_fkey_tables={},
            views=[],
            node_columns={"book": ["node_id"]},
        )

        statement = sa.text(
            'SELECT * FROM "public"."_view" ORDER BY table_name'
        )
        with connection.engine.connect() as conn:
            before = conn.execute(statement).fetchall()

        blocker = connection.engine.connect()
        blocker_transaction = blocker.begin()
        blocker.execute(
            sa.text(
                "SELECT pg_advisory_xact_lock("
                "hashtext(:namespace), hashtext(:resource))"
            ),
            {
                "namespace": "pgsync",
                "resource": f"{DEFAULT_SCHEMA}._view",
            },
        )
        try:
            with (
                patch("pgsync.view.VIEW_LOCK_TIMEOUT", "100ms"),
                pytest.raises(sa.exc.DBAPIError),
            ):
                create_view(
                    connection.engine,
                    pg_base.models,
                    pg_base.fetchall,
                    "lock-timeout-new",
                    DEFAULT_SCHEMA,
                    {"book", "publisher"},
                    user_defined_fkey_tables={},
                    views=[],
                    node_columns={"book": ["node_id"]},
                )
        finally:
            blocker_transaction.rollback()
            blocker.close()

        with connection.engine.connect() as conn:
            after = conn.execute(statement).fetchall()

        assert after == before

    @pytest.mark.usefixtures("table_creator")
    def test__create_view_postgresql_replaces_plain_view(
        self, connection, book_cls
    ):
        pg_base = Base(connection.engine.url.database)
        has_materialized_view = is_view(
            connection.engine,
            DEFAULT_SCHEMA,
            "_view",
            materialized=True,
        )
        has_plain_view = is_view(
            connection.engine,
            DEFAULT_SCHEMA,
            "_view",
            materialized=False,
        )
        with connection.engine.begin() as conn:
            if has_materialized_view:
                conn.execute(
                    DropView(DEFAULT_SCHEMA, "_view", materialized=True)
                )
            elif has_plain_view:
                conn.execute(
                    DropView(DEFAULT_SCHEMA, "_view", materialized=False)
                )
            conn.execute(
                sa.text(
                    'CREATE VIEW "public"."_view" AS SELECT '
                    "'legacy'::text AS table_name, "
                    "ARRAY['id']::text[] AS primary_keys, "
                    "ARRAY[]::text[] AS foreign_keys, "
                    "ARRAY['legacy-index']::text[] AS indices, "
                    "ARRAY['id']::text[] AS columns, "
                    "ARRAY[]::text[] AS payload_columns"
                )
            )

        assert is_view(
            connection.engine,
            DEFAULT_SCHEMA,
            "_view",
            materialized=False,
        )

        create_view(
            connection.engine,
            pg_base.models,
            pg_base.fetchall,
            "plain-replacement",
            DEFAULT_SCHEMA,
            {"book", "publisher"},
            user_defined_fkey_tables={},
            views=[],
            node_columns={"book": ["node_id"]},
        )

        assert is_view(
            connection.engine,
            DEFAULT_SCHEMA,
            "_view",
            materialized=True,
        )
        assert not is_view(
            connection.engine,
            DEFAULT_SCHEMA,
            "_view",
            materialized=False,
        )
        with connection.engine.connect() as conn:
            indices = conn.execute(
                sa.text(
                    'SELECT indices FROM "public"."_view" '
                    "WHERE table_name = 'legacy'"
                )
            ).scalar_one()
        assert "legacy-index" in indices

    def test__create_view_collects_metadata_before_atomic_ddl(self):
        engine = MagicMock()
        transaction = MagicMock()
        conn = MagicMock()
        transaction.__enter__.return_value = conn
        events = []
        engine.begin.side_effect = (
            lambda: events.append("begin") or transaction
        )

        primary_keys = object()
        foreign_keys = object()

        fetchall = MagicMock()

        def execute(statement, *args):
            if statement is primary_keys:
                events.append("primary_keys")
                result = MagicMock()
                result.fetchall.return_value = [("book", ["id"])]
                return result
            if statement is foreign_keys:
                events.append("foreign_keys")
                result = MagicMock()
                result.fetchall.return_value = [
                    ("book", ["publisher_id"])
                ]
                return result
            if isinstance(statement, sa.sql.elements.TextClause):
                events.append(str(statement))
                return MagicMock()
            if isinstance(statement, sa.sql.Select):
                events.append("existing_view")
                result = MagicMock()
                result.fetchall.return_value = [
                    (
                        "existing",
                        ["id"],
                        [],
                        ["old-index"],
                        ["name"],
                        ["name"],
                    )
                ]
                return result
            events.append(type(statement).__name__)
            return MagicMock()

        conn.execute.side_effect = execute

        def view_exists(*args, **kwargs):
            events.append("view_exists")
            return True

        with (
            patch("pgsync.view._is_view", side_effect=view_exists),
            patch("pgsync.view._primary_keys", return_value=primary_keys),
            patch("pgsync.view._foreign_keys", return_value=foreign_keys),
        ):
            create_view(
                engine,
                MagicMock(),
                fetchall,
                "testdb",
                DEFAULT_SCHEMA,
                {"book"},
                user_defined_fkey_tables={},
                views=[],
                node_columns={"book": ["name"]},
                payload_columns={"book": ["name"]},
            )

        assert events == [
            "begin",
            "SET LOCAL lock_timeout = '5s'",
            (
                "SELECT pg_advisory_xact_lock("
                "hashtext(:namespace), hashtext(:resource))"
            ),
            "view_exists",
            "existing_view",
            "primary_keys",
            "foreign_keys",
            "DropView",
            "CreateView",
            "DropIndex",
            "CreateIndex",
        ]
        fetchall.assert_not_called()
        engine.begin.assert_called_once_with()
        transaction.__exit__.assert_called_once_with(None, None, None)

    def test__create_view_ddl_failure_leaves_transaction_to_rollback(self):
        engine = MagicMock()
        transaction = MagicMock()
        conn = MagicMock()
        transaction.__enter__.return_value = conn
        engine.begin.return_value = transaction

        primary_keys = object()
        foreign_keys = object()

        fetchall = MagicMock()

        def execute(statement, *args):
            if statement is primary_keys:
                result = MagicMock()
                result.fetchall.return_value = [("book", ["id"])]
                return result
            if statement is foreign_keys:
                result = MagicMock()
                result.fetchall.return_value = []
                return result
            if isinstance(statement, sa.sql.Select):
                result = MagicMock()
                result.fetchall.return_value = [
                    (
                        "existing",
                        ["id"],
                        [],
                        ["old-index"],
                        ["name"],
                        [],
                    )
                ]
                return result
            if isinstance(statement, CreateView):
                raise RuntimeError("create failed")
            return MagicMock()

        conn.execute.side_effect = execute

        with (
            patch("pgsync.view._is_view", side_effect=[False, True]),
            patch("pgsync.view._primary_keys", return_value=primary_keys),
            patch("pgsync.view._foreign_keys", return_value=foreign_keys),
            pytest.raises(RuntimeError, match="create failed"),
        ):
            create_view(
                engine,
                MagicMock(),
                fetchall,
                "testdb",
                DEFAULT_SCHEMA,
                {"book"},
                user_defined_fkey_tables={},
                views=[],
                node_columns={},
            )

        exit_args = transaction.__exit__.call_args.args
        assert exit_args[0] is RuntimeError
        assert str(exit_args[1]) == "create failed"
        assert exit_args[2] is not None
        fetchall.assert_not_called()
        drop_view = next(
            call_args.args[0]
            for call_args in conn.execute.call_args_list
            if isinstance(call_args.args[0], DropView)
        )
        assert drop_view.materialized is False
        assert not any(
            isinstance(call_args.args[0], (DropIndex, CreateIndex))
            for call_args in conn.execute.call_args_list
        )

    def test_create_view_ddl_compile(self, connection):
        """Test CreateView DDL compiles correctly."""
        ddl = CreateView(
            DEFAULT_SCHEMA, "test_view", sa.select(1), materialized=False
        )
        # Verify DDL object has expected attributes
        assert ddl.schema == DEFAULT_SCHEMA
        assert ddl.name == "test_view"
        assert ddl.materialized is False

    def test_create_view_ddl_compile_materialized(self, connection):
        """Test CreateView DDL compiles correctly for materialized views."""
        ddl = CreateView(
            DEFAULT_SCHEMA, "test_mat_view", sa.select(1), materialized=True
        )
        assert ddl.materialized is True

    def test_drop_view_ddl_compile(self, connection):
        """Test DropView DDL compiles correctly."""
        ddl = DropView(DEFAULT_SCHEMA, "test_view", materialized=False)
        assert ddl.schema == DEFAULT_SCHEMA
        assert ddl.name == "test_view"
        assert ddl.materialized is False

    def test_drop_view_ddl_compile_materialized(self, connection):
        """Test DropView DDL compiles correctly for materialized views."""
        ddl = DropView(DEFAULT_SCHEMA, "test_mat_view", materialized=True)
        assert ddl.materialized is True

    def test_refresh_view_ddl_compile(self, connection):
        """Test RefreshView DDL compiles correctly."""
        ddl = RefreshView(DEFAULT_SCHEMA, "test_view")
        assert ddl.schema == DEFAULT_SCHEMA
        assert ddl.name == "test_view"

    def test_refresh_view_concurrently(self, connection):
        """Test RefreshView with concurrently option."""
        ddl = RefreshView(DEFAULT_SCHEMA, "test_view", concurrently=True)
        assert ddl.concurrently is True

    def test_create_index_ddl(self, connection):
        """Test CreateIndex DDL object."""
        ddl = CreateIndex(
            "test_idx", DEFAULT_SCHEMA, "book", ["isbn", "title"]
        )
        assert ddl.name == "test_idx"
        assert ddl.schema == DEFAULT_SCHEMA

    def test_drop_index_ddl(self, connection):
        """Test DropIndex DDL object."""
        ddl = DropIndex("test_idx")
        assert ddl.name == "test_idx"

    def test_is_view_not_exists(self, connection):
        """Test is_view returns False for non-existent view."""
        result = is_view(
            connection.engine,
            DEFAULT_SCHEMA,
            "nonexistent_view",
            materialized=True,
        )
        assert result is False

        result = is_view(
            connection.engine,
            DEFAULT_SCHEMA,
            "nonexistent_view",
            materialized=False,
        )
        assert result is False


# ============================================================================
# PHASE 7 EXTENDED TESTS - View.py Comprehensive Coverage
# ============================================================================


@pytest.mark.skipif(
    IS_MYSQL_COMPAT,
    reason="Skipped because IS_MYSQL_COMPAT env var is set",
)
@pytest.mark.usefixtures("table_creator")
class TestViewCompilation:
    """Extended tests for view DDL compilation."""

    def test_compile_create_view_with_literal_binds(self, connection):
        """Test compile_create_view with literal_binds for query parameters."""
        element = CreateView(
            schema=DEFAULT_SCHEMA,
            name="test_view_literal",
            selectable=sa.select(sa.literal(1).label("num")),
            materialized=False,
        )
        ddl = element.compile(dialect=connection.engine.dialect).string

        assert "CREATE" in ddl
        assert "VIEW" in ddl
        assert "test_view_literal" in ddl

    def test_compile_create_view_materialized_with_literal_binds(
        self, connection
    ):
        """Test compile_create_view for materialized view with literal_binds."""
        element = CreateView(
            schema=DEFAULT_SCHEMA,
            name="test_mat_view_literal",
            selectable=sa.select(sa.literal(1).label("num")),
            materialized=True,
        )
        ddl = element.compile(dialect=connection.engine.dialect).string

        assert "CREATE MATERIALIZED VIEW" in ddl
        assert "test_mat_view_literal" in ddl

    def test_compile_create_view_with_complex_query(
        self, connection, book_cls
    ):
        """Test compile_create_view with complex SELECT query."""
        book_table = book_cls.__table__
        query = sa.select(
            book_table.c.isbn,
            book_table.c.title,
        ).where(book_table.c.isbn.isnot(None))

        element = CreateView(
            schema=DEFAULT_SCHEMA,
            name="test_complex_view",
            selectable=query,
            materialized=False,
        )
        ddl = element.compile(dialect=connection.engine.dialect).string

        assert "WHERE" in ddl or "where" in ddl.lower()

    def test_compile_drop_view_non_materialized(self, connection):
        """Test compile_drop_view for regular views."""
        element = DropView(
            schema=DEFAULT_SCHEMA,
            name="test_drop_view",
            materialized=False,
        )
        ddl = element.compile(dialect=connection.engine.dialect).string

        assert "DROP" in ddl
        assert "VIEW" in ddl
        assert "MATERIALIZED" not in ddl

    def test_compile_drop_view_materialized(self, connection):
        """Test compile_drop_view for materialized views."""
        element = DropView(
            schema=DEFAULT_SCHEMA,
            name="test_drop_mat_view",
            materialized=True,
        )
        ddl = element.compile(dialect=connection.engine.dialect).string

        assert "DROP MATERIALIZED VIEW" in ddl

    def test_compile_refresh_view_concurrently_true(self, connection):
        """Test compile_refresh_view with CONCURRENTLY flag."""
        element = RefreshView(
            schema=DEFAULT_SCHEMA,
            name="test_refresh_view",
            concurrently=True,
        )
        ddl = element.compile(dialect=connection.engine.dialect).string

        assert "CONCURRENTLY" in ddl
        assert "REFRESH MATERIALIZED VIEW" in ddl

    def test_compile_refresh_view_concurrently_false(self, connection):
        """Test compile_refresh_view without CONCURRENTLY flag."""
        element = RefreshView(
            schema=DEFAULT_SCHEMA,
            name="test_refresh_view",
            concurrently=False,
        )
        ddl = element.compile(dialect=connection.engine.dialect).string

        assert "CONCURRENTLY" not in ddl
        assert "REFRESH MATERIALIZED VIEW" in ddl


@pytest.mark.skipif(
    IS_MYSQL_COMPAT,
    reason="Skipped because IS_MYSQL_COMPAT env var is set",
)
@pytest.mark.usefixtures("table_creator")
class TestViewConstraints:
    """Extended tests for constraint queries."""

    def test_get_constraints_with_schema(self, connection):
        """Test _get_constraints returns a valid Select query."""
        from pgsync.view import _get_constraints

        pg_base = Base(connection.engine.url.database)
        query = _get_constraints(
            pg_base.models,
            DEFAULT_SCHEMA,
            {"book"},
            "primary_keys",
            "PRIMARY KEY",
        )
        assert isinstance(query, sa.sql.Select)

    def test_get_constraints_primary_keys(self, connection):
        """Test _primary_keys returns a valid Select query."""
        from pgsync.view import _primary_keys

        pg_base = Base(connection.engine.url.database)
        query = _primary_keys(
            pg_base.models,
            DEFAULT_SCHEMA,
            {"book"},
        )
        assert isinstance(query, sa.sql.Select)

    def test_get_constraints_foreign_keys(self, connection):
        """Test _foreign_keys returns a valid Select query."""
        pg_base = Base(connection.engine.url.database)
        query = _foreign_keys(
            pg_base.models,
            DEFAULT_SCHEMA,
            {"book"},
        )
        assert isinstance(query, sa.sql.Select)

    def test_get_constraints_unique_constraints(self, connection):
        """Test _get_constraints with UNIQUE constraint type."""
        from pgsync.view import _get_constraints

        pg_base = Base(connection.engine.url.database)
        query = _get_constraints(
            pg_base.models,
            DEFAULT_SCHEMA,
            {"book"},
            "unique_keys",
            "UNIQUE",
        )
        assert isinstance(query, sa.sql.Select)


@pytest.mark.skipif(
    IS_MYSQL_COMPAT,
    reason="Skipped because IS_MYSQL_COMPAT env var is set",
)
@pytest.mark.usefixtures("table_creator")
class TestViewNonDefaultSchema:
    """Tests for views with non-default schemas."""

    def test_create_view_custom_schema(self, connection):
        """Test creating view in non-default schema."""
        custom_schema = "custom_schema"
        element = CreateView(
            schema=custom_schema,
            name="test_custom_view",
            selectable=sa.select(sa.literal(1).label("num")),
            materialized=False,
        )
        ddl = element.compile(dialect=connection.engine.dialect).string

        assert custom_schema in ddl
        assert "test_custom_view" in ddl

    def test_drop_view_custom_schema(self, connection):
        """Test dropping view from non-default schema."""
        custom_schema = "custom_schema"
        element = DropView(
            schema=custom_schema,
            name="test_custom_view",
            materialized=False,
        )
        ddl = element.compile(dialect=connection.engine.dialect).string

        assert custom_schema in ddl

    def test_refresh_view_custom_schema(self, connection):
        """Test refreshing materialized view in non-default schema."""
        custom_schema = "custom_schema"
        element = RefreshView(
            schema=custom_schema,
            name="test_custom_mat_view",
            concurrently=False,
        )
        ddl = element.compile(dialect=connection.engine.dialect).string

        assert custom_schema in ddl


@pytest.mark.skipif(
    IS_MYSQL_COMPAT,
    reason="Skipped because IS_MYSQL_COMPAT env var is set",
)
@pytest.mark.usefixtures("table_creator")
class TestViewHelpers:
    """Extended tests for view helper methods."""

    def test_view_initialization(self):
        """Test CreateView object initialization."""
        view = CreateView(
            name="test_view",
            schema="public",
            selectable=sa.select(sa.literal(1)),
        )

        assert view.name == "test_view"
        assert view.schema == "public"

    def test_view_with_custom_properties(self):
        """Test CreateView with additional properties."""
        view = CreateView(
            name="test_view",
            schema="public",
            selectable=sa.select(sa.literal(1)),
            materialized=True,
        )

        assert hasattr(view, "materialized")
        assert view.materialized is True

    def test_view_name_property(self, connection):
        """Test CreateView name property."""
        view = CreateView(
            name="test_view",
            schema="public",
            selectable=sa.select(sa.literal(1)),
        )

        assert view.name == "test_view"

    def test_view_schema_property(self, connection):
        """Test CreateView schema property."""
        view = CreateView(
            name="test_view",
            schema="custom_schema",
            selectable=sa.select(sa.literal(1)),
        )

        assert view.schema == "custom_schema"

    def test_is_view_returns_false_for_tables(self, connection):
        """Test is_view returns False for regular tables."""
        from pgsync.view import is_view

        # book is a table, not a view
        result = is_view(
            connection.engine,
            DEFAULT_SCHEMA,
            "book",
            materialized=False,
        )

        assert result is False
