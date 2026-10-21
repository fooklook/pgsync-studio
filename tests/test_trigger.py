"""Trigger tests."""

import pytest
import sqlalchemy as sa

from pgsync.base import Base
from pgsync.settings import IS_MYSQL_COMPAT
from pgsync.trigger import CREATE_TRIGGER_TEMPLATE


def test_trigger_template_contains_durable_outbox_write():
    assert "CREATE OR REPLACE FUNCTION _pgsync_table_notify()" in CREATE_TRIGGER_TEMPLATE
    assert "notification = JSON_BUILD_OBJECT" in CREATE_TRIGGER_TEMPLATE
    assert "TO_REGCLASS(TG_TABLE_SCHEMA || '._pgsync_change_event')" in CREATE_TRIGGER_TEMPLATE
    assert "INSERT INTO %I._pgsync_change_event" in CREATE_TRIGGER_TEMPLATE
    assert "event_xmin BIGINT" in CREATE_TRIGGER_TEMPLATE
    assert "tg_op, event_xmin, payload" in CREATE_TRIGGER_TEMPLATE
    assert " xmin BIGINT" not in CREATE_TRIGGER_TEMPLATE
    assert "PERFORM PG_NOTIFY(channel, notification::TEXT)" in CREATE_TRIGGER_TEMPLATE
    assert "EXCEPTION WHEN OTHERS" in CREATE_TRIGGER_TEMPLATE
    assert "pgsync durable outbox write failed open" in CREATE_TRIGGER_TEMPLATE
    assert "pgsync notify failed open" in CREATE_TRIGGER_TEMPLATE
    assert "pgsync trigger failed open" in CREATE_TRIGGER_TEMPLATE


@pytest.mark.skipif(
    IS_MYSQL_COMPAT,
    reason="Skipped because IS_MYSQL_COMPAT env var is set",
)
@pytest.mark.usefixtures("table_creator")
class TestTrigger(object):
    """Trigger tests."""

    def test_trigger_template(self):
        assert "CREATE OR REPLACE FUNCTION _pgsync_table_notify()" in CREATE_TRIGGER_TEMPLATE

    def test_trigger_primary_key_function(self, connection):
        tables = {
            "book": ["isbn"],
            "publisher": ["id"],
            "book_language": ["id"],
            "author": ["id"],
            "language": ["id"],
            "subject": ["id"],
            "city": ["id"],
            "country": ["id"],
            "continent": ["id"],
        }
        pg_base = Base(connection.engine.url.database)
        for table_name, primary_keys in tables.items():
            query = (
                f"SELECT ARRAY_AGG(attname) "
                f"FROM pg_index "
                f"JOIN pg_attribute ON attrelid = indrelid AND attnum = ANY(indkey) "  # noqa E501
                f"WHERE indrelid = '{table_name}'::regclass AND indisprimary"
            )
            rows = pg_base.fetchall(sa.text(query))[0]
            assert list(rows)[0] == primary_keys

    def test_trigger_foreign_key_function(self, connection):
        tables = {
            "book": ["publisher_id", "buyer_id", "seller_id"],
            "publisher": None,
            "book_language": ["book_isbn", "language_id"],
            "author": ["city_id"],
            "language": None,
            "subject": None,
            "city": ["country_id"],
            "country": ["continent_id"],
            "continent": None,
        }
        pg_base = Base(connection.engine.url.database)
        for table_name, foreign_keys in tables.items():
            query = (
                f"SELECT ARRAY_AGG(column_name::TEXT) FROM information_schema.key_column_usage "  # noqa E501
                f"WHERE constraint_catalog=current_catalog AND "
                f"table_name='{table_name}' AND position_in_unique_constraint NOTNULL "  # noqa E501
            )
            rows = pg_base.fetchall(sa.text(query))[0]
            if rows[0]:
                assert sorted(rows[0]) == sorted(foreign_keys)
            else:
                assert rows[0] == foreign_keys
