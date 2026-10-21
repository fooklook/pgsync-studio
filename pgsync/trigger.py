"""PGSync trigger template.

This module contains the template for a PostgreSQL trigger function that sends
change notifications asynchronously via PG_NOTIFY.

The trigger constructs a JSON notification containing:
- The table name and schema
- The operation type (INSERT, UPDATE, DELETE, TRUNCATE)
- The old and new row data (primary keys, foreign keys, and payload fields)
- The associated Elasticsearch/OpenSearch indices
"""

from .constants import MATERIALIZED_VIEW, TRIGGER_FUNC

TABLE_NOTIFY_VERSION = "2026-06-23-1"
TABLE_NOTIFY_COMMENT_PREFIX = "pgsync-table-notify:"
TABLE_NOTIFY_COMMENT = (
    f"{TABLE_NOTIFY_COMMENT_PREFIX}{TABLE_NOTIFY_VERSION}"
)

CHANGE_EVENT_TABLE = "_pgsync_change_event"

CREATE_TRIGGER_TEMPLATE = f"""
CREATE OR REPLACE FUNCTION {TRIGGER_FUNC}() RETURNS TRIGGER AS $$
DECLARE
  channel TEXT;
  old_row JSON;
  new_row JSON;
  notification JSON;
  event_xmin BIGINT;
  _indices TEXT [];
  _primary_keys TEXT [];
  _foreign_keys TEXT [];
  _columns TEXT [];
  _payload_columns TEXT [];
  _payload_keys TEXT [];
  _changed BOOLEAN;

BEGIN
    -- database is also the channel name.
    channel := CURRENT_DATABASE();

    IF TG_OP = 'DELETE' THEN

        SELECT primary_keys, foreign_keys, indices, payload_columns
        INTO _primary_keys, _foreign_keys, _indices, _payload_columns
        FROM {MATERIALIZED_VIEW}
        WHERE table_name = TG_TABLE_NAME;

        _primary_keys := COALESCE(_primary_keys, ARRAY[]::TEXT[]);
        _foreign_keys := COALESCE(_foreign_keys, ARRAY[]::TEXT[]);
        _payload_columns := COALESCE(_payload_columns, ARRAY[]::TEXT[]);
        _payload_keys := ARRAY(
            SELECT DISTINCT key
            FROM UNNEST(_primary_keys || _foreign_keys || _payload_columns) AS key
        );

        old_row = ROW_TO_JSON(OLD);
        old_row := (
            SELECT JSONB_OBJECT_AGG(key, value)
            FROM JSON_EACH(old_row)
            WHERE key = ANY(_payload_keys)
        );
        event_xmin := OLD.xmin;
    ELSE
        IF TG_OP <> 'TRUNCATE' THEN

            SELECT primary_keys, foreign_keys, indices, columns, payload_columns
            INTO _primary_keys, _foreign_keys, _indices, _columns, _payload_columns
            FROM {MATERIALIZED_VIEW}
            WHERE table_name = TG_TABLE_NAME;

            -- normalize null to empty arrays
            _primary_keys := COALESCE(_primary_keys, ARRAY[]::TEXT[]);
            _foreign_keys := COALESCE(_foreign_keys, ARRAY[]::TEXT[]);
            _columns := COALESCE(_columns, ARRAY[]::TEXT[]);
            _payload_columns := COALESCE(_payload_columns, ARRAY[]::TEXT[]);
            _payload_keys := ARRAY(
                SELECT DISTINCT key
                FROM UNNEST(_primary_keys || _foreign_keys || _payload_columns) AS key
            );

            -- Only react if any _columns actually changed
            IF TG_OP = 'UPDATE' THEN
                SELECT EXISTS (
                    SELECT 1
                    FROM JSONB_EACH(TO_JSONB(NEW.*)) n
                    JOIN JSONB_EACH(TO_JSONB(OLD.*)) o USING (key)
                    WHERE n.key = ANY(_columns)
                    AND n.value IS DISTINCT FROM o.value
                )
                INTO _changed;

                IF NOT _changed THEN
                    RETURN NEW;  -- skip notification; nothing relevant changed
                END IF;
            END IF;

            new_row = ROW_TO_JSON(NEW);
            new_row := (
                SELECT JSONB_OBJECT_AGG(key, value)
                FROM JSON_EACH(new_row)
                WHERE key = ANY(_payload_keys)
            );
            IF TG_OP = 'UPDATE' THEN
                old_row = ROW_TO_JSON(OLD);
                old_row := (
                    SELECT JSONB_OBJECT_AGG(key, value)
                    FROM JSON_EACH(old_row)
                    WHERE key = ANY(_payload_keys)
                );
            END IF;
            event_xmin := NEW.xmin;
        END IF;
    END IF;

    -- construct the notification as a JSON object.
    notification = JSON_BUILD_OBJECT(
        'xmin', event_xmin,
        'new', new_row,
        'old', old_row,
        'indices', _indices,
        'tg_op', TG_OP,
        'table', TG_TABLE_NAME,
        'schema', TG_TABLE_SCHEMA
    );

    BEGIN
        IF TO_REGCLASS(TG_TABLE_SCHEMA || '.{CHANGE_EVENT_TABLE}') IS NOT NULL THEN
            EXECUTE FORMAT(
                'INSERT INTO %I.{CHANGE_EVENT_TABLE} (channel, schema_name, table_name, tg_op, event_xmin, payload) VALUES ($1, $2, $3, $4, $5, $6)',
                TG_TABLE_SCHEMA
            )
            USING channel, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP, event_xmin, notification::JSONB;
        END IF;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'pgsync durable outbox write failed open for %.% %: [%] %',
            TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP, SQLSTATE, SQLERRM;
    END;

    -- Notify/Listen updates occur asynchronously,
    -- so this doesn't block the Postgres trigger procedure.
    BEGIN
        PERFORM PG_NOTIFY(channel, notification::TEXT);
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'pgsync notify failed open for %.% %: [%] %',
            TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP, SQLSTATE, SQLERRM;
    END;

  RETURN NEW;
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'pgsync trigger failed open for %.% %: [%] %',
        TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP, SQLSTATE, SQLERRM;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
"""
