CREATE TABLE ops.source_poll_counts (
    bucket_start timestamptz NOT NULL,
    source_mode text NOT NULL,
    feed_kind text NOT NULL,
    successful bigint NOT NULL CHECK (successful >= 0),
    failed bigint NOT NULL CHECK (failed >= 0),
    PRIMARY KEY (bucket_start, source_mode, feed_kind)
);

CREATE FUNCTION ops.count_source_poll(finished_at timestamptz, mode_at_poll text, kind_at_poll text, ok boolean)
RETURNS void LANGUAGE sql SET lock_timeout = '500ms' AS $$
    INSERT INTO ops.source_poll_counts AS c (bucket_start, source_mode, feed_kind, successful, failed)
    VALUES (date_bin('5 minutes', finished_at, 'epoch'), mode_at_poll, kind_at_poll, ok::int, (NOT ok)::int)
    ON CONFLICT (bucket_start, source_mode, feed_kind) DO UPDATE SET
        successful = c.successful + EXCLUDED.successful,
        failed = c.failed + EXCLUDED.failed
$$;
