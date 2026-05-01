-- Add ArenaClass to stream and stream_epochs.
-- 0 = Dedicated (default, today's fast path), 1 = Shared (future: many streams
-- per arena). Captured on the stream row as the declared policy and on each
-- stream_epochs row as the class snapshot at epoch allocation time so runtime
-- class transitions (added in a later plan) do not rewrite history.
ALTER TABLE stream
    ADD COLUMN arena_class TINYINT UNSIGNED NOT NULL DEFAULT 0;
ALTER TABLE stream_epochs
    ADD COLUMN arena_class TINYINT UNSIGNED NOT NULL DEFAULT 0;
