-- Rename `extent` table to `stream_epochs` per shared-arena spec P1.
-- Column schema unchanged; only the table identifier changes.
ALTER TABLE extent RENAME TO stream_epochs;
