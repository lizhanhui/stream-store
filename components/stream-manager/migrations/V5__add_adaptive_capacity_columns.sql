-- Add adaptive extent capacity columns to stream table
-- min_extent_capacity: minimum extent size (default 8 MiB)
-- max_extent_capacity: maximum extent size (default 256 MiB)
-- For existing streams, set both to current extent_capacity for backward compatibility

ALTER TABLE stream
ADD COLUMN min_extent_capacity INT NOT NULL DEFAULT 8388608,
ADD COLUMN max_extent_capacity INT NOT NULL DEFAULT 268435456;

-- Initialize new streams to have min=8MiB and max=256MiB by default
-- Existing streams keep current extent_capacity logic
