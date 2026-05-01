-- Drop adaptive extent capacity columns; design moves to fixed extent_capacity
-- per ExtentNodeConfig; per-stream capacity tuning is obsolete after the
-- shared-arena refactor (see docs/superpowers/specs/2026-04-24-shared-arena-design.md).
ALTER TABLE stream
    DROP COLUMN min_extent_capacity,
    DROP COLUMN max_extent_capacity,
    DROP COLUMN extent_growth_factor;
