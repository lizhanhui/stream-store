CREATE TABLE stream (
    stream_id           BIGINT PRIMARY KEY AUTO_INCREMENT,
    stream_name         VARCHAR(512) NOT NULL UNIQUE,
    stream_type         VARCHAR(32) NOT NULL DEFAULT 'DATA',
    replication_factor  SMALLINT NOT NULL DEFAULT 2,
    extent_capacity     INT NOT NULL DEFAULT 67108864,
    min_extent_capacity INT NOT NULL DEFAULT 8388608,
    max_extent_capacity INT NOT NULL DEFAULT 268435456,
    extent_growth_factor INT NOT NULL DEFAULT 2,
    cache_extents       INT NOT NULL DEFAULT 4,
    epoch               INT NOT NULL DEFAULT 0,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
