CREATE TABLE extent (
    stream_id     BIGINT NOT NULL,
    extent_id     INT NOT NULL,
    start_offset  BIGINT NOT NULL,
    end_offset    BIGINT NOT NULL DEFAULT 0,
    state         TINYINT NOT NULL DEFAULT 1,
    s3_key        VARCHAR(1024),
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    sealed_at     TIMESTAMP NULL,
    flushed_at    TIMESTAMP NULL,
    PRIMARY KEY (stream_id, extent_id),
    INDEX idx_stream_state (stream_id, state)
);

CREATE TABLE extent_replica (
    stream_id     BIGINT NOT NULL,
    extent_id     INT NOT NULL,
    node_addr     VARCHAR(256) NOT NULL,
    role          TINYINT NOT NULL DEFAULT 0,
    PRIMARY KEY (stream_id, extent_id, node_addr),
    INDEX idx_node (node_addr)
);

CREATE TABLE stream_sequence (
    stream_id       BIGINT PRIMARY KEY,
    next_extent_id  INT NOT NULL DEFAULT 0
);
