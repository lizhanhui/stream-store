CREATE TABLE extent (
    stream_id     INT UNSIGNED NOT NULL,
    epoch         INT NOT NULL DEFAULT 0,    
    extent_id     INT NOT NULL,
    start_offset  BIGINT NOT NULL,
    end_offset    BIGINT NOT NULL DEFAULT 0,
    state         TINYINT NOT NULL DEFAULT 1,
    s3_key        VARCHAR(1024),
    created_at    DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3),
    sealed_at     DATETIME(3) NULL,
    flushed_at    DATETIME(3) NULL,
    PRIMARY KEY (stream_id, extent_id),
    INDEX idx_stream_state (stream_id, state)
);

CREATE TABLE stream_sequence (
    stream_id       BIGINT PRIMARY KEY,
    next_extent_id  INT NOT NULL DEFAULT 0,
    created_at      DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3),
    updated_at      DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
);
