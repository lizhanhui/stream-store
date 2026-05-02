CREATE TABLE stream (
    stream_id           INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    stream_name         VARCHAR(512) NOT NULL UNIQUE,
    replication_factor  TINYINT UNSIGNED NOT NULL DEFAULT 2,
    cache_epochs        SMALLINT UNSIGNED NOT NULL DEFAULT 4,
    storage_class       TINYINT UNSIGNED NOT NULL DEFAULT 0,
    arena_class         TINYINT UNSIGNED NOT NULL DEFAULT 0,
    epoch               INT NOT NULL DEFAULT 0,
    created_at          DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3),
    updated_at          DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
);

CREATE TABLE stream_epochs (
    stream_id     INT UNSIGNED NOT NULL,
    epoch         INT NOT NULL,
    start_offset  BIGINT NOT NULL,
    end_offset    BIGINT NOT NULL DEFAULT 0,
    state         TINYINT NOT NULL DEFAULT 1,
    arena_class   TINYINT UNSIGNED NOT NULL DEFAULT 0,
    created_at    DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3),
    sealed_at     DATETIME(3) NULL,
    flushed_at    DATETIME(3) NULL,
    PRIMARY KEY (stream_id, epoch),
    INDEX idx_stream_state (stream_id, state)
);

CREATE TABLE stream_replica (
    stream_id     INT UNSIGNED NOT NULL,
    epoch         INT NOT NULL,
    node_addr     VARCHAR(256) NOT NULL,
    role          TINYINT NOT NULL DEFAULT 0,
    created_at    DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3),
    updated_at    DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (stream_id, epoch, node_addr),
    INDEX idx_node (node_addr)
);

CREATE TABLE stream_epoch_s3 (
    id            BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    stream_id     INT UNSIGNED NOT NULL,
    epoch         INT NOT NULL,
    start_offset  BIGINT NOT NULL,
    end_offset    BIGINT NOT NULL,
    s3_key        VARCHAR(1024) NOT NULL,
    created_at    DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3),
    UNIQUE KEY uq_stream_epoch_s3_range (stream_id, epoch, start_offset, end_offset),
    INDEX idx_stream_epoch_s3_epoch (stream_id, epoch),
    INDEX idx_stream_epoch_s3_key (s3_key(255))
);

CREATE TABLE node (
    node_id              VARCHAR(256) PRIMARY KEY,
    addr                 VARCHAR(256) NOT NULL,
    heartbeat_interval_ms INT NOT NULL DEFAULT 5000,
    last_heartbeat       DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3),
    state                TINYINT NOT NULL DEFAULT 1
);

CREATE TABLE node_metrics (
  node_id                VARCHAR(256) PRIMARY KEY,
  available_memory_bytes BIGINT UNSIGNED NOT NULL DEFAULT 0,
  total_memory_bytes     BIGINT UNSIGNED NOT NULL DEFAULT 0,
  appends_per_sec        INT UNSIGNED NOT NULL DEFAULT 0,
  active_extent_count    INT UNSIGNED NOT NULL DEFAULT 0,
  bytes_written_per_sec  BIGINT UNSIGNED NOT NULL DEFAULT 0,
  updated_at             DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
);

CREATE TABLE stream_manager_leadership (
  id          INT PRIMARY KEY DEFAULT 1,
  node_id     VARCHAR(256) NOT NULL DEFAULT '',
  lease_until DATETIME NOT NULL DEFAULT '2000-01-01 00:00:00'
);

INSERT INTO stream_manager_leadership (id, node_id, lease_until)
  VALUES (1, '', '2000-01-01 00:00:00');
