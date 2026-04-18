CREATE TABLE stream (
    stream_id           INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    stream_name         VARCHAR(512) NOT NULL UNIQUE,
    stream_type         VARCHAR(32) NOT NULL DEFAULT 'DATA',
    replication_factor  TINYINT UNSIGNED NOT NULL DEFAULT 2,
    extent_capacity     INT NOT NULL DEFAULT 67108864,
    min_extent_capacity INT NOT NULL DEFAULT 8388608,
    max_extent_capacity INT NOT NULL DEFAULT 268435456,
    extent_growth_factor TINYINT UNSIGNED NOT NULL DEFAULT 2,
    cache_extents       SMALLINT UNSIGNED NOT NULL DEFAULT 4,
    storage_class       TINYINT UNSIGNED NOT NULL DEFAULT 0,
    epoch               INT NOT NULL DEFAULT 0,    
    created_at          DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3),
    updated_at          DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
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
