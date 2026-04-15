CREATE TABLE node (
    node_id              VARCHAR(256) PRIMARY KEY,
    addr                 VARCHAR(256) NOT NULL,
    heartbeat_interval_ms INT NOT NULL DEFAULT 5000,
    last_heartbeat       DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3),
    state                TINYINT NOT NULL DEFAULT 1
);
