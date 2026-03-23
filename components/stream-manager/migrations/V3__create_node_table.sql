CREATE TABLE node (
    node_id              VARCHAR(256) PRIMARY KEY,
    addr                 VARCHAR(256) NOT NULL,
    heartbeat_interval_ms INT NOT NULL DEFAULT 5000,
    last_heartbeat       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    state                TINYINT NOT NULL DEFAULT 1
);
