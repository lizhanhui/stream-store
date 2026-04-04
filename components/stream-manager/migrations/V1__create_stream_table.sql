CREATE TABLE stream (
    stream_id           BIGINT PRIMARY KEY AUTO_INCREMENT,
    stream_name         VARCHAR(512) NOT NULL UNIQUE,
    stream_type         VARCHAR(32) NOT NULL DEFAULT 'DATA',
    replication_factor  SMALLINT NOT NULL DEFAULT 2,
    extent_capacity     INT NOT NULL DEFAULT 67108864,
    epoch               INT NOT NULL DEFAULT 0,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
