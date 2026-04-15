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
