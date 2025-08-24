-- Create database if it doesn't exist (already created by environment variable)
-- CREATE DATABASE IF NOT EXISTS edge;

-- Create users if they don't exist (edge user is auto-created by environment variable)
CREATE USER IF NOT EXISTS 'edge'@'%' IDENTIFIED BY 'edgepass';
GRANT ALL ON edge.* TO 'edge'@'%';

CREATE USER IF NOT EXISTS 'reader'@'%' IDENTIFIED BY 'readpass';
GRANT SELECT ON edge.* TO 'reader'@'%';

USE edge;

/* ======= Global mirror ======= */
CREATE TABLE IF NOT EXISTS global_mirror_versions (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  dataset_id VARCHAR(255) NOT NULL,
  version     VARCHAR(255) NOT NULL,
  checksum    VARCHAR(64)  NOT NULL,
  ts          BIGINT       NOT NULL,
  UNIQUE KEY uniq_dataset_version (dataset_id, version)
);

CREATE TABLE IF NOT EXISTS global_rows (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  dataset_id VARCHAR(255) NOT NULL,
  version     VARCHAR(255) NOT NULL,
  item        JSON         NOT NULL,
  KEY idx_dataset_version (dataset_id, version)
);

CREATE TABLE IF NOT EXISTS user_contexts (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  user_id    VARCHAR(255) NOT NULL,
  dataset_id VARCHAR(255) NOT NULL,
  ctx        JSON         NOT NULL,
  ts         BIGINT       NOT NULL,
  UNIQUE KEY uniq_user_dataset (user_id, dataset_id)
);

CREATE TABLE IF NOT EXISTS user_views (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  user_id    VARCHAR(255) NOT NULL,
  dataset_id VARCHAR(255) NOT NULL,
  version     VARCHAR(255) NOT NULL,
  item        JSON         NOT NULL,
  ts          BIGINT       NOT NULL,
  KEY idx_user_dataset_version (user_id, dataset_id, version)
);

/* ======= UserDB metadata ======= */
CREATE TABLE IF NOT EXISTS userdb_tables (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  user_id    VARCHAR(255) NOT NULL,
  table_name VARCHAR(255) NOT NULL,
  phy_table  VARCHAR(64)  NOT NULL,
  pk_path    VARCHAR(255) NOT NULL,
  ts_path    VARCHAR(255) NOT NULL DEFAULT '$.updated_at',
  created_at BIGINT NOT NULL,
  UNIQUE KEY uniq_user_tbl (user_id, table_name)
);

CREATE TABLE IF NOT EXISTS userdb_table_indexes (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  user_id    VARCHAR(255) NOT NULL,
  table_name VARCHAR(255) NOT NULL,
  col_name   VARCHAR(64)  NOT NULL,
  json_path  VARCHAR(255) NOT NULL,
  col_type   ENUM('string','number','integer','datetime','boolean') NOT NULL DEFAULT 'string',
  UNIQUE KEY uniq_idx (user_id, table_name, col_name)
);

-- Create a simple completion marker
CREATE TABLE IF NOT EXISTS init_complete (
  id INT PRIMARY KEY DEFAULT 1,
  completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert completion marker
INSERT INTO init_complete (id) VALUES (1);
