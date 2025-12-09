-- 流式计算实践数据库初始化脚本
CREATE DATABASE IF NOT EXISTS streaming_db;
USE streaming_db;

-- 用户行为结果表
CREATE TABLE IF NOT EXISTS user_behavior_stats (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    event_type VARCHAR(20) NOT NULL,
    event_count BIGINT DEFAULT 0,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    process_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_event (user_id, event_type),
    INDEX idx_window (window_start)
);

-- 风控检测结果表
CREATE TABLE IF NOT EXISTS fraud_detection_results (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    transaction_id VARCHAR(50) NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    amount DECIMAL(15,2) NOT NULL,
    risk_score DECIMAL(5,2) DEFAULT 0,
    risk_reason VARCHAR(200),
    detection_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'DETECTED',
    INDEX idx_user_risk (user_id, risk_score),
    INDEX idx_transaction (transaction_id)
);

-- IoT 设备监控表
CREATE TABLE IF NOT EXISTS iot_device_monitoring (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    device_id VARCHAR(50) NOT NULL,
    metric_name VARCHAR(50) NOT NULL,
    metric_value DOUBLE NOT NULL,
    threshold DOUBLE NOT NULL,
    alert_level VARCHAR(20) DEFAULT 'NORMAL',
    timestamp TIMESTAMP NOT NULL,
    process_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_device_metric (device_id, metric_name),
    INDEX idx_timestamp (timestamp)
);

-- 创建流式计算用户
CREATE USER IF NOT EXISTS 'streaming_user'@'%' IDENTIFIED BY 'streaming123';
GRANT ALL PRIVILEGES ON streaming_db.* TO 'streaming_user'@'%';
FLUSH PRIVILEGES;
