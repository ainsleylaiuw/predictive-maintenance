USE pmdb;

CREATE TABLE telemetry_with_model AS
SELECT t.*, m.model
FROM PdM_telemetry t
JOIN PdM_machines m ON t.machineID = m.machineID;

ALTER TABLE telemetry_with_model ADD COLUMN failure_next_24h INT DEFAULT 0;
ALTER TABLE telemetry_with_model ADD COLUMN failure_comp1_next_24h INT DEFAULT 0;
ALTER TABLE telemetry_with_model ADD COLUMN failure_comp2_next_24h INT DEFAULT 0;
ALTER TABLE telemetry_with_model ADD COLUMN failure_comp3_next_24h INT DEFAULT 0;
ALTER TABLE telemetry_with_model ADD COLUMN failure_comp4_next_24h INT DEFAULT 0;

-- Any failure in next 24h
UPDATE telemetry_with_model t
SET failure_next_24h = 1
WHERE EXISTS (
    SELECT 1 FROM PdM_failures f
    WHERE f.machineID = t.machineID
      AND f.datetime > t.datetime
      AND f.datetime <= t.datetime + INTERVAL 24 HOUR
); 

-- Component-specific
UPDATE telemetry_with_model t
SET failure_comp1_next_24h = 1
WHERE EXISTS (
    SELECT 1 FROM PdM_failures f
    WHERE f.machineID = t.machineID
      AND f.failure = 'comp1'
      AND f.datetime > t.datetime
      AND f.datetime <= t.datetime + INTERVAL 24 HOUR
);

UPDATE telemetry_with_model t
SET failure_comp2_next_24h = 1
WHERE EXISTS (
    SELECT 1 FROM PdM_failures f
    WHERE f.machineID = t.machineID
      AND f.failure = 'comp2'
      AND f.datetime > t.datetime
      AND f.datetime <= t.datetime + INTERVAL 24 HOUR
);

UPDATE telemetry_with_model t
SET failure_comp3_next_24h = 1
WHERE EXISTS (
    SELECT 1 FROM PdM_failures f
    WHERE f.machineID = t.machineID
      AND f.failure = 'comp3'
      AND f.datetime > t.datetime
      AND f.datetime <= t.datetime + INTERVAL 24 HOUR
);

UPDATE telemetry_with_model t
SET failure_comp4_next_24h = 1
WHERE EXISTS (
    SELECT 1 FROM PdM_failures f
    WHERE f.machineID = t.machineID
      AND f.failure = 'comp4'
      AND f.datetime > t.datetime
      AND f.datetime <= t.datetime + INTERVAL 24 HOUR
); -- checkpoint, ran all before this
-- Lag1 features
ALTER TABLE telemetry_with_model ADD COLUMN volt_lag1 FLOAT;
ALTER TABLE telemetry_with_model ADD COLUMN rotate_lag1 FLOAT;
ALTER TABLE telemetry_with_model ADD COLUMN pressure_lag1 FLOAT;
ALTER TABLE telemetry_with_model ADD COLUMN vibration_lag1 FLOAT;
UPDATE telemetry_with_model t
JOIN (
    SELECT 
        t1.machineID, 
        t1.datetime, 
        t2.volt AS volt_lag1
    FROM telemetry_with_model t1
    JOIN telemetry_with_model t2
      ON t1.machineID = t2.machineID
     AND t2.datetime = t1.datetime - INTERVAL 1 HOUR
) AS lagged
ON t.machineID = lagged.machineID AND t.datetime = lagged.datetime
SET t.volt_lag1 = lagged.volt_lag1; 
-- checkpoint

UPDATE telemetry_with_model t
SET rotate_lag1 = (
    SELECT t2.rotate FROM telemetry_with_model t2
    WHERE t2.machineID = t.machineID
      AND t2.datetime = t.datetime - INTERVAL 1 HOUR
    LIMIT 1
);
UPDATE telemetry_with_model t
SET pressure_lag1 = (
    SELECT t2.pressure FROM telemetry_with_model t2
    WHERE t2.machineID = t.machineID
      AND t2.datetime = t.datetime - INTERVAL 1 HOUR
    LIMIT 1
);
UPDATE telemetry_with_model t
SET vibration_lag1 = (
    SELECT t2.vibration FROM telemetry_with_model t2
    WHERE t2.machineID = t.machineID
      AND t2.datetime = t.datetime - INTERVAL 1 HOUR
    LIMIT 1
);
-- Lag2 features
ALTER TABLE telemetry_with_model ADD COLUMN volt_lag2 FLOAT;
ALTER TABLE telemetry_with_model ADD COLUMN rotate_lag2 FLOAT;
ALTER TABLE telemetry_with_model ADD COLUMN pressure_lag2 FLOAT;
ALTER TABLE telemetry_with_model ADD COLUMN vibration_lag2 FLOAT;
UPDATE telemetry_with_model t
SET volt_lag2 = (
    SELECT t2.volt FROM telemetry_with_model t2
    WHERE t2.machineID = t.machineID
      AND t2.datetime = t.datetime - INTERVAL 2 HOUR
    LIMIT 1
);
UPDATE telemetry_with_model t
SET rotate_lag2 = (
    SELECT t2.rotate FROM telemetry_with_model t2
    WHERE t2.machineID = t.machineID
      AND t2.datetime = t.datetime - INTERVAL 2 HOUR
    LIMIT 1
);
UPDATE telemetry_with_model t
SET pressure_lag2 = (
    SELECT t2.pressure FROM telemetry_with_model t2
    WHERE t2.machineID = t.machineID
      AND t2.datetime = t.datetime - INTERVAL 2 HOUR
    LIMIT 1
);
UPDATE telemetry_with_model t
SET vibration_lag2 = (
    SELECT t2.vibration FROM telemetry_with_model t2
    WHERE t2.machineID = t.machineID
      AND t2.datetime = t.datetime - INTERVAL 2 HOUR
    LIMIT 1
);

-- Lag3 features
ALTER TABLE telemetry_with_model ADD COLUMN volt_lag3 FLOAT;
ALTER TABLE telemetry_with_model ADD COLUMN rotate_lag3 FLOAT;
ALTER TABLE telemetry_with_model ADD COLUMN pressure_lag3 FLOAT;
ALTER TABLE telemetry_with_model ADD COLUMN vibration_lag3 FLOAT;
UPDATE telemetry_with_model t
SET volt_lag3 = (
    SELECT t2.volt FROM telemetry_with_model t2
    WHERE t2.machineID = t.machineID
      AND t2.datetime = t.datetime - INTERVAL 3 HOUR
    LIMIT 1
);
UPDATE telemetry_with_model t
SET rotate_lag3 = (
    SELECT t2.rotate FROM telemetry_with_model t2
    WHERE t2.machineID = t.machineID
      AND t2.datetime = t.datetime - INTERVAL 3 HOUR
    LIMIT 1
);
UPDATE telemetry_with_model t
SET pressure_lag3 = (
    SELECT t2.pressure FROM telemetry_with_model t2
    WHERE t2.machineID = t.machineID
      AND t2.datetime = t.datetime - INTERVAL 3 HOUR
    LIMIT 1
);
UPDATE telemetry_with_model t
SET vibration_lag3 = (
    SELECT t2.vibration FROM telemetry_with_model t2
    WHERE t2.machineID = t.machineID
      AND t2.datetime = t.datetime - INTERVAL 3 HOUR
    LIMIT 1
);
-- 

-- Rolling average 6hr window
ALTER TABLE telemetry_with_model ADD COLUMN volt_roll_mean_6 FLOAT;
ALTER TABLE telemetry_with_model ADD COLUMN rotate_roll_mean_6 FLOAT;
ALTER TABLE telemetry_with_model ADD COLUMN pressure_roll_mean_6 FLOAT;
ALTER TABLE telemetry_with_model ADD COLUMN vibration_roll_mean_6 FLOAT;
UPDATE telemetry_with_model t
SET volt_roll_mean_6 = (
    SELECT AVG(t2.volt)
    FROM telemetry_with_model t2
    WHERE t2.machineID = t.machineID
      AND t2.datetime BETWEEN t.datetime - INTERVAL 6 HOUR AND t.datetime
);
UPDATE telemetry_with_model t
SET rotate_roll_mean_6 = (
    SELECT AVG(t2.rotate)
    FROM telemetry_with_model t2
    WHERE t2.machineID = t.machineID
      AND t2.datetime BETWEEN t.datetime - INTERVAL 6 HOUR AND t.datetime
);
UPDATE telemetry_with_model t
SET pressure_roll_mean_6 = (
    SELECT AVG(t2.pressure)
    FROM telemetry_with_model t2
    WHERE t2.machineID = t.machineID
      AND t2.datetime BETWEEN t.datetime - INTERVAL 6 HOUR AND t.datetime
);
UPDATE telemetry_with_model t
SET vibration_roll_mean_6 = (
    SELECT AVG(t2.vibration)
    FROM telemetry_with_model t2
    WHERE t2.machineID = t.machineID
      AND t2.datetime BETWEEN t.datetime - INTERVAL 6 HOUR AND t.datetime
);
-- Rolling average 12hr window
ALTER TABLE telemetry_with_model ADD COLUMN volt_roll_mean_12 FLOAT;
ALTER TABLE telemetry_with_model ADD COLUMN rotate_roll_mean_12 FLOAT;
ALTER TABLE telemetry_with_model ADD COLUMN pressure_roll_mean_12 FLOAT;
ALTER TABLE telemetry_with_model ADD COLUMN vibration_roll_mean_12 FLOAT;
UPDATE telemetry_with_model t
SET volt_roll_mean_12 = (
    SELECT AVG(t2.volt)
    FROM telemetry_with_model t2
    WHERE t2.machineID = t.machineID
      AND t2.datetime BETWEEN t.datetime - INTERVAL 12 HOUR AND t.datetime
);
UPDATE telemetry_with_model t
SET rotate_roll_mean_12 = (
    SELECT AVG(t2.rotate)
    FROM telemetry_with_model t2
    WHERE t2.machineID = t.machineID
      AND t2.datetime BETWEEN t.datetime - INTERVAL 12 HOUR AND t.datetime
);
UPDATE telemetry_with_model t
SET pressure_roll_mean_12 = (
    SELECT AVG(t2.pressure)
    FROM telemetry_with_model t2
    WHERE t2.machineID = t.machineID
      AND t2.datetime BETWEEN t.datetime - INTERVAL 12 HOUR AND t.datetime
);
UPDATE telemetry_with_model t
SET vibration_roll_mean_12 = (
    SELECT AVG(t2.vibration)
    FROM telemetry_with_model t2
    WHERE t2.machineID = t.machineID
      AND t2.datetime BETWEEN t.datetime - INTERVAL 12 HOUR AND t.datetime
);

-- 

-- Rolling error count 6hr window
ALTER TABLE telemetry_with_model ADD COLUMN error1_count_6h INT DEFAULT 0;
ALTER TABLE telemetry_with_model ADD COLUMN error2_count_6h INT DEFAULT 0;
ALTER TABLE telemetry_with_model ADD COLUMN error3_count_6h INT DEFAULT 0;
ALTER TABLE telemetry_with_model ADD COLUMN error4_count_6h INT DEFAULT 0;
ALTER TABLE telemetry_with_model ADD COLUMN error5_count_6h INT DEFAULT 0;
UPDATE telemetry_with_model t
SET error1_count_6h = (
    SELECT COUNT(*) FROM PdM_errors e
    WHERE e.machineID = t.machineID
      AND e.errorID = 'error1'
      AND e.datetime BETWEEN t.datetime - INTERVAL 6 HOUR AND t.datetime
);
UPDATE telemetry_with_model t
SET error2_count_6h = (
    SELECT COUNT(*) FROM PdM_errors e
    WHERE e.machineID = t.machineID
      AND e.errorID = 'error2'
      AND e.datetime BETWEEN t.datetime - INTERVAL 6 HOUR AND t.datetime
);
UPDATE telemetry_with_model t
SET error3_count_6h = (
    SELECT COUNT(*) FROM PdM_errors e
    WHERE e.machineID = t.machineID
      AND e.errorID = 'error3'
      AND e.datetime BETWEEN t.datetime - INTERVAL 6 HOUR AND t.datetime
);
UPDATE telemetry_with_model t
SET error4_count_6h = (
    SELECT COUNT(*) FROM PdM_errors e
    WHERE e.machineID = t.machineID
      AND e.errorID = 'error4'
      AND e.datetime BETWEEN t.datetime - INTERVAL 6 HOUR AND t.datetime
);
UPDATE telemetry_with_model t
SET error5_count_6h = (
    SELECT COUNT(*) FROM PdM_errors e
    WHERE e.machineID = t.machineID
      AND e.errorID = 'error5'
      AND e.datetime BETWEEN t.datetime - INTERVAL 6 HOUR AND t.datetime
);

-- Rolling error count 12hr window
ALTER TABLE telemetry_with_model ADD COLUMN error1_count_12h INT DEFAULT 0;
ALTER TABLE telemetry_with_model ADD COLUMN error2_count_12h INT DEFAULT 0;
ALTER TABLE telemetry_with_model ADD COLUMN error3_count_12h INT DEFAULT 0;
ALTER TABLE telemetry_with_model ADD COLUMN error4_count_12h INT DEFAULT 0;
ALTER TABLE telemetry_with_model ADD COLUMN error5_count_12h INT DEFAULT 0;
UPDATE telemetry_with_model t
SET error1_count_12h = (
    SELECT COUNT(*) FROM PdM_errors e
    WHERE e.machineID = t.machineID
      AND e.errorID = 'error1'
      AND e.datetime BETWEEN t.datetime - INTERVAL 12 HOUR AND t.datetime
);
UPDATE telemetry_with_model t
SET error2_count_12h = (
    SELECT COUNT(*) FROM PdM_errors e
    WHERE e.machineID = t.machineID
      AND e.errorID = 'error2'
      AND e.datetime BETWEEN t.datetime - INTERVAL 12 HOUR AND t.datetime
);
UPDATE telemetry_with_model t
SET error3_count_12h = (
    SELECT COUNT(*) FROM PdM_errors e
    WHERE e.machineID = t.machineID
      AND e.errorID = 'error3'
      AND e.datetime BETWEEN t.datetime - INTERVAL 12 HOUR AND t.datetime
);
UPDATE telemetry_with_model t
SET error4_count_12h = (
    SELECT COUNT(*) FROM PdM_errors e
    WHERE e.machineID = t.machineID
      AND e.errorID = 'error4'
      AND e.datetime BETWEEN t.datetime - INTERVAL 12 HOUR AND t.datetime
);
UPDATE telemetry_with_model t
SET error5_count_12h = (
    SELECT COUNT(*) FROM PdM_errors e
    WHERE e.machineID = t.machineID
      AND e.errorID = 'error5'
      AND e.datetime BETWEEN t.datetime - INTERVAL 12 HOUR AND t.datetime
);
-- 

ALTER TABLE telemetry_with_model ADD COLUMN no_failure_next_24h INT;

UPDATE telemetry_with_model
SET no_failure_next_24h = CASE
    WHEN failure_comp1_next_24h = 0
     AND failure_comp2_next_24h = 0
     AND failure_comp3_next_24h = 0
     AND failure_comp4_next_24h = 0
    THEN 1 ELSE 0
END;

