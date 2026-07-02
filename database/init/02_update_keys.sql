-- ============================================================
-- EMS DATABASE MIGRATION
-- Replace equipment_id (SERIAL) with tag_id (VARCHAR) as the
-- primary key of ems.equipment, e.g. '310L2-AG-01'
--
-- All tables are currently empty, so this just changes the
-- schema shape (column types/constraints) with no data to move.
-- device_id is untouched (separate sub-equipment id).
-- ============================================================

BEGIN;

-- ============================================================
-- Referencing tables: drop equipment_id FK column FIRST
-- (must happen before dropping equipment.equipment_id, since
-- these FKs depend on it)
-- ============================================================

-- ---- electrical_measurements ----
ALTER TABLE ems.electrical_measurements
    DROP COLUMN equipment_id;

-- ---- process_variables ----
ALTER TABLE ems.process_variables
    DROP COLUMN equipment_id;

-- ---- equipment_kpis ----
ALTER TABLE ems.equipment_kpis
    DROP COLUMN equipment_id;

-- ---- alarm_history ----
ALTER TABLE ems.alarm_history
    DROP COLUMN equipment_id;

-- ============================================================
-- ems.equipment: swap PK from equipment_id to tag_id
-- ============================================================

ALTER TABLE ems.equipment
    DROP COLUMN equipment_id;

ALTER TABLE ems.equipment
    ADD COLUMN tag_id VARCHAR(50) PRIMARY KEY;

-- ============================================================
-- Referencing tables: add tag_id FK column
-- ============================================================

ALTER TABLE ems.electrical_measurements
    ADD COLUMN tag_id VARCHAR(50) NOT NULL
    REFERENCES ems.equipment(tag_id);

ALTER TABLE ems.process_variables
    ADD COLUMN tag_id VARCHAR(50) NOT NULL
    REFERENCES ems.equipment(tag_id);

-- equipment_id was nullable here originally, so tag_id stays nullable too
ALTER TABLE ems.equipment_kpis
    ADD COLUMN tag_id VARCHAR(50)
    REFERENCES ems.equipment(tag_id);

ALTER TABLE ems.alarm_history
    ADD COLUMN tag_id VARCHAR(50)
    REFERENCES ems.equipment(tag_id);

COMMIT;
