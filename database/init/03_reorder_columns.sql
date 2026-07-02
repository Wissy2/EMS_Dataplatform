-- ============================================================
-- OPTIONAL: reorder columns in ems.equipment so tag_id appears
-- first. Purely cosmetic — has no effect on constraints, FKs,
-- or query behavior. Postgres doesn't support reordering
-- columns in place, so we rebuild the table.
-- ============================================================

BEGIN;

ALTER TABLE ems.equipment RENAME TO equipment_old;

CREATE TABLE ems.equipment (
    tag_id VARCHAR(50) PRIMARY KEY,
    area_id INTEGER NOT NULL REFERENCES ems.areas(area_id) ON DELETE CASCADE,
    device_id VARCHAR(100) NOT NULL UNIQUE,
    equipment_name VARCHAR(150),
    equipment_type VARCHAR(100),
    mqtt_topic_template TEXT,
    manufacturer VARCHAR(100),
    rated_power_kw NUMERIC(10,2),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO ems.equipment (
    tag_id, area_id, device_id, equipment_name, equipment_type,
    mqtt_topic_template, manufacturer, rated_power_kw, created_at
)
SELECT
    tag_id, area_id, device_id, equipment_name, equipment_type,
    mqtt_topic_template, manufacturer, rated_power_kw, created_at
FROM ems.equipment_old;

-- Re-point FKs from the referencing tables to the new table
ALTER TABLE ems.electrical_measurements
    DROP CONSTRAINT electrical_measurements_tag_id_fkey,
    ADD CONSTRAINT electrical_measurements_tag_id_fkey
        FOREIGN KEY (tag_id) REFERENCES ems.equipment(tag_id);

ALTER TABLE ems.process_variables
    DROP CONSTRAINT process_variables_tag_id_fkey,
    ADD CONSTRAINT process_variables_tag_id_fkey
        FOREIGN KEY (tag_id) REFERENCES ems.equipment(tag_id);

ALTER TABLE ems.equipment_kpis
    DROP CONSTRAINT equipment_kpis_tag_id_fkey,
    ADD CONSTRAINT equipment_kpis_tag_id_fkey
        FOREIGN KEY (tag_id) REFERENCES ems.equipment(tag_id);

ALTER TABLE ems.alarm_history
    DROP CONSTRAINT alarm_history_tag_id_fkey,
    ADD CONSTRAINT alarm_history_tag_id_fkey
        FOREIGN KEY (tag_id) REFERENCES ems.equipment(tag_id);

DROP TABLE ems.equipment_old;

GRANT ALL PRIVILEGES ON ems.equipment TO ems_user;

COMMIT;
