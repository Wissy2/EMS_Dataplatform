-- ============================================================
-- EMS METADATA SEED DATA
-- Areas and equipment used by the Node-RED MQTT flows.
-- ============================================================

BEGIN;

INSERT INTO ems.areas (line_id, area_name)
SELECT pl.line_id, area_name
FROM ems.production_lines pl
CROSS JOIN (
    VALUES
        ('Extraction'),
        ('Washing'),
        ('Flotation'),
        ('Utilities'),
        ('Storage_Handling')
) AS areas(area_name)
WHERE pl.line_code = 'Line-1'
ON CONFLICT (line_id, area_name) DO NOTHING;

INSERT INTO ems.equipment (
    tag_id,
    area_id,
    device_id,
    equipment_name,
    equipment_type,
    mqtt_topic_template
)
SELECT
    seed.tag_id,
    a.area_id,
    seed.device_id,
    seed.equipment_name,
    seed.equipment_type,
    seed.mqtt_topic_template
FROM (
    VALUES
        ('L1-EXTR-BW',  'Extraction',       'L1-EXTR-BW',  'Bucket Wheel Excavator',        'Power Meter',      'Al_Youssoufia_Plant/Line-1/Extraction/Bucket_Wheel_Excavator/+'),
        ('L1-EXTR-CV1', 'Extraction',       'L1-EXTR-CV1', 'Conveyor C1',                   'Conveyor',         'Al_Youssoufia_Plant/Line-1/Extraction/Conveyor_C1/+'),
        ('L1-EXTR-WBS', 'Extraction',       'L1-EXTR-WBS', 'Weigh Belt Scale',              'Weigh Belt Scale', 'Al_Youssoufia_Plant/Line-1/Extraction/Weigh_Belt_Scale/+'),
        ('L1-WASH-WPA', 'Washing',          'L1-WASH-WPA', 'Washing Pump A',                'Pump',             'Al_Youssoufia_Plant/Line-1/Washing/Washing_Pump_A/+'),
        ('L1-WASH-WFM', 'Washing',          'L1-WASH-WFM', 'Water Flow Meter Inlet',        'Flow Meter',       'Al_Youssoufia_Plant/Line-1/Washing/Water_Flow_Meter_Inlet/+'),
        ('L1-FLOT-AG1', 'Flotation',        'L1-FLOT-AG1', 'Flotation Cell Agitator 1',     'Agitator',         'Al_Youssoufia_Plant/Line-1/Flotation/Flotation_Cell_Agitator_1/+'),
        ('L1-FLOT-SP',  'Flotation',        'L1-FLOT-SP',  'Slurry Pump',                   'Pump',             'Al_Youssoufia_Plant/Line-1/Flotation/Slurry_Pump/+'),
        ('L1-FLOT-WFM', 'Flotation',        'L1-FLOT-WFM', 'Water Flow Meter Flotation',    'Flow Meter',       'Al_Youssoufia_Plant/Line-1/Flotation/Water_Flow_Meter_Flotation/+'),
        ('L1-UTIL-AC',  'Utilities',        'L1-UTIL-AC',  'Air Compressor Flotation',      'Compressor',       'Al_Youssoufia_Plant/Line-1/Utilities/Air_Compressor_Flotation/+'),
        ('L1-STRG-CV7', 'Storage_Handling', 'L1-STRG-CV7', 'Conveyor C1 Output',            'Conveyor',         'Al_Youssoufia_Plant/Line-1/Storage_Handling/Conveyor_C1_Output/+'),
        ('L1-STRG-WBS', 'Storage_Handling', 'L1-STRG-WBS', 'Weigh Belt Scale Output',       'Weigh Belt Scale', 'Al_Youssoufia_Plant/Line-1/Storage_Handling/Weigh_Belt_Scale_Output/+')
) AS seed(tag_id, area_name, device_id, equipment_name, equipment_type, mqtt_topic_template)
JOIN ems.areas a ON a.area_name = seed.area_name
JOIN ems.production_lines pl ON pl.line_id = a.line_id AND pl.line_code = 'Line-1'
ON CONFLICT (tag_id) DO NOTHING;

COMMIT;
