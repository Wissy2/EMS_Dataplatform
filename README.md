# EMS Project

## Requirements
- Docker Desktop (Windows or Linux)
- Git

## Setup
git clone https://github.com/Wissy2/EMS_Dataplatform.git
cd EMS_Dataplatform

## Start the whole stack 
docker compose up -d

## What runs
| Service     | URL / Port               |
|-------------|--------------------------|
| Node-RED    | http://localhost:1880    |
| EMQX broker | http://localhost:18083   |
| MQTT        | http://localhost:1883           |
| Flink       | http://localhost:8081   |

## Verify data is flowing
## Subscribe to all topics and watch live messages
>docker exec ems-dataplatform-mqtt-broker-1 mosquitto_sub -h localhost -t "#" -v

## Raw_ Data Processor 
## submit the job 
>docker exec flink-jobmanager flink run    --python /opt/flink/jobs/raw_processor/main.py    --pyFiles /opt/flink/jobs/raw_processor/    -d

## Analytics / KPI Processor
## If this project was started before the wide KPI tables were added, apply the KPI schema once
>powershell -ExecutionPolicy Bypass -File .\scripts\apply-kpi-output-schema.ps1

## submit the job
>docker exec flink-jobmanager flink run    --python /opt/flink/jobs/kpi_processor/kpi_job.py    --pyFiles /opt/flink/jobs/kpi_processor/    -d

## Low-resource backfill workflow
The KPI processor reads the normalized Kafka topics written by the raw processor. You do not need to keep both Flink jobs running at the same time:

1. Start the stack.
2. Run the raw processor until data appears in `ems.raw_measurements` and the normalized topics.
3. Cancel/stop the raw processor in the Flink UI or CLI.
4. Run the KPI processor. Its default `KAFKA_START_OFFSET=earliest` lets it consume retained normalized Kafka data.

If the KPI processor has already used the default consumer group, use a fresh group id for a replay:
>docker exec -e KPI_KAFKA_GROUP_ID=ems-kpi-backfill-001 flink-jobmanager flink run    --python /opt/flink/jobs/kpi_processor/kpi_job.py    --pyFiles /opt/flink/jobs/kpi_processor/    -d

## Verify on FLink UI
Open http://localhost:8081

## Verify database 
## Connect to timescaleDB
>docker exec -it timescaledb psql -U ems_user -d ems_db
## Inside Timescaledb container 
>\dt ems.*
>SELECT * FROM ems.raw_measurements ORDER BY ingestion_timestamp DESC;
>SELECT * FROM ems.equipment_kpis ORDER BY window_end DESC;
## To quit 
>\q

