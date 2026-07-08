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
## submit the job
>docker exec flink-jobmanager flink run    --python /opt/flink/jobs/kpi_processor/kpi_job.py    --pyFiles /opt/flink/jobs/kpi_processor/    -d

## Verify on FLink UI
Open http://localhost:8081

## Verify database 
## Connect to timescaleDB
>docker exec -it timescaledb psql -U ems_user -d ems_db
## Inside Timescaledb container 
>\dt ems.*
>SELECT * FROM ems.raw_measurements ORDER BY ingestion_timestamp DESC;
## To quit 
>\q

