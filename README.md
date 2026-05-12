# EMS Project

## Requirements
- Docker Desktop (Windows or Linux)
- Git

## Setup
git clone https://github.com/Wissy2/EMS_Dataplatform.git
cd ems-project
docker compose up --build

## What runs
| Service     | URL / Port               |
|-------------|--------------------------|
| Modbus sim  | localhost:502            |
| Node-RED    | http://localhost:1880    |
| EMQX broker | http://localhost:18083   |
| MQTT        | localhost:1883           |

## Verify data is flowing
# Subscribe to all topics and watch live messages
docker exec ems-dataplatform-mqtt-broker-1 mosquitto_sub -h localhost -t "#" -v

##After Adding FLink
## Start the whole stack 
docker compose up -d
docker compose up --build

##submit the flink job 
docker exec flink-jobmanager \
  flink run --python /opt/flink/jobs/threshold_alerts.py
 
##verify 
docker exec flink-jobmanager flink list
also open http://localhost:8081

##Check kafka consumer on topic ems.meters.1
docker exec -it kafka \
  kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ems.meters.1 \
  --from-beginning
  
##make sure ems.alerts topic exist 
docker exec -it kafka \
  kafka-topics --list --bootstrap-server localhost:9092


##To trigger a test alert, publish a fake out-of-range message to ems.meters.1
docker exec -it kafka \
  kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic ems.meters
##then paste this 
> {"device_id":"PM-001","device_name":"Power Meter 1","timestamp":"2025-05-08T10:00:00Z","measurements":{"frequency_Hz":50.0,"voltage_V":270.0,"current_A":12.0,"power_factor":0.95,"thd_voltage_pct":2.1,"thd_current_pct":4.3,"active_energy_kWh":1024}}

> {"device_id":"PM-001","device_name":"Power Meter 1","timestamp":"2025-05-08T10:00:00Z","measurements":{"frequency_Hz":55.0,"voltage_V":230.0,"current_A":12.0,"power_factor":0.95,"thd_voltage_pct":2.1,"thd_current_pct":4.3,"active_energy_kWh":1024}}


##Open new terminal for alerts
docker exec -it kafka \
  kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ems.alerts \
  --from-beginning
