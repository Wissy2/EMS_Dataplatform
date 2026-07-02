# EMS Project

## Requirements
- Docker Desktop (Windows or Linux)
- Git

## Setup
git clone https://github.com/Wissy2/EMS_Dataplatform.git
cd EMS_Dataplatform

## Start the whole stack 
docker compose up -d
docker compose up --build 

## What runs
| Service     | URL / Port               |
|-------------|--------------------------|
| Modbus sim  | localhost:502            |
| Node-RED    | http://localhost:1880    |
| EMQX broker | http://localhost:18083   |
| MQTT        | localhost:1883           |
| Flink       |  http://localhost:8081   |

## Verify data is flowing
# Subscribe to all topics and watch live messages
docker exec ems-dataplatform-mqtt-broker-1 mosquitto_sub -h localhost -t "#" -v

##Raw_ Data Processor 
## submit the job 
docker exec flink-jobmanager flink run    --python /opt/flink/jobs/raw_processor/main.py    --pyFiles /opt/flink/jobs/raw_processor/    -d






##########################################################
## submit the flink job 
docker exec flink-jobmanager \
  flink run --python /opt/flink/jobs/threshold_alerts.py
 
## verify 
docker exec flink-jobmanager flink list

## Check kafka consumer on topic ems.meters.1
docker exec -it kafka \
  kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic ems.meters.1 \
  --from-beginning
  
## make sure ems.alerts topic exist 
docker exec -it kafka \
  kafka-topics --list --bootstrap-server localhost:29092


## To trigger a test alert, publish a fake out-of-range message to ems.meters.1
for i in {1..20 }; do
  echo '{"device_id":"PM-001","device_name":"Power Meter 1","timestamp":"2025-05-08T10:00:00Z","measurements":{"frequency_Hz":50.0,"voltage_V":270.0,"current_A":12.0,"power_factor":0.95,"thd_voltage_pct":2.1,"thd_current_pct":4.3,"active_energy_kWh":1024}}' | \
  docker exec -i kafka kafka-console-producer \
    --bootstrap-server localhost:29092 \
    --topic ems.meters.1
  sleep 0.5
done

## Open new terminal for alerts
docker exec -it kafka \
  kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic ems.alerts \
  --from-beginning
