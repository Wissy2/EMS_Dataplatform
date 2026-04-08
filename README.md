# EMS Project

## Requirements
- Docker Desktop (Windows or Linux)
- Git

## Setup
git clone https://github.com/your-org/ems-project
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
docker run --rm eclipse-mosquitto \
  mosquitto_sub -h host.docker.internal -t "ems/#" -v