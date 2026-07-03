# IoT-based Smart Padel Court System
**Universidade de Coimbra — Internet of Things**
Student: Giovanni Faedo | ID: 2025267503 | Academic Year 2025/2026

---

## Project Overview
End-to-end IoT solution for smart management of padel courts. The system monitors
environmental conditions (temperature, humidity, luminosity, occupancy) in real time,
stores data on AWS DynamoDB, and allows remote court booking via a web dashboard
with physical LED feedback on the Zolertia RE-Mote boards.

---

## Repository Structure

```
SmartPadel/
├── mqtt_publisher1.c     # Court 01 — Physical node + 6LoWPAN Border Router (Contiki-NG)
├── mqtt_publisher.c      # Court 02 — Physical sensor node (Contiki-NG)
├── project-conf.h        # Contiki-NG configuration (MQTT broker IP, buffer size)
├── Makefile              # Compilation rules for Zolertia RE-Mote (USE_TUNSLIP6=1)
├── mqtt-bridge.py        # MQTT bridge: local Mosquitto ↔ AWS IoT Core (TLS 1.2)
├── simulator.py          # Virtual nodes simulator (Courts 03–08)
├── app.py                # Flask backend — REST API + MQTT client
├── templates/
│   └── index2.html       # Web dashboard (HTML5 + JS + Chart.js)
├── certs/                # AWS IoT Core certificates (NOT included — see below)
│   ├── AmazonRootCA1.pem
│   ├── certificate.pem.crt
│   └── private.pem.key
└── README.md
```

---

## Requirements

### Python dependencies
```bash
pip3 install paho-mqtt flask boto3 requests
```

### System dependencies (Ubuntu VM)
```bash
sudo apt-get install mosquitto mosquitto-clients
```

### AWS Configuration
1. Create an AWS IoT Core **Thing** and download the certificates
2. Place them in the `certs/` folder:
   - `AmazonRootCA1.pem`
   - `certificate.pem.crt`
   - `private.pem.key`
3. Update the endpoint in `mqtt-bridge.py` and `app.py`:
```python
CLOUD_MQTT_URL = "your-endpoint.iot.eu-west-1.amazonaws.com"
```

---

## How to Run

Open **4 terminals** on the Ubuntu VM in this order:

**Terminal 1 — tunslip6 (Border Router):**
```bash
cd ~/contiki/tools
sudo ./tunslip6 -s /dev/ttyUSB0 fd00::1/64
```

**Terminal 2 — MQTT Bridge:**
```bash
cd ~/SmartPadel
python3 mqtt-bridge.py
```

**Terminal 3 — Virtual Nodes Simulator:**
```bash
cd ~/SmartPadel
python3 simulator.py
```

**Terminal 4 — Flask Dashboard:**
```bash
cd ~/SmartPadel
python3 app.py
```

Then open the dashboard on any browser:
```
http://<VM_IP>:5000
```

---

## Hardware Setup

| Board | Role | Connection |
|---|---|---|
| Zolertia RE-Mote #1 | Court 01 + Border Router | USB → VM (`/dev/ttyUSB0`) |
| Zolertia RE-Mote #2 | Court 02 | USB → Mac (power only) |

**Sensors connected to each board:**
- DHT22 — Temperature & Humidity
- TSL2561 — Luminosity (lux)
- PIR — Occupancy detection (ADC)

---

## System Architecture

```
Zolertia RE-Mote (C/Contiki-NG)
    → 6LoWPAN/IPv6 → tunslip6
        → Mosquitto (local, port 1883)
            → mqtt-bridge.py
                → AWS IoT Core (TLS 1.2, port 8883)
                    → IoT Rule → DynamoDB
                    → Lambda → API Gateway → Flask → Dashboard
```

**Downlink (booking command):**
```
Dashboard → Flask /api/booking
    → AWS IoT Core (deec/cmd/leds)
        → mqtt-bridge.py
            → Mosquitto → Zolertia RE-Mote
                → LED RED (booked) / LED GREEN (available)
```

---

## MQTT Topics

| Topic | Direction | Description |
|---|---|---|
| `deec/evt/status` | Uplink | Sensor data from nodes |
| `deec/cmd/leds` | Downlink | Booking commands to nodes |

---

## Message Format

**Uplink payload (every 30s):**
```json
{
  "node_id": "Court_01",
  "ts": "1782928730",
  "temp": 31.1,
  "hum": 48.8,
  "lux": 10,
  "occupancy": 1,
  "booking": 0
}
```

**Downlink payload (booking command):**
```json
{
  "node_id": "Court_01",
  "command": "SET_BOOKING",
  "status": "ON"
}
```
