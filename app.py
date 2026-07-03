
# -*- coding: utf-8 -*-
"""
 * IoT Smart Padel Court System - Flask Backend
 * Universidade de Coimbra - Giovanni Faedo - 2025267503
 *
 * REST API Endpoints:
 *   GET  /              → Web dashboard (HTML interface)
 *   GET  /api/courts    → Current status of all courts (JSON)
 *   POST /api/booking   → Sends booking downlink command to AWS IoT Core
*/
"""

from flask import Flask, render_template, jsonify, request
import paho.mqtt.client as mqtt
import ssl
import json
import threading
import time
import requests
from datetime import datetime, timedelta

app = Flask(__name__, template_folder='templates')

# ── Configurations AWS IoT Core ────────────────────────────────────────────
CLOUD_MQTT_URL        = "your-endpoint.iot.eu-west-1.amazonaws.com"
CERTIFICATE_AUTH_FILE = "certs/AmazonRootCA1.pem"
CERT_PEM_FILE         = "certs/certificate.pem.crt"
PRIVATE_KEY_FILE      = "certs/private.pem.key"
CLOUD_PORT            = 8883

# MQTT Topics (shared with C nodes and bridge)
SENSORS_TOPIC  = "deec/evt/status"   # uplink from IoT nodes
COMMANDS_TOPIC = "deec/cmd/leds"     # downlink to IoT nodes

# AWS infrastructure configuration
LAMBDA_API_URL = "https://vkwuylcn74.execute-api.eu-west-1.amazonaws.com/default/padel-microservice"
DYNAMO_TABLE_NAME = "padel_iot_db"

# ─────────────────────────────────────────────────────────────
# In-memory state of all courts
# Updated in real-time by MQTT messages
# ─────────────────────────────────────────────────────────────
courts_state = {
    "Court_01": {"node_id":"Court_01","type":"physical","temp":0,"hum":0,"lux":0,"occupancy":0,"booking":0,"last_seen":None},
    "Court_02": {"node_id":"Court_02","type":"physical","temp":0,"hum":0,"lux":0,"occupancy":0,"booking":0,"last_seen":None},
    "Court_03": {"node_id":"Court_03","type":"virtual", "temp":0,"hum":0,"lux":0,"occupancy":0,"booking":0,"last_seen":None},
    "Court_04": {"node_id":"Court_04","type":"virtual", "temp":0,"hum":0,"lux":0,"occupancy":0,"booking":0,"last_seen":None},
    "Court_05": {"node_id":"Court_05","type":"virtual", "temp":0,"hum":0,"lux":0,"occupancy":0,"booking":0,"last_seen":None},
    "Court_06": {"node_id":"Court_06","type":"virtual", "temp":0,"hum":0,"lux":0,"occupancy":0,"booking":0,"last_seen":None},
    "Court_07": {"node_id":"Court_07","type":"virtual", "temp":0,"hum":0,"lux":0,"occupancy":0,"booking":0,"last_seen":None},
    "Court_08": {"node_id":"Court_08","type":"virtual", "temp":0,"hum":0,"lux":0,"occupancy":0,"booking":0,"last_seen":None},
}

# Thread safety for shared state
state_lock = threading.Lock()

# Recent system alerts (max 10 stored in memory)
alerts = []

# Global MQTT client (used for publishing downlink commands)
cloud_client = None

# ── Callback MQTT ──────────────────────────────────────────────────────────
def on_connect(client, userdata, flags, rc):
    """Called when connection to AWS IoT Core is established."""
    if rc == 0:
        print("[MQTT] Connected to AWS IoT Core successfully")
        client.subscribe(SENSORS_TOPIC)
        print(f"[MQTT] Subscribed to topic: {SENSORS_TOPIC}")
    else:
        print(f"[MQTT] Connection failed (code={rc})")

def on_message(client, userdata, msg):
    """
    Handles uplink messages from IoT nodes.
    Updates in-memory court state and generates alerts.
    """
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        node_id = payload.get("node_id", "")

        # Normalize Court_01 naming (border router vs node naming mismatch)
        if "Court_01" in node_id:
            node_id = "Court_01"

        if node_id not in courts_state:
            print(f"[MQTT] Unknown node received: {node_id}")
            return

        with state_lock:

            old_occ = courts_state[node_id]["occupancy"]
            new_occ = payload.get("occupancy", 0)

            # Generate alert if motion detected on unbooked court
            if new_occ == 1 and old_occ == 0:
                if courts_state[node_id]["booking"] == 0:
                    alert = {
                        "node_id": node_id,
                        "message": f"Motion detected on {node_id} (NOT booked)",
                        "timestamp": time.strftime("%H:%M:%S")
                    }
                    alerts.append(alert)

                    if len(alerts) > 10:
                        alerts.pop(0)

                    print(f"[ALERT] {alert['message']}")

            # Update state
            courts_state[node_id].update({
                "temp":      payload.get("temp", courts_state[node_id]["temp"]),
                "hum":       payload.get("hum", courts_state[node_id]["hum"]),
                "lux":       payload.get("lux", courts_state[node_id]["lux"]),
                "occupancy": payload.get("occupancy", courts_state[node_id]["occupancy"]),
                "booking":   payload.get("booking", courts_state[node_id]["booking"]),
                "last_seen": time.strftime("%H:%M:%S"),
            })

        print(f"[MQTT] Updated state for {node_id}")

    except json.JSONDecodeError as e:
        print(f"[MQTT] JSON parsing error: {e}")
    except Exception as e:
        print(f"[MQTT] Unexpected error: {e}")

def on_disconnect(client, userdata, rc):
    """Handles MQTT disconnection events."""
    print(f"[MQTT] Disconnected (rc={rc}). Reconnecting...")

# ── Avvio client MQTT in background ───────────────────────────────────────
def start_mqtt():
    """Initializes and runs MQTT client in background thread."""
    global cloud_client

    cloud_client = mqtt.Client(client_id="flask-dashboard")

    cloud_client.on_connect    = on_connect
    cloud_client.on_message    = on_message
    cloud_client.on_disconnect = on_disconnect

    # Secure connection to AWS IoT Core (TLS)
    cloud_client.tls_set(
        ca_certs=CERTIFICATE_AUTH_FILE,
        certfile=CERT_PEM_FILE,
        keyfile=PRIVATE_KEY_FILE,
        tls_version=ssl.PROTOCOL_TLSv1_2
    )

    cloud_client.tls_insecure_set(False)

    try:
        cloud_client.connect(CLOUD_MQTT_URL, CLOUD_PORT, keepalive=60)
        print(f"[MQTT] Connecting to {CLOUD_MQTT_URL}:{CLOUD_PORT}")
        cloud_client.loop_forever()

    except Exception as e:
        print(f"[MQTT] Connection error: {e}")

# ─────────────────────────────────────────────────────────────
# Flask REST API Routes
# ─────────────────────────────────────────────────────────────
@app.route("/")
def index():
    """Serves the main dashboard HTML page."""
    return render_template("index2.html")

@app.route("/api/courts", methods=["GET"])
def get_courts():
    """
    Returns the current state of all courts.

    This endpoint is periodically called by the frontend
    (every 30 seconds) to refresh the dashboard.
    """
    with state_lock:
        data = list(courts_state.values())

    return jsonify(data)

@app.route("/api/history", methods=["GET"])
def get_history():
    """
    Retrieves historical sensor data from AWS Lambda + DynamoDB.

    Supports optional filtering by node_id via query parameter.
    """
    try:
        resp = requests.get(
            LAMBDA_API_URL,
            params={"TableName": DYNAMO_TABLE_NAME},
            timeout=10
        )

        raw = resp.json()
        items = raw.get("Items", [])

        # Convert DynamoDB format to standard JSON
        def unwrap(item):
            out = {}
            for k, v in item.items():
                if "S" in v:
                    out[k] = v["S"]
                elif "N" in v:
                    try:
                        out[k] = float(v["N"]) if "." in v["N"] else int(v["N"])
                    except ValueError:
                        out[k] = v["N"]
                else:
                    out[k] = v
            return out

        history = [unwrap(i) for i in items]

        # Optional filtering by node_id
        node_filter = request.args.get("node_id")
        if node_filter:
            history = [h for h in history if h.get("node_id") == node_filter]

        # Sort by timestamp
        history.sort(key=lambda h: int(h.get("ts", 0)))

        return jsonify(history)

    except Exception as e:
        print(f"[Lambda] Error retrieving history: {e}")
        return jsonify({"error": str(e)}), 502

@app.route("/api/alerts", methods=["GET"])
def get_alerts():
    """Returns recent system alerts."""
    return jsonify(alerts)

@app.route("/api/booking", methods=["POST"])
def booking():
    """
    Receives a booking command from the dashboard and publishes it
    to AWS IoT Core via MQTT.

    Expected JSON body:
      {
        "node_id": "Court_01",
        "command": "SET_BOOKING",
        "status": "ON",
        "duration": 30 (optional)
      }
    """

    body = request.get_json()

    if not body:
        return jsonify({"error": "Missing JSON body"}), 400

    node_id = body.get("node_id")
    status  = body.get("status")  # "ON" or "OFF"

    # Validate node
    if node_id not in courts_state:
        return jsonify({"error": f"Unknown node: {node_id}"}), 404

    # Validate status
    if status not in ("ON", "OFF"):
        return jsonify({"error": "status must be ON or OFF"}), 400

    # MQTT payload
    payload = json.dumps({
        "node_id": node_id,
        "command": "SET_BOOKING",
        "status": status
    }, separators=(',', ':'))

    # Publish via MQTT (AWS IoT Core)
    if cloud_client and cloud_client.is_connected():
        cloud_client.publish(COMMANDS_TOPIC, payload, qos=1)
        print(f"[BOOKING] Downlink sent: {payload}")
    else:
        print("[BOOKING] MQTT not connected — command not sent")
        return jsonify({"error": "MQTT not connected"}), 503

    # Optional booking duration handling
    duration = body.get("duration")
    booking_end = None

    if status == "ON" and duration:
        booking_end = (
            datetime.now() + timedelta(minutes=int(duration))
        ).strftime("%H:%M")

    # Optimistic local update
    with state_lock:
        courts_state[node_id]["booking"] = 1 if status == "ON" else 0
        courts_state[node_id]["booking_end"] = booking_end if status == "ON" else None

    return jsonify({
        "ok": True,
        "node_id": node_id,
        "status": status,
        "booking_end": booking_end
    })


# ─────────────────────────────────────────────────────────────
# Application entry point
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Start MQTT client in background thread
    mqtt_thread = threading.Thread(target=start_mqtt, daemon=True)
    mqtt_thread.start()

    # Allow MQTT connection to stabilize
    time.sleep(2)

    print("[Flask] Dashboard available at http://localhost:5000")

    # Start Flask server
    app.run(host="0.0.0.0", port=5000, debug=False)