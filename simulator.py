# -*- coding: utf-8 -*-
# Virtual Courts Simulator - Smart Padel Court System
# University of Coimbra - Giovanni Faedo - 2025267503

import paho.mqtt.client as mqtt
import json
import time
import random
import threading

# MQTT broker configuration
LOCAL_MQTT_URL = "localhost"

# MQTT topics
PUB_TOPIC = "deec/evt/status"     # Topic used to publish court status
CMD_TOPIC = "deec/cmd/leds"       # Topic used to receive booking commands

# Time interval (seconds) between consecutive publications
PUBLISH_INTERVAL = 30

# Definition of the virtual padel courts.
# Each court is initialized with random environmental values.
VIRTUAL_COURTS = [
    {"id": "Court_03", "base_temp": round(random.uniform(29.5, 32.0), 1), "base_hum": random.randint(45, 52), "base_lux": random.randint(8, 15)},
    {"id": "Court_04", "base_temp": round(random.uniform(29.5, 32.0), 1), "base_hum": random.randint(45, 52), "base_lux": random.randint(8, 15)},
    {"id": "Court_05", "base_temp": round(random.uniform(29.5, 32.0), 1), "base_hum": random.randint(45, 52), "base_lux": random.randint(8, 15)},
    {"id": "Court_06", "base_temp": round(random.uniform(29.5, 32.0), 1), "base_hum": random.randint(45, 52), "base_lux": random.randint(8, 15)},
    {"id": "Court_07", "base_temp": round(random.uniform(29.5, 32.0), 1), "base_hum": random.randint(45, 52), "base_lux": random.randint(8, 15)},
    {"id": "Court_08", "base_temp": round(random.uniform(29.5, 32.0), 1), "base_hum": random.randint(45, 52), "base_lux": random.randint(8, 15)},
]

# Shared state containing the latest values for each virtual court
state = {
    c["id"]: {
        "temp": c["base_temp"],
        "hum": c["base_hum"],
        "lux": c["base_lux"],
        "occupancy": 0,   # Court initially empty
        "booking": 0,     # Court initially available
    }
    for c in VIRTUAL_COURTS
}

# Lock used to safely access shared data from multiple threads
state_lock = threading.Lock()


def on_connect(client, userdata, flags, rc):
    """
    Callback executed when the MQTT client connects to the broker.
    If the connection is successful, subscribe to the command topic.
    """
    if rc == 0:
        client.subscribe(CMD_TOPIC)
        print("[MQTT] Connected to " + LOCAL_MQTT_URL +
              " - Subscribed to topic " + CMD_TOPIC)
    else:
        print("[MQTT] Connection failed (return code = " + str(rc) + ")")


def on_message(client, userdata, msg):
    """
    Callback executed whenever a message is received from the command topic.

    The received message updates the booking status of a specific court.
    """
    try:
        payload = json.loads(msg.payload.decode("utf-8"))

        node_id = payload.get("node_id", "")
        status = payload.get("status", "")

        # Ignore unknown courts
        if node_id not in state:
            return

        # Update booking status safely
        with state_lock:
            state[node_id]["booking"] = 1 if status == "ON" else 0

        # Simulate LED color according to booking state
        led = "RED" if status == "ON" else "GREEN"

        print("[CMD] " + node_id +
              " booking status set to " + status +
              " - LED color: " + led)

    except Exception as e:
        print("[CMD] Error while parsing command: " + str(e))


def simulate_court(client, court_id, startup_delay):
    """
    Simulates the behaviour of a virtual padel court.

    Every PUBLISH_INTERVAL seconds:
    - environmental values are slightly updated;
    - occupancy may randomly change;
    - the new status is published via MQTT.
    """

    # Delay the startup so that not all courts publish simultaneously
    time.sleep(startup_delay)

    print("[SIM] " + court_id + " simulation started")

    while True:

        with state_lock:
            s = state[court_id]

            # Simulate small temperature variations
            s["temp"] = round(
                max(18.0, min(32.0,
                    s["temp"] + random.uniform(-0.3, 0.3))), 1)

            # Simulate humidity variations
            s["hum"] = max(
                30,
                min(90,
                    round(s["hum"] + random.uniform(-1.5, 1.5)))
            )

            # Simulate light intensity variations
            s["lux"] = max(
                5,
                min(20,
                    round(s["lux"] + random.uniform(-2, 2)))
            )

            # With a small probability, change occupancy state
            if random.random() < 0.02:
                s["occupancy"] = 1 - s["occupancy"]

            # Prepare JSON payload
            payload = {
                "node_id": court_id,
                "ts": str(int(time.time())),
                "temp": s["temp"],
                "hum": s["hum"],
                "lux": s["lux"],
                "occupancy": s["occupancy"],
                "booking": s["booking"],
            }

        # Publish payload via MQTT
        client.publish(PUB_TOPIC, json.dumps(payload))

        print("[SIM] " + court_id +
              " published: " +
              json.dumps(payload))

        # Wait before the next publication
        time.sleep(PUBLISH_INTERVAL)


# Create MQTT client
client = mqtt.Client()

# Register callback functions
client.on_connect = on_connect
client.on_message = on_message

# Connect to the MQTT broker
client.connect(LOCAL_MQTT_URL, 1883, 60)

# Start MQTT network loop in a background thread
client.loop_start()

# Create one simulation thread for each virtual court
for i, court in enumerate(VIRTUAL_COURTS):
    t = threading.Thread(
        target=simulate_court,
        args=(client, court["id"], i * 5)
    )

    t.daemon = True
    t.start()

print("[SIM] Virtual court simulator started for "
      + str(len(VIRTUAL_COURTS)) + " courts.")

print("[SIM] Publishing data every "
      + str(PUBLISH_INTERVAL)
      + " seconds on topic "
      + PUB_TOPIC)

print("[SIM] Press Ctrl+C to stop the simulator.")

try:
    while True:
        # Keep the main thread alive while simulation threads run
        time.sleep(0.1)

except KeyboardInterrupt:

    print("[SIM] Simulator stopped by user.")

    client.loop_stop()
    client.disconnect()