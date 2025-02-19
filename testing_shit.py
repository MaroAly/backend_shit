import pip

def install(package):
    if hasattr(pip, 'main'):
        pip.main(['install', package])
    else:
        pip._internal.main(['install', package])
install("paho-mqtt")
install("supabase")
import paho.mqtt.client as mqtt
import ssl
import json
from supabase import create_client, Client

# HiveMQ Cloud connection details
broker_address = "b9a9599b658d4bfa80390c3585960a4f.s1.eu.hivemq.cloud"
broker_port = 8883
username = "Mahmoud7essam"
password = "Mahmoud7essam"
topic = "ESP32/sensors"  # Replace with your actual topic
messages = []
# Callback when connecting to the broker
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to HiveMQ Cloud!")
        client.subscribe(topic)
    else:
        print(f"Connection failed with code {rc}")
# Supabase settings


url: str = "https://jgglikwemwntylzibekb.supabase.co"
key: str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImpnZ2xpa3dlbXdudHlsemliZWtiIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Mzg1Nzg3MDUsImV4cCI6MjA1NDE1NDcwNX0.tTzX2Mto1TrlhSL1Z-3A8ZOg3iceLVcmDKj-2fJQbvg"
supabase: Client = create_client(url, key)

############

# Callback when receiving messages
def on_message(client, userdata, msg):
    try:
        message_dict = json.loads(msg.payload.decode())
        print(f"Received message: {message_dict}") 
        messages.append(message_dict)
        # Ensure consistent data structure
        expected_columns = ['dateTime', 'counter', 'N', 'P', 'K', 'temperature', 'humidity', 'pH', 'Air']
        if all(col in message_dict for col in expected_columns):
            try:
                response = (
                    supabase.table("IoT_Data")
                    .insert({
                        "dateTime": message_dict["dateTime"],
                        "counter": message_dict["counter"],
                        "N": message_dict["N"],
                        "P": message_dict["P"],
                        "K": message_dict["K"],
                        "temperature": message_dict["temperature"],
                        "humidity": message_dict["humidity"],
                        "pH": message_dict["pH"],
                        "Air": message_dict["Air"]
                    })
                    .execute()
                )
                print(f"Inserted into Supabase: {response}")  # Debug print
            except Exception as e:
                print(f"Error inserting into Supabase: {e}")

    except UnicodeDecodeError:
        print(f"\nTopic: {msg.topic}")
        print(f"Raw payload (hex): {msg.payload.hex()}")

# Create MQTT client
client = mqtt.Client(protocol=mqtt.MQTTv311)

# Configure SSL/TLS
client.tls_set(
    ca_certs=None,
    certfile=None,
    keyfile=None,
    cert_reqs=ssl.CERT_REQUIRED,
    tls_version=ssl.PROTOCOL_TLS,
    ciphers=None
)

# Set credentials
client.username_pw_set(username, password)

# Assign callbacks
client.on_connect = on_connect
client.on_message = on_message

# Connect to broker
try:
    print(f"Connecting to {broker_address}...")
    client.connect(broker_address, port=broker_port, keepalive=60)
    client.loop_forever()
except Exception as e:
    print(f"Connection error: {e}")
