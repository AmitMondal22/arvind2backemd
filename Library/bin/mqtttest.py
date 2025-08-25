import paho.mqtt.client as mqtt
from controllers.device_to_server import WaterController
from Library.DotDictLibrary import DotDictLibrary
from utils.date_time_format import get_current_datetime
from models.mqtt_model import MqttPublishDeviceSchedule
from models.device_data_model import DeviceAdd
from controllers.admin import DeviceController
import json
import asyncio
from datetime import datetime

class MqttLibraryClass:
    def __init__(self, broker_address, broker_port, username, password):
        self.client = mqtt.Client()
        self.client.username_pw_set(username, password)  # Set MQTT authentication
        self.broker_address = broker_address
        self.broker_port = broker_port
        # Callback functions
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.subscriptions = []

    def on_connect(self, client, userdata, flags, rc):
        print(f"Connected with result code {rc}")
        for topic, qos in self.subscriptions:
            print(f"Subscribing to {topic} with QoS {qos}")
            client.subscribe(topic, qos=qos)

    def on_message(self, client, userdata, msg):
        try:
            print(f"Message received on topic {msg.topic}")
            topic_name = msg.topic
            parts = topic_name.split('/')
            
            if parts[1] == "water_ms":
                reqdata = DotDictLibrary(json.loads(msg.payload.decode('utf-8')))
                asyncio.run(WaterController.get_weather_data(reqdata, parts[2], parts[3]))
            elif parts[1] == "settings" and parts[2] == "AA":
                reqdata = DotDictLibrary(json.loads(msg.payload.decode('utf-8')))
                print("Received settings data:", reqdata.key)
            elif parts[1] == "registration" and parts[2] == "AA":
                reqdata = DotDictLibrary(json.loads(msg.payload.decode('utf-8')))
                paramsdata = [
                    DeviceAdd(
                        client_id=1,
                        device=str(reqdata.UID),
                        device_name=str(reqdata.D_NAME),
                        do_channel=1,
                        model='TSWF01',
                        lat=str(reqdata.LAT),
                        lon=str(reqdata.LONG),
                        imei_no=str(reqdata.IMEI),
                        last_maintenance=datetime.now().strftime("%Y-%m-%d")
                    )
                ]
                asyncio.run(DeviceController.add_device(paramsdata))
        except Exception as e:
            print("Error in on_message", e)
    
    def connect(self):
        self.client.connect(self.broker_address, self.broker_port, 60)
        self.client.loop_start()

    def subscribe(self, topics):
        for topic, qos in topics:
            if (topic, qos) not in self.subscriptions:
                self.subscriptions.append((topic, qos))
                if self.client.is_connected():
                    print("Subscribed to topic: ", topic)
                    self.client.subscribe(topic, qos=qos)

    def publish(self, topic, message, qos=0):
        self.client.publish(topic, message, qos=qos)

    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()
        print("Disconnected from MQTT broker")

# Example usage
mqtt_client = MqttLibraryClass("broker_address", 1883, "arist_automation", "Arist@%#$@45354")
mqtt_client.connect()
