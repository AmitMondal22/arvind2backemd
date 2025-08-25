import paho.mqtt.client as mqtt
from controllers.device_to_server import EnergyController,UpsController
from Library.DotDictLibrary import DotDictLibrary
import json
import asyncio

class AutoRegistrationMqttLibraryClass:
    def __init__(self, broker_address, broker_port):
        self.client = mqtt.Client()
        self.broker_address = broker_address
        self.broker_port = broker_port
        # Callback functions
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.subscriptions = []

    def on_connect(self, client, userdata, flags, rc):
        for topic, qos in self.subscriptions:
            client.subscribe(topic, qos=qos)

    def on_message(self, client, userdata, msg):
        reqdata=DotDictLibrary(json.loads(msg.payload))
        if reqdata.device == "UPS":
            asyncio.run(UpsController.get_ups_data(reqdata))
        elif reqdata.device == "ENE":
            asyncio.run(EnergyController.get_energy_data(reqdata))
    

    def connect(self):
        self.client.connect(self.broker_address, self.broker_port, 60)
        self.client.loop_start()

    def subscribe(self, topics):
        for topic, qos in topics:
            self.subscriptions.append((topic, qos))
            if self.client.is_connected():
                self.client.subscribe(topic, qos=qos)

    def publish(self, topic, message, qos=0):
        self.client.publish(topic, message, qos=qos)

    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()