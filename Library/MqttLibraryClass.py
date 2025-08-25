import paho.mqtt.client as mqtt
from controllers.device_to_server import WaterController
from routes import mqtt_routes
from Library.DotDictLibrary import DotDictLibrary
from utils.date_time_format import get_current_datetime
from models.mqtt_model import MqttPublishDeviceSchedule
from models.device_data_model import DeviceAdd, WaterDeviceData
from controllers.admin import DeviceController
import json
import asyncio
from datetime import datetime
from utils.filter_Mqtt_Data import parse_lora_packet 

class MqttLibraryClass:
    def __init__(self, broker_address, broker_port,username, password):
        self.client = mqtt.Client()
        self.client.username_pw_set(username, password)
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
            topic_name=msg.topic
            parts = topic_name.split('/')
            # reqdata=DotDictLibrary(json.loads(msg.payload))
            # if parts[0] == "ums":
            if parts[1] == "ARVIND":
                # print(json.loads(msg.payload.decode('utf-8')))
                reqdata=DotDictLibrary(json.loads(msg.payload.decode('utf-8')))
                # device_id = reqdata.uid + reqdata.nid
                device_id = reqdata.nid
                print(">>>>>>>>",device_id)
                gateway_id = reqdata.uid + "0000"
                imei_id = reqdata.imei
                cid_id = 1
                date_time = reqdata.dt
                
                # Parse the date string; notice that the year is 2 digits (%y)
                device_dt = datetime.strptime(date_time, "%d-%m-%y %H:%M:%S")

                # Format it to "%Y-%m-%d"
                formatted_date = device_dt.strftime("%Y.%m.%d")
                formatted_itme = device_dt.strftime("%H:%M:%S")
                
                fwver = reqdata.fwver
                
                # node_msg = reqdata.msg

                # encode_data = parse_lora_packet(node_msg)
                
                # bits = encode_data['DO_status']
                # bit_string = ''.join(map(str, bits))
                # if 'DeviceID' in encode_data and encode_data['DeviceID'] is not None:
                bit_string = ''.join(map(str, reqdata.sw))

                print("[mqtt res]", bit_string)
                
                deviceData =  WaterDeviceData(
                    UID =  device_id,
                    DT = formatted_date,
                    TIME = formatted_itme,
                    TW = 0.0,
                    A1 = reqdata.p1,
                    A2 = reqdata.p2,
                    TOT1 = 0,
                    TOT2 = 0,
                    DO = bit_string,
                    BAT_V = reqdata.batV,
                )

                asyncio.run(WaterController.get_weather_data(deviceData,cid_id,device_id))
                asyncio.run(WaterController.update_device(device_id,imei_id,gateway_id)) 
                    
                    
                    
            elif parts[1] == "settings":
                # if parts[2] == "AA":
                reqdata=DotDictLibrary(json.loads(msg.payload.decode('utf-8')))
                print(msg.payload.decode('utf-8'))
                
                settingsData = MqttPublishDeviceSchedule(
                                    device=str(reqdata.UID),
                                    do_type=int(0 if reqdata.DOTYPE == 4 else 1 if reqdata.DOTYPE == 5 else 0),  # 4 auto, 5 manual
                                    do_no=int(reqdata.CH + 1),
                                    one_on_time=str(f"{reqdata.ONHR}:{reqdata.ONMIN}:00"),  # Example: "17:45:05"
                                    one_off_time=str(f"{reqdata.OFFHR}:{reqdata.OFFMIN}:00"),
                                    two_on_time=str(f"{reqdata.ON1HR}:{reqdata.ON1MIN}:00"),
                                    two_off_time=str(f"{reqdata.OFF1HR}:{reqdata.OFF1MIN}:00"),
                                    datalog_sec=int(reqdata.LOG_S / 60)
                                )
                user_data = {}  # Create an empty dictionary
                user_data['client_id'] = parts[2]  # Assign 123 to 'client_id'
                user_data['user_id'] = 0  # Assign 123 to 'client_id'
                print("RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR",settingsData,user_data['client_id'])
                asyncio.run(mqtt_routes.insert_updatesheduling(user_data,settingsData)) 
            # elif parts[1] == "registration":
                # if parts[2] == "AA":
                #     reqdata=DotDictLibrary(json.loads(msg.payload.decode('utf-8')))
                    
                #     paramsdata = [
                #             DeviceAdd(
                #                 client_id=1,
                #                 device=str(reqdata.UID),
                #                 device_name=str(reqdata.D_NAME),
                #                 do_channel=1,
                #                 model='TSWF01',
                #                 lat=str(reqdata.LAT),
                #                 lon=str(reqdata.LONG),
                #                 imei_no=str(reqdata.IMEI),
                #                 last_maintenance=datetime.now().strftime("%Y-%m-%d")  # Get current date
                #             )
                #         ]
                        
                #     asyncio.run(DeviceController.add_device(paramsdata)) 

                
        except Exception as e:
            print("Error in on_message",e)
    

    def connect(self):
        self.client.connect(self.broker_address, self.broker_port, 60)
        self.client.loop_start()

    # def subscribe(self, topics):
    #     for topic, qos in topics:
    #         self.subscriptions.append((topic, qos))
    #         if self.client.is_connected():
    #             print("jdsbcjh")
    #             self.client.subscribe(topic, qos=qos)
    def subscribe(self, topics):
        for topic, qos in topics:
            # Check if the topic is already subscribed
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