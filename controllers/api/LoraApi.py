from fastapi import HTTPException
import requests
import json
import base64
from typing import Any
from fastapi.encoders import jsonable_encoder
import time
from utils.date_time_format import get_current_datetime
from db_model.MASTER_MODEL import  insert_data,select_one_data,update_data



# import grpc
# from chirpstack_api import api


def encode_to_base64(data):
    if isinstance(data, str):
        # Convert string to bytes
        data_bytes = data.encode('utf-8')
    elif isinstance(data, dict):
        # Convert dictionary to JSON string and then to bytes
        data_json = json.dumps(data)
        data_bytes = data_json.encode('utf-8')
    else:
        raise ValueError("Data must be a string or a dictionary")

    # Encode bytes to base64
    base64_bytes = base64.b64encode(data_bytes)
    # Convert base64 bytes to string
    base64_string = base64_bytes.decode('utf-8')
    
    return base64_string


api_key = 'NNSXS.SKSGTTX6IIDKM7THS3RATJBRL5ZHMKO4A6ZBGXY.WVF3AVQVGOAVWK3E7CIGPVLXPHREIL5D5FXJCK5E2BCKZWL6PAVA'
application_id = 'streetlighttechavo'
device_id = 'eui-0080e115002b5637'

def decode_base64(encoded_str):
    """Decode a Base64 encoded string."""
    return base64.b64decode(encoded_str).decode('utf-8')


async def uplink(user, params):
    try:
        return "data"
    except Exception as e:
        raise e
    


# def send_downlink(user: Any, params: Any) -> dict:
#     # Replace these values with your actual values
   

#     url = f'https://eu1.cloud.thethings.network/api/v3/as/applications/{application_id}/devices/{device_id}/packages/storage/uplink_message'
#     headers = {
#         'Authorization': f'Bearer {api_key}',
#         'Accept': 'text/event-stream'
#     }
#     # Use params argument provided to the method
#     response_params = params or {
#         'limit': 10,
#         'after': '2020-08-20T00:00:00Z'
#     }

#     response = requests.get(url, headers=headers, params=response_params)

#     if response.status_code == 200:
#         return response.json()  # Convert the response to a dictionary
#     else:
#         # Optionally, handle errors more gracefully
#         return {'error': f'Error: {response.status_code} - {response.text}'}
        
    

# async def webhooks_send_downlink():
   
#     frm_payload = "R1, ,1,10,22,17,30,24,7,2024,16,07,33,ZZ"

#     # Encode the string to bytes first
#     frm_payload_bytes = frm_payload.encode('utf-8')

#     # Base64 encode the bytes
#     encoded_payload = base64.b64encode(frm_payload_bytes)
#     encoded_string = encoded_payload.decode('utf-8')
    
#     # time.sleep(2)
#     url = "https://eu1.cloud.thethings.network/api/v3/as/applications/streetlighttechavo/webhooks/test/devices/eui-0080e115002b5637/down/replace"

#     print(encoded_string)
#     print("///////////////////////")
#     payload = json.dumps({
#     "downlinks": [
#         {
#         "frm_payload": encoded_string,
#         "f_port": 15,
#         "priority": "NORMAL"
#         }
#     ]
#     })
#     print(payload)
#     headers = {
#     'Content-Type': 'application/json',
#     'Authorization': 'Bearer NNSXS.SKSGTTX6IIDKM7THS3RATJBRL5ZHMKO4A6ZBGXY.WVF3AVQVGOAVWK3E7CIGPVLXPHREIL5D5FXJCK5E2BCKZWL6PAVA'
#     }

#     response = requests.request("POST", url, headers=headers, data=payload)

#     print(response.text)
#     return{'success': 'Downlink sent successfully'}




async def webhooks_send_downlink_test(dev_eui: str, payload: str):
    try:
        base64_encoded = encode_to_base64(payload)
        print(base64_encoded)
        url = f'http://lora.techavo.in:8080/api/devices/{dev_eui}/queue'
        # url = f'http://lora.techavo.in:8080/api/devices/{dev_eui}/downlinks'
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Grpc-Metadata-Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhcGlfa2V5X2lkIjoiYTBmOTUzZTQtNWRlMi00NDhiLWJiMmQtYWQxOTM3OTMxMGRlIiwiYXVkIjoiYXMiLCJpc3MiOiJhcyIsIm5iZiI6MTcyMjk0NDE5Miwic3ViIjoiYXBpX2tleSJ9.ep4D5-YaGQru0o0ur77TK5CuwtFFNPlQaSu0zfrw6Lo'
        }

        data = {
            "deviceQueueItem": {
                "confirmed": True,
                "data": base64_encoded,
                "fPort": 2
            }
        }
        delete= requests.delete(url, headers=headers)
        time.sleep(0.5)
        response = requests.post(url, headers=headers, json=data)
        print(response.text)
        print(delete.text)
        print("?????????????./././../././././/.")
        return True
    except Exception as e:
        print(e)
        return False
    

async def webhooks_send_downlink_test_menual(dev_eui: str, payload: str):
    try:
        base64_encoded = encode_to_base64(payload)
        print(base64_encoded)
        url = f'http://lora.techavo.in:8080/api/devices/{dev_eui}/queue'
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Grpc-Metadata-Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhcGlfa2V5X2lkIjoiYTBmOTUzZTQtNWRlMi00NDhiLWJiMmQtYWQxOTM3OTMxMGRlIiwiYXVkIjoiYXMiLCJpc3MiOiJhcyIsIm5iZiI6MTcyMjk0NDE5Miwic3ViIjoiYXBpX2tleSJ9.ep4D5-YaGQru0o0ur77TK5CuwtFFNPlQaSu0zfrw6Lo'
        }

        data = {
            "deviceQueueItem": {
                "confirmed": True,
                "data": base64_encoded,
                "fPort": 2
            }
        }

        response = requests.post(url, headers=headers, json=data)
        print(response.text)
        print("?????????????")
        return True
    except Exception as e:
        print(e)
        return False
    

async def update_device_schedule_settings(client_id,decodedev_eui,device_mode,sunrise_hour,sunrise_min,sunset_hour,sunset_min,datalog_interval):
    try:
        current_datetime = get_current_datetime()
        select="st_sl_settings_id, device_id, device, client_id, sunrise_hour, sunrise_min, sunset_hour, sunset_min, created_by"
        conditions=f"device = '{decodedev_eui}' AND client_id = {client_id} "

        find_devices=select_one_data("st_sl_settings_scheduling", select, conditions,None)
        print(find_devices)

        if find_devices is None or not find_devices:
            print("No devices found")
            conditionsdev=f"device = '{decodedev_eui}' AND client_id = {client_id} "

            find_devices_device_id=select_one_data("md_device", "device_id", conditionsdev,None)
            

            if find_devices_device_id is not None:
                db_device_id=find_devices_device_id['device_id'] 
                columns="device_id,device, client_id,device_mode, sunrise_hour, sunrise_min, sunset_hour, sunset_min,datalog_interval, created_by, created_at"
                row_data= f"{db_device_id},'{decodedev_eui}', {client_id}, '{device_mode}', '{sunrise_hour}', '{sunrise_min}', '{sunset_hour}', '{sunset_min}',{datalog_interval}, {0}, '{current_datetime}'"
                insdata=insert_data("st_sl_settings_scheduling", columns, row_data)
        # else:
        #     if sunrise_hour != find_devices['sunrise_hour'] or sunrise_min != find_devices['sunrise_min'] or sunset_hour != find_devices['sunset_hour'] or sunset_min != find_devices['sunset_min']:
        #         print("Error inserting")
        #         setvalue={"sunrise_hour": sunrise_hour, "sunrise_min": sunrise_min, "sunset_hour": sunset_hour, "sunset_min": sunset_min, "updated_at": current_datetime}
        #         print("Requestdata",setvalue , conditions)
        #         insdata=update_data("st_sl_settings_scheduling",setvalue , conditions)

        return True
    except Exception as e:
        raise e




# def get_devices(application_id):
#     """Fetch all devices for a given application ID."""
#     url = f'http://lora.techavo.in:8080/api/applications/{application_id}/devices'
#     headers = {
#         'Grpc-Metadata-Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhcGlfa2V5X2lkIjoiYTBmOTUzZTQtNWRlMi00NDhiLWJiMmQtYWQxOTM3OTMxMGRlIiwiYXVkIjoiYXMiLCJpc3MiOiJhcyIsIm5iZiI6MTcyMjk0NDE5Miwic3ViIjoiYXBpX2tleSJ9.ep4D5-YaGQru0o0ur77TK5CuwtFFNPlQaSu0zfrw6Lo'
#     }
#     response = requests.get(url, headers=headers)
#     if response.status_code == 200:
#         print("..................................................", response)
#         return response.json().get('result', [])
#     else:
#         print(f"Failed to fetch devices: {response.status_code}")
#         return []

async def get_devices(application_id):
    # url = f'http://lora.techavo.in:8080/api/devices?applicationID={application_id}'
    # headers = {
    #     'Grpc-Metadata-Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhcGlfa2V5X2lkIjoiYTBmOTUzZTQtNWRlMi00NDhiLWJiMmQtYWQxOTM3OTMxMGRlIiwiYXVkIjoiYXMiLCJpc3MiOiJhcyIsIm5iZiI6MTcyMjk0NDE5Miwic3ViIjoiYXBpX2tleSJ9.ep4D5-YaGQru0o0ur77TK5CuwtFFNPlQaSu0zfrw6Lo'
    # }

    # response = requests.get(url, headers=headers)
    # if response.status_code == 200:
    #     # print(".................................",response.text)
    #     return response.json()
    # else:
    #     print(f"Error fetching devices: {response.status_code}")
    #     return None
    
    url = f'http://lora.techavo.in:8080/api/devices?applicationID=6'
    # url = f'http://lora.techavo.in:8080/api/devices'
    headers = {
        # 'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhcGlfa2V5X2lkIjoiYTBmOTUzZTQtNWRlMi00NDhiLWJiMmQtYWQxOTM3OTMxMGRlIiwiYXVkIjoiYXMiLCJpc3MiOiJhcyIsIm5iZiI6MTcyMjk0NDE5Miwic3ViIjoiYXBpX2tleSJ9.ep4D5-YaGQru0o0ur77TK5CuwtFFNPlQaSu0zfrw6Lo'
        'Grpc-Metadata-Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhcGlfa2V5X2lkIjoiYTBmOTUzZTQtNWRlMi00NDhiLWJiMmQtYWQxOTM3OTMxMGRlIiwiYXVkIjoiYXMiLCJpc3MiOiJhcyIsIm5iZiI6MTcyMjk0NDE5Miwic3ViIjoiYXBpX2tleSJ9.ep4D5-YaGQru0o0ur77TK5CuwtFFNPlQaSu0zfrw6Lo'
    }
    params = {
        'applicationID': application_id
    }

    response = requests.get(url, headers=headers, params=params)
    print("response",response)

    # response = requests.get(url, headers=headers)
    if response.status_code == 200:
        devices = response.json().get('result', [])
        print("devices",devices)
        if devices:
            dev_euis = [device['devEUI'] for device in devices]
            return dev_euis
        else:
            return "No devices found for the specified application."
    else:
        return f"Failed to retrieve devices: {response.status_code} - {response.text}"
    
    



def send_downlink(dev_eui_encoded, data, fPort):
    """Send downlink data to a device."""
    dev_eui = decode_base64(dev_eui_encoded)
    url = f'http://lora.techavo.in:8080/api/devices/{dev_eui}/queue'
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Grpc-Metadata-Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhcGlfa2V5X2lkIjoiYTBmOTUzZTQtNWRlMi00NDhiLWJiMmQtYWQxOTM3OTMxMGRlIiwiYXVkIjoiYXMiLCJpc3MiOiJhcyIsIm5iZiI6MTcyMjk0NDE5Miwic3ViIjoiYXBpX2tleSJ9.ep4D5-YaGQru0o0ur77TK5CuwtFFNPlQaSu0zfrw6Lo'
    }
    payload = {
        "deviceQueueItem": {
            "confirmed": True,
            "data": data,  # Base64 encoded payload data
            "fPort": fPort
        }
    }
    response = requests.post(url, headers=headers, json=payload)
    print(f"Downlink response for {dev_eui}: {response.status_code}")
    return response.status_code == 200



async def manage_devices(application_id, downlink_data, fPort):
    devices = await get_devices(application_id)
    print("devices", devices)
    if devices:
        for device in devices['result']:
            print("?????????????",device)
    # for device in devices:
    #     device_id = device['id']
    #     dev_eui_encoded = device['devEUI']
        
    #     # Send downlink
    #     send_downlink(dev_eui_encoded, downlink_data, fPort)