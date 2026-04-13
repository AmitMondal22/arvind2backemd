from fastapi import APIRouter, HTTPException,Response, Depends,Request
from controllers.user import UserController
from middleware.MyMiddleware import mw_client,mw_user,mw_user_client
from models.mqtt_model import MqttWfmsDO,MqttAllWfmsDO, MqttPublishDeviceSchedule,MqttPublishDeviceScheduleList, ResetMqttPublishDeviceSchedule, MqttReadSchedule, MqttReadLastData
from utils.date_time_format import get_current_datetime
from Library.DecimalEncoder import DecimalEncoder

from db_model.MASTER_MODEL import select_last_data,insert_data,update_data,select_one_data,custom_select_sql_query
from datetime import timedelta

# from Library.MqttLibrary import mqtt_client, MQTT_TOPIC,publish_energy_message
from Library.MqttLibraryClass import MqttLibraryClass

from controllers.device_to_server import WaterController

from utils.response import errorResponse, successResponse
import json
import time

# from models.mqtt_model import MqttEnergyDeviceData

from hooks.update_event_hooks import update_topics

mqtt_routes = APIRouter()

mqtt_client = MqttLibraryClass("aristautomationmqtt.iotblitz.in", 1883,"arist_automation", "Arist@%%$@45354")
# Connect to the MQTT broker
mqtt_client.connect()



# @mqtt_routes.on_event("startup")
# async def startup_event():
#     mqtt_client.subscribe([("hello", 0),("hello1", 0)])
 
@mqtt_routes.on_event("startup")
async def startup_event():
    await subscribe_topics()

# =========================================================
# MQTT TOPIC

async def subscribe_topics():
    try:
        data = await update_topics()
        print("Subscribing to topics:", data)
        mqtt_client.subscribe(data)
    except Exception as e:
        print("Error in subscribing topics:", e)
        
# =========================================================
# @mqtt_routes.post("/publish/")
# async def publish_message(message_data: MqttEnergyDeviceData):
#     try:
#         # mqtt_client = MqttLibraryClass("test/topic")
#         mqtt_client.publish(f"ems/{message_data.ib_id}/{message_data.device}", message_data.json(), qos=0)
#         return {"message": "Message published successfully"}
#     except Exception as e:
#         return {"error": str(e)}


def encode_do_to_frame(device_id, do_states):
    if len(do_states) != 8:
        raise ValueError("Must provide exactly 8 DO states")

    # 1) Extract last 4 digits of device_id as UID
    digits = ''.join(ch for ch in device_id if ch.isdigit())
    
    print(digits)
    if len(digits) < 4:
        raise ValueError("Device ID must have at least 4 digits")
    uid = int(digits[-4:])

    uid_hi = (uid >> 8) & 0xFF
    uid_lo = uid & 0xFF

    # 2) Base-3 pack DO states into 24-bit integer
    v = 0
    for d in do_states:
        if d not in (0, 1, 2):
            raise ValueError("DO states must be 0, 1, or 2")
        v = v * 3 + d
        
    print(">>>.",v)
    b2 = (v >> 16) & 0xFF
    b3 = (v >> 8) & 0xFF
    b4 = v & 0xFF

    # 3) Build payload (5 bytes)
    payload = [uid_hi, uid_lo, b2, b3, b4]

    # 4) Convert to 10-char hex
    hexPayload = ''.join(f"{b:02X}" for b in payload)

    # 5) Final LoRa frame
    return f"*LO,{hexPayload}#"



@mqtt_routes.post("/publish_all_digital_output", dependencies=[Depends(mw_user_client)])
async def publish_message(request: Request, message_data: MqttAllWfmsDO):
    try:
        user_data=request.state.user_data 
        # Sort the list by do_no just in case it's not ordered
        sorted_dos = sorted(message_data.do, key=lambda x: x.do_no)
        # Build the doData string
        # doData = ','.join(str(do.do_status) for do in sorted_dos)
        doData = [int(do.do_status) for do in sorted_dos]
        doData = doData[:-1]
        # doData = str(doData)
        # Example print or return
        print(f"doData = {doData}")
        
        condi=f"device='{message_data.device}'"
        deviceData = select_last_data("md_device","gateway_id",condi)
        print("}{}{}{}{}{}{}",deviceData)
        getway_id = deviceData['gateway_id']
        
        print("...............",message_data.device,doData)
        
        srdata = encode_do_to_frame(message_data.device,doData)
        # srdata=f"*OPADO, {message_data.device},{doData}#"
        print(srdata)
        print(f"/ST/'{getway_id}'")
        mqtt_client.publish(f"/ST/{getway_id}", srdata, qos=0)
        resdata = successResponse(user_data, message="Message published successfully")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error")


@mqtt_routes.post("/publish_digital_output", dependencies=[Depends(mw_user_client)])
async def publish_message(request: Request, message_data: MqttWfmsDO):
    try:
        user_data=request.state.user_data 
        
        # condi=f"device_id = '{message_data.device_id}' AND device='{message_data.device}' AND client_id = '{user_data['client_id']}'"

        # find_device=select_last_data("td_water_data", "do_status", condi , "created_at")
        
        # stt = find_device['do_status']
        stt = "000000000"
        do_no = (message_data.do_no-1)  # Position to replace (0-based index)
        do_status = message_data.do_status  # New value to insert at the specified position
        
        print("do_status",do_no,do_status, stt)

        # Convert the string to a list to allow modification
        stt_list = list(stt)
        # stt_list = [int(char) for char in stt_list]
        # stt_list = [str(int(char) + 1) for char in stt_list]
        # Replace the value at the specified position
        stt_list[do_no] = str(do_status)
        # Convert the list back to a string
        stt = ",".join(stt_list)
        srdata=f"*OPADO, {message_data.device},{stt}#"
        
            
            # formatted_number = ",".join(str(message_data.digital_output))
            # # srdata=f"*OPADO, ,{formatted_number}#"
            # srdata=f"*OPADO, ,1,2,0,0#"
            # *OPADO, ,2,2,2,2,2,2,1,1#
        mqtt_client.publish(f"/WFMS/{message_data.device}", srdata, qos=0)
        resdata = successResponse(user_data, message="Message published successfully")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error")


def days_to_mask(days_str: str) -> int:
    day_map = {
        "sun": 0,
        "mon": 1,
        "tue": 2,
        "wed": 3,
        "thu": 4,
        "fri": 5,
        "sat": 6
    }

    mask = 0
    for d in days_str.lower().split(","):
        d = d.strip()
        if d in day_map:
            mask |= (1 << day_map[d])

    return mask
    

# @mqtt_routes.post("/publish_schedule", dependencies=[Depends(mw_user_client)])
# async def publish_message(request: Request, message_data: MqttPublishDeviceSchedule):
#     try:
#         user_data=request.state.user_data
#         print("???????????????/",user_data)
#         one_on_time=message_data.one_on_time
#         one_off_time=message_data.one_off_time
#         two_on_time=message_data.two_on_time
#         two_off_time=message_data.two_off_time
#         one_on_hr, one_on_min, _ = one_on_time.split(":")
#         one_off_hr, one_off_min, _ = one_off_time.split(":")
#         two_on_hr, two_on_min, _ = two_on_time.split(":")
#         two_off_hr, two_off_min, _ = two_off_time.split(":")
        
#         user_id = await insert_updatesheduling(user_data,message_data)
        
#         dotype = 4 if message_data.do_type == 0 else 5
        
#         # DeviceID=0001 | DO Type=2 | Channel=4 | ON=06:30 | OFF=08:45 | Days=All (Sun–Sat)  
#         # *LC,   0001 2 4 06 1E 08 2D 7F#
#         pubdata=f"^LC,{message_data.device},{dotype},{message_data.do_no-1},{one_on_hr},{one_on_min},{one_off_hr},{one_off_min}*"
        
        
#         # pubdata=f"*CONFIG,{message_data.device},{message_data.do_no-1},{dotype},{one_on_hr},{one_on_min},{one_off_hr},{one_off_min},{two_on_hr},{two_on_min},{two_off_hr},{two_off_min},{message_data.datalog_sec*60}#"
        
#         # srdata=f"*OPADO, ,1,2,0,0#"
#         # //*DOTIM,UID,D0-INDEX,DO_TYPE,ON-HR,ON-MIN,OFF-HR,OFF-MIN,ON1-HR,ON1-MIN,OFF1-HR,OFF1-MIN#
#         # //*DOTIM, ,0,4,16,00,17,00,18,00,19,00#
        
        
#         # mqtt_client.publish(f"/WFMS/{message_data.device}", pubdata, qos=0)
        
#         resdata = successResponse(user_id, message="Message published successfully")
#         return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
#     except ValueError as ve:
#         # If there's a ValueError, return a 400 Bad Request with the error message
#         raise HTTPException(status_code=400, detail=str(ve))
#     except Exception as e:
#         # For any other unexpected error, return a 500 Internal Server Error
#         raise HTTPException(status_code=500, detail="Internal server error")
    
# async def publish_settings(message_data: MqttPublishDeviceSchedule):


@mqtt_routes.post("/publish_schedule", dependencies=[Depends(mw_user_client)])
async def publish_message(request: Request, message_data: MqttPublishDeviceSchedule):
    try:
        user_data = request.state.user_data

        # Time split
        one_on_hr = message_data.one_on_time.hour
        one_on_min = message_data.one_on_time.minute

        one_off_hr = message_data.one_off_time.hour
        one_off_min = message_data.one_off_time.minute

        two_on_hr = message_data.two_on_time.hour
        two_on_min = message_data.two_on_time.minute

        two_off_hr = message_data.two_off_time.hour
        two_off_min = message_data.two_off_time.minute
        
        
        on_hr_hex = f"{one_on_hr:02X}"
        on_min_hex = f"{one_on_min:02X}"
        off_hr_hex = f"{one_off_hr:02X}"
        off_min_hex = f"{one_off_min:02X}"

        # Convert values
        device_id_int = int(message_data.device)  # e.g. "0002" → 2
        rxUID_hex = f"{device_id_int:04X}"        # 2 bytes HEX

        do_type = message_data.do_type
        channel = (message_data.do_no - 1)
        
        
        if do_type == 0:
            do_type_mapped = 4
        elif do_type == 1:
            do_type_mapped = 5
        else:
            raise ValueError("Invalid do_type")

        # Byte 2 (do_type + channel)
        byte2 = ((do_type_mapped & 0x0F) << 4) | (channel & 0x0F)

        # Time HEX
        on_hr_hex = f"{int(one_on_hr):02X}"
        on_min_hex = f"{int(one_on_min):02X}"
        off_hr_hex = f"{int(one_off_hr):02X}"
        off_min_hex = f"{int(one_off_min):02X}"

        # Days mask
        days_mask = days_to_mask(message_data.days)
        days_hex = f"{days_mask:02X}"

        # Final payload (8 bytes)
        hex_payload = (
            f"{rxUID_hex}"
            f"{byte2:02X}"
            f"{on_hr_hex}"
            f"{on_min_hex}"
            f"{off_hr_hex}"
            f"{off_min_hex}"
            f"{days_hex}"
        )

        pubdata = f"*LC,{hex_payload}#"

        print("Generated:", pubdata)
        
        
        userdata=request.state.user_data
        condition = f"client_id={userdata['client_id']} AND device_id = {message_data.device_id}"
        data = select_one_data("md_device","gateway_id", condition,order_by="device_id DESC")

        

        mqtt_client.publish(f"/ST/{data['gateway_id']}", pubdata, qos=1)

        user_id = await insert_updatesheduling(user_data, message_data)
        
        
        

        return Response(
            content=json.dumps(successResponse(user_id, message="Message published successfully")),
            media_type="application/json",
            status_code=200
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def remove_none_fields(data: dict):
    return {k: v for k, v in data.items() if v is not None}
    
async def insert_updatesheduling(user_data, message_data: MqttPublishDeviceSchedule):

    print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>LLLLLLLLLLLL")
    current_datetime = get_current_datetime()

    condi = f"device='{message_data.device}' AND do_no={message_data.do_no} AND client_id={user_data['client_id']}"

    find_device_schedule = select_last_data("device_schedule", "schedule_id", condi, "created_at")
    print(">>>>>>>>>>>>>>>>>", find_device_schedule)

    # Convert Pydantic model → dict
    data_dict = message_data.dict()

    # Remove None fields
    clean_data = remove_none_fields(data_dict)

    if find_device_schedule is not None:
        # UPDATE → only non-null fields
        columns = {
            **clean_data,
            "updated_at": current_datetime,
            "created_by": user_data['user_id']
        }

        user_id = update_data("device_schedule", columns, condi)

    else:
        # INSERT → build dynamically
        insert_dict = {
            "client_id": user_data['client_id'],
            "device": message_data.device,
            "do_no": message_data.do_no,
            "created_by": user_data['user_id'],
            "created_at": current_datetime,
            **clean_data
        }

        # Remove duplicate keys if any
        insert_dict.pop("schedule_id", None)
        insert_dict.pop("organization_id", None)

        columns = ", ".join(insert_dict.keys())

        # Handle NULL properly
        values = []
        for v in insert_dict.values():
            if v is None:
                values.append("NULL")
            elif isinstance(v, str):
                values.append(f"'{v}'")
            else:
                values.append(str(v))

        values_str = ", ".join(values)

        user_id = insert_data("device_schedule", columns, values_str)

    print("KKKKKKKKKKKK", user_id)

    await send_readsettings(user_data['client_id'], message_data.device, message_data.do_no)

    return user_id





async def send_readsettings(client_id, device_id, dono):
    try:
        # Lazy import inside the function
        from Library.WsConnectionManagerManyDeviceTypes import WsConnectionManagerManyDeviceTypes
        manager = WsConnectionManagerManyDeviceTypes()
        # background_tasks = BackgroundTasks()
        
        from routes.ws_routes import sennd_ws_message 
        
        
        condition=f"device='{device_id}' AND client_id = {client_id}"
        select="device_id, client_id, device, device_name, DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s') AS created_at"
        data = select_one_data("md_device",select, condition,None)
        
                
        custom_sql=f""" SELECT * 
                        FROM device_schedule 
                        WHERE 
                            device = '{device_id}'
                            AND client_id = {client_id}
                            AND do_no = {dono}
                        ORDER BY schedule_id DESC  
                        LIMIT 1"""
        lastdata=custom_select_sql_query(custom_sql,None)
        print(";;;;",lastdata)
        twodata={"shedulingdata":lastdata}
        await sennd_ws_message("WFMS_SETTINGS",client_id,data['device_id'],device_id, json.dumps(twodata, cls=DecimalEncoder))
       
        return json.dumps(lastdata, cls=DecimalEncoder)
    except Exception as e:
        raise ValueError("Could not fetch data",e)





@mqtt_routes.post("/reset_sheduling", dependencies=[Depends(mw_user_client)])
async def reset_sheduling(request: Request,message_data: ResetMqttPublishDeviceSchedule):
    pubdata=f"*TORST,{message_data.device}#"
    
    # srdata=f"*OPADO, ,1,2,0,0#"
    # //*DOTIM,UID,D0-INDEX,DO_TYPE,ON-HR,ON-MIN,OFF-HR,OFF-MIN,ON1-HR,ON1-MIN,OFF1-HR,OFF1-MIN#
    # //*DOTIM, ,0,4,16,00,17,00,18,00,19,00#
    
    
    mqtt_client.publish(f"/WFMS/{message_data.device}", pubdata, qos=0)
    return pubdata
    

@mqtt_routes.post("/read_sheduling", dependencies=[Depends(mw_user_client)])
async def reset_sheduling(request: Request, message_data: MqttReadSchedule):
    try:
        # Convert Device ID → HEX (4 digits)
        device_id_int = int(message_data.device)   # "0050" → 50
        print("device_id_int-------------------",message_data)
        uid_hex = f"{device_id_int:04X}"           # → 0032

        # Channel → HEX (2 digits)
        channel = message_data.do_no
        ch_hex = f"{channel:02X}"                  # → 01

        # Payload
        payload = f"{uid_hex}{ch_hex}"

        # Command selection
        if message_data.request_type == 0:
            cmd = "RT"   # Read Timer
        else:
            cmd = "RM"   # Read Mode

        pubdata = f"*{cmd},{payload}#"
        
        
        userdata=request.state.user_data
        print(">>>>>>>>>>>>>>>>>>>>>>>>>",message_data)
        condition = f"client_id={userdata['client_id']} AND device_id = {message_data.device_id}"
        data = select_one_data("md_device","gateway_id", condition,order_by="device_id DESC")

        # Publish MQTT
        mqtt_client.publish(f"/ST/{data['gateway_id']}", pubdata, qos=0)

        print("Published:", pubdata)
        
        print(">>>>>>>>>>>>>>>>>>>>>>>>>||||||||||||||",userdata['client_id'], message_data.device, channel)
        await send_readsettings(userdata['client_id'], message_data.device, channel)

        return {
            "status": "success",
            "command": pubdata
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    
@mqtt_routes.post("/read_last_data", dependencies=[Depends(mw_user_client)])
async def read_last_data(request: Request, message_data: MqttReadLastData):
    try:
        # 🔹 Convert device_id → HEX (4 digit uppercase)
        device_id_int = int(message_data.device_id)
        nid = format(device_id_int, '04X')  # Example: 10 → 000A

        # 🔹 Create payload
        cmd = "LRDT"
        pubdata = f"*{cmd},{nid}#"
        # pubdata = f"*{cmd},{message_data.device_id}#"

        # 🔹 Get user data
        userdata = request.state.user_data

        # 🔹 Fetch gateway_id
        condition = f"client_id={userdata['client_id']} AND device_id={message_data.device_id}"
        data = select_one_data(
            "md_device",
            "gateway_id",
            condition,
            order_by="device_id DESC"
        )

        if not data:
            raise HTTPException(status_code=404, detail="Device not found")

        # 🔹 Publish MQTT
        topic = f"/ST/{data['gateway_id']}"
        mqtt_client.publish(topic, pubdata, qos=0)

        print("Published:", pubdata, "→ Topic:", topic)
        
        
        columns={"device_status":"OFFLINE"}
        condition = f"device_id = {message_data.device_id}"
        update_data("md_device",columns,condition)

        return {
            "status": "success",
            "command": pubdata,
            "topic": topic
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    
    
        
def convert_timedelta(obj):
    if isinstance(obj, timedelta):
        # Convert timedelta to total seconds or a string representation
        return str(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

@mqtt_routes.post("/publish_schedule_data", dependencies=[Depends(mw_user_client)])
async def publish_schedule_data(request: Request, message_data: MqttPublishDeviceScheduleList):
    try:
        user_data=request.state.user_data
        condition=f"device='{message_data.device}' AND do_no ={message_data.do_no} AND client_id = {user_data['client_id']}"
        select="schedule_id, client_id, device, do_type, datalog_sec, do_no, one_on_time, one_off_time, two_on_time, two_off_time,days, created_by, DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s') AS created_at, DATE_FORMAT(updated_at, '%Y-%m-%d %H:%i:%s') AS updated_at"
        data = select_one_data("device_schedule",select, condition,order_by="schedule_id DESC")
        print(">>>>>>>>>>>>>>>>>",data)
        resdata = successResponse(data, message="Shedule successfully")
        print('????????????????????',json.dumps(resdata,default=convert_timedelta))
        return Response(content=json.dumps(resdata,default=convert_timedelta), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error")
