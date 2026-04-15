from fastapi import APIRouter, HTTPException,Response, Depends,Request
from controllers.user import UserController
from middleware.MyMiddleware import mw_client,mw_user,mw_user_client
from models.mqtt_model import MqttWfmsDO,MqttAllWfmsDO, MqttPublishDeviceSchedule,MqttPublishDeviceScheduleList, ResetMqttPublishDeviceSchedule, MqttReadSchedule, MqttReadLastData
from utils.date_time_format import get_current_datetime
from Library.DecimalEncoder import DecimalEncoder

from db_model.MASTER_MODEL import select_last_data,insert_data,update_data,select_one_data,custom_select_sql_query
from datetime import timedelta

from Library.MqttLibraryClass import MqttLibraryClass

from controllers.device_to_server import WaterController

from utils.response import errorResponse, successResponse
import json
from datetime import datetime, time

from hooks.update_event_hooks import update_topics

mqtt_routes = APIRouter()

mqtt_client = MqttLibraryClass("aristautomationmqtt.iotblitz.in", 1883,"arist_automation", "Arist@%%$@45354")
# Connect to the MQTT broker
mqtt_client.connect()

# @mqtt_routes.on_event("startup")
#     mqtt_client.subscribe([("hello", 0),("hello1", 0)])
 
@mqtt_routes.on_event("startup")
async def startup_event():
    await subscribe_topics()

# =========================================================
# MQTT TOPIC

async def subscribe_topics():
    try:
        data = await update_topics()
        mqtt_client.subscribe(data)
    except Exception as e:
        print("Error subscribing to topics:", e)
        
# =========================================================
# @mqtt_routes.post("/publish/")
#     try:
#         return {"message": "Message published successfully"}
#         return {"error": str(e)}

def encode_do_to_frame(device_id, do_states):
    if len(do_states) != 8:
        raise ValueError("Must provide exactly 8 DO states")

    # 1) Extract last 4 digits of device_id as UID
    digits = ''.join(ch for ch in device_id if ch.isdigit())
    
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
        
    b2 = (v >> 16) & 0xFF
    b3 = (v >> 8) & 0xFF
    b4 = v & 0xFF

    # 3) Build payload (5 bytes)
    payload = [uid_hi, uid_lo, b2, b3, b4]

    # 4) Convert to 10-char hex
    hexPayload = ''.join(f"{b:02X}" for b in payload)

    # 5) Final LoRa frame
    return f"*LO,{hexPayload}#"

def encode_gc_frame(device_id, do_states):
    if len(do_states) != 8:
        raise ValueError("Must provide exactly 8 DO states")

    # ✅ Extract numeric part
    digits = ''.join(ch for ch in device_id if ch.isdigit())

    if not digits:
        raise ValueError("Device ID must contain digits")

    # ✅ FIX: avoid last 4-only → use full or safe slice
    uid = int(digits) % 65536   # always fit in 2 bytes

    uid_hi = (uid >> 8) & 0xFF
    uid_lo = uid & 0xFF

    # Base-3 packing
    v = 0
    for d in do_states:
        if d not in (0, 1, 2):
            raise ValueError("DO states must be 0, 1, or 2")
        v = v * 3 + d

    b2 = (v >> 16) & 0xFF
    b3 = (v >> 8) & 0xFF
    b4 = v & 0xFF

    payload = [uid_hi, uid_lo, b2, b3, b4]

    hexPayload = ''.join(f"{b:02X}" for b in payload)

    return f"*GC,{hexPayload}#"

@mqtt_routes.post("/publish_all_digital_output", dependencies=[Depends(mw_user_client)])
async def publish_message(request: Request, message_data: MqttAllWfmsDO):
    try:
        user_data=request.state.user_data 
        # Sort the list by do_no just in case it's not ordered
        sorted_dos = sorted(message_data.do, key=lambda x: x.do_no)
        # Build the doData string
        doData = [int(do.do_status) for do in sorted_dos]
        doData = doData[:-1]
        # Example print or return
        
        condi=f"device='{message_data.device}'"
        deviceData = select_last_data("md_device","gateway_id",condi)
        getway_id = deviceData['gateway_id']
        
        
        srdata = encode_do_to_frame(message_data.device,doData)
        mqtt_client.publish(f"/ST/{getway_id}", srdata, qos=0)
        resdata = successResponse(user_data, message="Message published successfully")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")

@mqtt_routes.post("/publish_digital_output", dependencies=[Depends(mw_user_client)])
async def publish_message(request: Request, message_data: MqttWfmsDO):
    try:
        user_data=request.state.user_data 
        

        
        stt = "000000000"
        do_no = (message_data.do_no-1)  # Position to replace (0-based index)
        do_status = message_data.do_status  # New value to insert at the specified position
        

        # Convert the string to a list to allow modification
        stt_list = list(stt)
        # Replace the value at the specified position
        stt_list[do_no] = str(do_status)
        # Convert the list back to a string
        stt = ",".join(stt_list)
        srdata=f"*OPADO, {message_data.device},{stt}#"
        
            
            # *OPADO, ,2,2,2,2,2,2,1,1#
        mqtt_client.publish(f"/WFMS/{message_data.device}", srdata, qos=0)
        resdata = successResponse(user_data, message="Message published successfully")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
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
    

#     try:
        
        
        
#         # *LC,   0001 2 4 06 1E 08 2D 7F#
        
        
        
#         # //*DOTIM,UID,D0-INDEX,DO_TYPE,ON-HR,ON-MIN,OFF-HR,OFF-MIN,ON1-HR,ON1-MIN,OFF1-HR,OFF1-MIN#
#         # //*DOTIM, ,0,4,16,00,17,00,18,00,19,00#
        
        
        
    

@mqtt_routes.post("/publish_schedule", dependencies=[Depends(mw_user_client)])
async def publish_message(request: Request, message_data: MqttPublishDeviceSchedule):
    try:
        user_data = request.state.user_data

        # -----------------------------
        # Time split
        # -----------------------------
        one_on_hr = message_data.one_on_time.hour
        one_on_min = message_data.one_on_time.minute
        one_off_hr = message_data.one_off_time.hour
        one_off_min = message_data.one_off_time.minute

        # -----------------------------
        # Slot + Enable
        # -----------------------------
        slot = message_data.slot if message_data.slot is not None else 0  # 0–2
        enable = message_data.enable if hasattr(message_data, "enable") else 1  # default enabled

        if slot not in [0, 1, 2]:
            raise ValueError("Slot must be 0, 1, or 2")

        # Byte 3: Enable (bit7) + Slot (bit0–1)
        byte3 = ((1 if enable else 0) << 7) | (slot & 0x03)

        # -----------------------------
        # Device ID → HEX
        # -----------------------------
        device_id_int = int(message_data.device)
        rxUID_hex = f"{device_id_int:04X}"

        # -----------------------------
        # DO Type Mapping
        # -----------------------------
        do_type = message_data.do_type
        channel = (message_data.do_no - 1)

        if do_type == 0:
            do_type_mapped = 4
        elif do_type == 1:
            do_type_mapped = 5
        else:
            raise ValueError("Invalid do_type")

        # Byte 2: DO Type + Channel
        byte2 = ((do_type_mapped & 0x0F) << 4) | (channel & 0x0F)

        # -----------------------------
        # Time HEX
        # -----------------------------
        on_hr_hex = f"{one_on_hr:02X}"
        on_min_hex = f"{one_on_min:02X}"
        off_hr_hex = f"{one_off_hr:02X}"
        off_min_hex = f"{one_off_min:02X}"

        # -----------------------------
        # Days mask
        # -----------------------------
        days_mask = days_to_mask(message_data.days)
        days_hex = f"{days_mask:02X}"

        # -----------------------------
        # Final Payload (9 bytes)
        # -----------------------------
        hex_payload = (
            f"{rxUID_hex}"     # 2 bytes
            f"{byte2:02X}"     # 1 byte
            f"{byte3:02X}"     # 1 byte (ENABLE + SLOT)
            f"{on_hr_hex}"     # 1 byte
            f"{on_min_hex}"    # 1 byte
            f"{off_hr_hex}"    # 1 byte
            f"{off_min_hex}"   # 1 byte
            f"{days_hex}"      # 1 byte
        )

        pubdata = f"*LC,{hex_payload}#"

        # -----------------------------
        # Get Gateway
        # -----------------------------
        userdata = request.state.user_data
        condition = f"client_id={userdata['client_id']} AND device_id = {message_data.device_id}"
        data = select_one_data("md_device", "gateway_id", condition, order_by="device_id DESC")

        # -----------------------------
        # Publish MQTT
        # -----------------------------
        mqtt_client.publish(f"/ST/{data['gateway_id']}", pubdata, qos=1)

        # -----------------------------
        # Save DB
        # -----------------------------
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

def format_value(v):
    if v is None:
        return None
    elif isinstance(v, datetime):
        return v.strftime('%Y-%m-%d %H:%M:%S')
    elif isinstance(v, time):
        return v.strftime('%H:%M:%S')
    return v
    
async def insert_updatesheduling(user_data, message_data: MqttPublishDeviceSchedule):

    current_datetime = get_current_datetime()

    # ================= CONDITION =================
    condi = f"device='{message_data.device}' AND do_no={message_data.do_no} AND client_id={user_data['client_id']}"
    
    if message_data.slot is not None:
        condi += f" AND slot={message_data.slot}"

    find_device_schedule = select_last_data(
        "device_schedule", "schedule_id", condi, "created_at"
    )

    # ================= CLEAN DATA =================
    data_dict = message_data.dict()
    clean_data = remove_none_fields(data_dict)

    # ❌ Remove unwanted keys
    clean_data.pop("device_id", None)
    clean_data.pop("organization_id", None)
    clean_data.pop("schedule_id", None)

    # ✅ Ensure slot & status exist
    if "slot" not in clean_data:
        clean_data["slot"] = 0   # default slot

    if "status" not in clean_data:
        clean_data["status"] = 1  # default active

    # ✅ Format values
    clean_data = {k: format_value(v) for k, v in clean_data.items()}

    # ================= UPDATE =================
    if find_device_schedule is not None:
        columns = {
            **clean_data,
            "updated_at": current_datetime,
            "created_by": user_data['user_id']
        }

        columns = {k: format_value(v) for k, v in columns.items()}

        user_id = update_data("device_schedule", columns, condi)

    # ================= INSERT =================
    else:
        insert_dict = {
            "client_id": user_data['client_id'],
            "device": message_data.device,
            "do_no": message_data.do_no,
            "created_by": user_data['user_id'],
            "created_at": current_datetime,
            **clean_data
        }

        # ❌ Remove unwanted keys again (safety)
        insert_dict.pop("schedule_id", None)
        insert_dict.pop("organization_id", None)
        insert_dict.pop("device_id", None)

        insert_dict = {k: format_value(v) for k, v in insert_dict.items()}

        columns = ", ".join(insert_dict.keys())

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

    # ================= MQTT TRIGGER =================
    await send_readsettings(
        user_data['client_id'],
        message_data.device,
        message_data.do_no
    )

    return user_id



async def send_readsettings(client_id, device_id, dono):
    try:
        from Library.WsConnectionManagerManyDeviceTypes import WsConnectionManagerManyDeviceTypes
        manager = WsConnectionManagerManyDeviceTypes()
        
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
        twodata={"shedulingdata":lastdata}
        await sennd_ws_message("WFMS_SETTINGS",client_id,data['device_id'],device_id, json.dumps(twodata, cls=DecimalEncoder))
       
        return json.dumps(lastdata, cls=DecimalEncoder)
    except Exception as e:
        raise ValueError("Could not fetch data",e)

@mqtt_routes.post("/reset_sheduling", dependencies=[Depends(mw_user_client)])
async def reset_sheduling(request: Request,message_data: ResetMqttPublishDeviceSchedule):
    pubdata=f"*TORST,{message_data.device}#"
    
    # //*DOTIM,UID,D0-INDEX,DO_TYPE,ON-HR,ON-MIN,OFF-HR,OFF-MIN,ON1-HR,ON1-MIN,OFF1-HR,OFF1-MIN#
    # //*DOTIM, ,0,4,16,00,17,00,18,00,19,00#
    
    
    mqtt_client.publish(f"/WFMS/{message_data.device}", pubdata, qos=0)
    return pubdata
    


@mqtt_routes.post("/read_sheduling", dependencies=[Depends(mw_user_client)])
async def read_scheduling(request: Request, message_data: MqttReadSchedule):
    # try:
        user_data = request.state.user_data

        # ✅ Device → UID HEX
        device_int = int(message_data.device)   # "0050" → 50
        uid_hex = f"{device_int:04X}"           # → 0032

        channel = message_data.do_no & 0x0F     # safety
        slot = (message_data.slot or 1) & 0x03  # only 0–3 allowed

        # ✅ Command selection
        if message_data.request_type == 0:
            cmd = "RT"   # Read Timer

            # RT Read → UID + Channel (no slot needed usually)
            payload = f"{uid_hex}{channel:02X}"

        else:
            cmd = "RM"   # Read Mode

            # RM Read → UID + Channel
            payload = f"{uid_hex}{channel:02X}"
            
        


        pubdata = f"*{cmd},{payload}#"

        print("payload",pubdata)
        
        condition = f"client_id={user_data['client_id']} AND device_id = {message_data.device_id}"

        data = select_one_data(
            "md_device",
            "gateway_id",
            condition,
            order_by="device_id DESC"
        )

        if not data:
            raise HTTPException(status_code=404, detail="Device not found")

        # ✅ MQTT Publish
        mqtt_client.publish(f"/ST/{data['gateway_id']}", pubdata, qos=1)

        # ✅ Send response to frontend/socket
        await send_readsettings(user_data['client_id'], message_data.device, channel)

        return {
            "status": "success",
            "command": pubdata,
            "info": {
                "uid": uid_hex,
                "channel": channel,
                "slot": slot,
                "type": cmd
            }
        }

    # except Exception as e:
    #     raise HTTPException(status_code=500, detail=str(e))
    
    
@mqtt_routes.post("/read_last_data", dependencies=[Depends(mw_user_client)])
async def read_last_data(request: Request, message_data: MqttReadLastData):
    try:
        # 🔹 Convert device_id → HEX (4 digit uppercase)
        device_id_int = int(message_data.device_id)
        nid = format(device_id_int, '04X')  # Example: 10 → 000A

        # 🔹 Create payload
        cmd = "LRDT"
        pubdata = f"*{cmd},{nid}#"

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
        if message_data.slot is not None:
            condition += f" AND slot={message_data.slot}"
        select="schedule_id, client_id, device, do_type, datalog_sec, do_no, slot, status, one_on_time, one_off_time, two_on_time, two_off_time,days, created_by, DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s') AS created_at, DATE_FORMAT(updated_at, '%Y-%m-%d %H:%i:%s') AS updated_at"
        data = select_one_data("device_schedule",select, condition,order_by="schedule_id DESC")
        resdata = successResponse(data, message="Shedule successfully")
        return Response(content=json.dumps(resdata,default=convert_timedelta), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")
