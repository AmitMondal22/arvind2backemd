from fastapi import APIRouter, HTTPException,Response, Depends,Request
from controllers.user import UserController
from middleware.MyMiddleware import mw_client,mw_user,mw_user_client
from models.mqtt_model import MqttWfmsDO, MqttPublishDeviceSchedule,MqttPublishDeviceScheduleList, ResetMqttPublishDeviceSchedule, MqttReadSchedule
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





@mqtt_routes.post("/publish_digital_output", dependencies=[Depends(mw_user_client)])
async def publish_message(request: Request, message_data: MqttWfmsDO):
    # try:
        user_data=request.state.user_data 
        current_datetime = get_current_datetime()
        # condi=f"device_id = '{message_data.device_id}' AND device='{message_data.device}' AND client_id = '{user_data['client_id']}'"

        # find_device=select_last_data("td_water_data", "do_status", condi , "created_at")
        
       
        
        print("do_status>>>>>>>>>>>>>>???",message_data.do_no)
        if (message_data.do_no == 1 or message_data.do_no == 2 or message_data.do_no == 3 or message_data.do_no == 4 or message_data.do_no == 5 or message_data.do_no == 6 or message_data.do_no == 7 or message_data.do_no == 8):
            
            # stt = find_device['do_status']
            stt = "000000000"
            do_no = (message_data.do_no-1)  # Position to replace (0-based index)
            do_status = message_data.do_status  # New value to insert at the specified position
            
        
            # Convert the string to a list to allow modification
            stt_list = list(stt)
            # stt_list = [int(char) for char in stt_list]
            # stt_list = [str(int(char) + 1) for char in stt_list]
            # Replace the value at the specified position
            stt_list[do_no] = str(do_status)
            # Convert the list back to a string
            stt = ",".join(stt_list)
           
            
            
            
            
            
            columns2 = "client_id, device_id, device, di_status, do_status, status, created_at"
            value2 = f"{user_data['client_id']},{message_data.device_id},'{message_data.device}','{stt}','{stt}','1','{current_datetime}'"
            insert_data("td_dido", columns2, value2)
                

            condi=f"device='{message_data.device}' AND device_id ={message_data.device_id} AND client_id = {user_data['client_id']}"
            find_device_schedule=select_last_data("td_dido_settings", "di_status, do_status", condi , "created_at")
            # find_device_schedule = {'di_status': '1,0,0,0,0,0,0,0,0', 'do_status': '1,0,0,0,0,0,0,0,0'}
            if find_device_schedule is not None:
                print(">>>>>>>>>>>>>.",find_device_schedule['di_status'])
                olddata = find_device_schedule['di_status']
                a_list = olddata.split(",")
                a_list[do_no] = str(do_status)
                a = ",".join(a_list)
                print("ZZZZz",a)
                
                columns = {"di_status": a, "do_status": a, "updated_at": get_current_datetime()}
                data = update_data("td_dido_settings", columns, condi)
            else:
                
                columns = "client_id, device_id, device, di_status, do_status, created_at"
                value = f"{user_data['client_id']},{message_data.device_id},'{message_data.device}','{stt}','{stt}','{current_datetime}'"
                project_id = insert_data("td_dido_settings", columns, value)
            
            
                
            
                
                # formatted_number = ",".join(str(message_data.digital_output))
                # # srdata=f"*OPADO, ,{formatted_number}#"
                # srdata=f"*OPADO, ,1,2,0,0#"
                # *OPADO, ,2,2,2,2,2,2,1,1#
            srdata=f"*OPADO, {message_data.device},{stt}#"
            mqtt_client.publish(f"/WFMS/{message_data.device}", srdata, qos=0)
            
            stt_list[do_no] = str(1)
            # Convert the list back to a string
            stt2 = ",".join(stt_list)
            print("do_status",do_no,"___",do_status,"___", stt2)
            time.sleep(2)
            srdata=f"*OPADO, {message_data.device},{stt2}#"
            
            mqtt_client.publish(f"/WFMS/{message_data.device}", srdata, qos=0)
        else:
            stt = "1,1,1,1,1,1,1,1,1"
            stt2 = "1,1,1,1,1,1,1,1,2"
            columns2 = "client_id, device_id, device, di_status, do_status, status, created_at"
            value2 = f"{user_data['client_id']},{message_data.device_id},'{message_data.device}','{stt}','{stt}','1','{current_datetime}'"
            insert_data("td_dido", columns2, value2)
                

            condi=f"device='{message_data.device}' AND device_id ={message_data.device_id} AND client_id = {user_data['client_id']}"
            find_device_schedule=select_last_data("td_dido_settings", "di_status, do_status", condi , "created_at")
            # find_device_schedule = {'di_status': '1,0,0,0,0,0,0,0,0', 'do_status': '1,0,0,0,0,0,0,0,0'}
            if find_device_schedule is not None:
                columns = {"di_status": stt, "do_status": stt, "updated_at": get_current_datetime()}
                data = update_data("td_dido_settings", columns, condi)
            else:
                
                columns = "client_id, device_id, device, di_status, do_status, created_at"
                value = f"{user_data['client_id']},{message_data.device_id},'{message_data.device}','{stt}','{stt}','{current_datetime}'"
                project_id = insert_data("td_dido_settings", columns, value)
                
            
            srdata=f"*OPADO, {message_data.device},{stt}#"
            srdata2=f"*OPADO, {message_data.device},{stt2}#"
            mqtt_client.publish(f"/WFMS/{message_data.device}", srdata2, qos=0)
            time.sleep(2)
            mqtt_client.publish(f"/WFMS/{message_data.device}", srdata, qos=0)
        resdata = successResponse(user_data, message="Message published successfully")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    # except ValueError as ve:
    #     # If there's a ValueError, return a 400 Bad Request with the error message
    #     raise HTTPException(status_code=400, detail=str(ve))
    # except Exception as e:
    #     # For any other unexpected error, return a 500 Internal Server Error
    #     raise HTTPException(status_code=500, detail="Internal server error")
    

@mqtt_routes.post("/publish_schedule", dependencies=[Depends(mw_user_client)])
async def publish_message(request: Request, message_data: MqttPublishDeviceSchedule):
    try:
        user_data=request.state.user_data
        print("???????????????/",user_data)
        one_on_time=message_data.one_on_time
        one_off_time=message_data.one_off_time
        two_on_time=message_data.two_on_time
        two_off_time=message_data.two_off_time
        one_on_hr, one_on_min, _ = one_on_time.split(":")
        one_off_hr, one_off_min, _ = one_off_time.split(":")
        two_on_hr, two_on_min, _ = two_on_time.split(":")
        two_off_hr, two_off_min, _ = two_off_time.split(":")
        
        user_id = await insert_updatesheduling(user_data,message_data)
        
        dotype = 4 if message_data.do_type == 0 else 5
        pubdata=f"*CONFIG,{message_data.device},{message_data.do_no-1},{dotype},{one_on_hr},{one_on_min},{one_off_hr},{one_off_min},{two_on_hr},{two_on_min},{two_off_hr},{two_off_min},{message_data.datalog_sec*60}#"
        
        # srdata=f"*OPADO, ,1,2,0,0#"
        # //*DOTIM,UID,D0-INDEX,DO_TYPE,ON-HR,ON-MIN,OFF-HR,OFF-MIN,ON1-HR,ON1-MIN,OFF1-HR,OFF1-MIN#
        # //*DOTIM, ,0,4,16,00,17,00,18,00,19,00#
        
        
        mqtt_client.publish(f"/WFMS/{message_data.device}", pubdata, qos=0)
        
        resdata = successResponse(user_id, message="Message published successfully")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error")
    
# async def publish_settings(message_data: MqttPublishDeviceSchedule):

    
async def insert_updatesheduling(user_data,message_data: MqttPublishDeviceSchedule):

    print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>LLLLLLLLLLLL")
    current_datetime = get_current_datetime()
    condi=f"device='{message_data.device}' AND do_no ={message_data.do_no} AND client_id = {user_data['client_id']}"
    find_device_schedule=select_last_data("device_schedule", "schedule_id", condi , "created_at")
    print(">>>>>>>>>>>>>>>>>",find_device_schedule)
    if find_device_schedule is not None:
        # columns={"organization_name":organization.organization_name,"created_by":organization.created_by,"updated_at":get_current_datetime()}
        columns={"do_type":message_data.do_type,"datalog_sec":message_data.datalog_sec, "one_on_time": message_data.one_on_time, "one_off_time":message_data.one_off_time, "two_on_time":message_data.two_on_time, "two_off_time":message_data.two_off_time, "created_by":user_data['user_id'], "updated_at":current_datetime}
        user_id = update_data("device_schedule", columns, condi)
    else:
        columns = "client_id, device, do_type, datalog_sec, do_no, one_on_time, one_off_time, two_on_time, two_off_time, created_by, created_at"
        value = f"{user_data['client_id']}, '{message_data.device}', {message_data.do_type}, {message_data.datalog_sec}, {message_data.do_no}, '{message_data.one_on_time}', '{message_data.one_off_time}', '{message_data.two_on_time}', '{message_data.two_off_time}',{user_data['user_id']}, '{current_datetime}'"
        user_id = insert_data("device_schedule", columns, value)
        
    print("KKKKKKKKKKKK",user_id)
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
async def reset_sheduling(request: Request,message_data: MqttReadSchedule):
    pubdata=f"*RDSETTING,{message_data.do_no-1}#"
    
    # srdata=f"*OPADO, ,1,2,0,0#"
    # //*DOTIM,UID,D0-INDEX,DO_TYPE,ON-HR,ON-MIN,OFF-HR,OFF-MIN,ON1-HR,ON1-MIN,OFF1-HR,OFF1-MIN#
    # //*DOTIM, ,0,4,16,00,17,00,18,00,19,00#
    
    
    mqtt_client.publish(f"/WFMS/{message_data.device}", pubdata, qos=0)
    return pubdata
    
    
def convert_timedelta(obj):
    if isinstance(obj, timedelta):
        # Convert timedelta to total seconds or a string representation
        return str(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

@mqtt_routes.post("/publish_schedule_data", dependencies=[Depends(mw_user_client)])
async def publish_schedule_data(request: Request, message_data: MqttPublishDeviceScheduleList):
    # try:
        user_data=request.state.user_data
        condition=f"device='{message_data.device}' AND do_no ={message_data.do_no} AND client_id = {user_data['client_id']}"
        select="schedule_id, client_id, device, do_type, datalog_sec, do_no, one_on_time, one_off_time, two_on_time, two_off_time, created_by, DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s') AS created_at, DATE_FORMAT(updated_at, '%Y-%m-%d %H:%i:%s') AS updated_at"
        data = select_one_data("device_schedule",select, condition,order_by="schedule_id DESC")
        print(">>>>>>>>>>>>>>>>>",data)
        resdata = successResponse(data, message="Shedule successfully")
        print('????????????????????',json.dumps(resdata,default=convert_timedelta))
        return Response(content=json.dumps(resdata,default=convert_timedelta), media_type="application/json", status_code=200)
    # except ValueError as ve:
    #     # If there's a ValueError, return a 400 Bad Request with the error message
    #     raise HTTPException(status_code=400, detail=str(ve))
    # except Exception as e:
    #     # For any other unexpected error, return a 500 Internal Server Error
    #     raise HTTPException(status_code=500, detail="Internal server error")
