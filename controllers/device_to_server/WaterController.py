from Library.AlertLibrary import send_alert
from db_model.MASTER_MODEL import insert_data,custom_select_sql_query,select_one_data,select_last_data, update_data
from utils.date_time_format import get_current_datetime,get_current_date,get_current_time
from fastapi import BackgroundTasks
from Library.DecimalEncoder import DecimalEncoder
# from Library import AlertLibrary
import json
from models import device_data_model
# from utils.week_date import weekdays_date
from datetime import datetime




async def get_weather_data(data:device_data_model.WaterDeviceData,client_id,device,gateway_id=None):
    try:
        print("Received data from device:", data)
        device_data=select_one_data("md_device","device_id",f"client_id={client_id} AND device='{device}'")
        if device_data is None:
            raise ValueError("device not found")
        
        device_id=device_data["device_id"]
        current_datetime = get_current_datetime()
      
        date_obj = datetime.strptime(data.DT, "%Y.%m.%d")
        formatted_date = date_obj.strftime("%Y-%m-%d")
        
        time_obj = datetime.strptime(data.TIME, "%H:%M:%S")
        formatted_time = time_obj.strftime("%H:%M:%S")
        
        columns = "client_id,branch_number,gateway_id, device_id, device,tw, flow_rate1, total_flow1,pressure, runhr, di_status, do_status,bat_v, date, time, created_at"
        
        value = f"{client_id},'{data.BRANCH_NUMBER}', '{gateway_id}', {device_id}, '{device}',{data.TW}, {data.A1},{data.TOT1},{data.A2},{0.0},'{data.DO}' ,'{data.DO}',{data.BAT_V}, '{formatted_date}', '{formatted_time}', '{current_datetime}'"
        weather_data_id = insert_data("td_water_data", columns, value)   
        if weather_data_id is None:
            raise ValueError("Weather data was not inserted")
        else:
            await send_last_weather_data(client_id, device_id,device)
            user_data = {"weather_data_id":weather_data_id, "device_id": device_id, "device": device}
        return user_data
    except Exception as e:
        raise ValueError("Could not fetch data",e)
    
    
async def update_device(device_id, imei=None, gateway_id=None, group_id=None):
    try:
        condition = f"device = '{device_id}'"

        # ✅ Fetch existing device
        device_data = select_one_data("md_device", "*", condition)
        if not device_data:
            raise ValueError("Device not found")
        columns = {
            "device_status": "ONLINE"
        }

        # ✅ IMEI always allowed to update
        if imei is not None:
            columns["imei_no"] = imei

        # ✅ Update gateway_id ONLY if DB value is NULL
        # if gateway_id is not None and device_data.get("gateway_id") is None:
        #     columns["gateway_id"] = gateway_id

        # ✅ Update branch_number ONLY if DB value is NULL
        if group_id is not None and device_data.get("branch_number") is None:
            columns["branch_number"] = group_id

        # ✅ Update only if something to update
        if len(columns) > 1:  # means something extra besides device_status
            update_data("md_device", columns, condition)

        # ✅ Call only if both updated (and previously NULL)
        # if (
        #     gateway_id is not None
        #     and group_id is not None
        #     and device_data.get("gateway_id") is None
        #     and device_data.get("branch_number") is None
        # ):
        #     await new_getway(gateway_id, group_id)

        return True

    except Exception as e:
        raise ValueError(f"Could not update device: {str(e)}")

async def alert_generate(client_id, device, data):
    try:
        send_alert(client_id, device, data)
        return True
    except Exception as e:
        raise ValueError("Could not fetch data",e)
    
    
async def new_getway(gateway_id, branch = None):
    try:
        print(f"Processing new gateway: {gateway_id} with branch: {branch}")
        md_gateway = select_one_data(
            "md_gateway",
            "*",
            f"gateway_id='{gateway_id}'"
        )

        current_datetime = get_current_datetime()

        # ✅ Gateway check
        if md_gateway and md_gateway.get("gateway_id"):
            print("Gateway already exists → skip insert")
        else:
            columns = "gateway_id, start_id, max_id, retry, created_at"
            value = f"'{gateway_id}', 0, 0, 2, '{current_datetime}'"
            insert_data("md_gateway", columns, value)

        # ✅ Branch logic ONLY if branch is not None / empty
        if branch:   # <-- this handles None, "", False
            manage_branch = select_one_data(
                "manage_branch",
                "*",
                f"branch_number='{branch}'"
            )

            if manage_branch and manage_branch.get("branch_number"):
                print("Branch already exists → skip insert")
            else:
                columns = "branch_name, branch_number, created_at"
                value = f"'{branch}', '{branch}', '{current_datetime}'"
                insert_data("manage_branch", columns, value)
        else:
            print("Branch is None/empty → skipping branch insert")

    except Exception as e:
        raise ValueError(f"Could not process gateway: {e}")
    
  
async def send_last_weather_data(client_id, device_id, device):
    try:
        # Lazy import inside the function
        from Library.WsConnectionManagerManyDeviceTypes import WsConnectionManagerManyDeviceTypes
        manager = WsConnectionManagerManyDeviceTypes()
        # background_tasks = BackgroundTasks()
        
        from routes.ws_routes import sennd_ws_message 
                
        custom_sql=f"""SELECT 
                            td.water_data_id, td.client_id, td.device_id, td.device, td.tw, 
                            td.flow_rate1, td.total_flow1, td.pressure, td.runhr, 
                            COALESCE(td.di_status, '000000000') AS di_status,
                            COALESCE(td.do_status, '000000000') AS do_status,
                            td.bat_v, td.date, td.time, 
                            td.created_at, td.updated_at
                        FROM 
                            td_water_data td
                        LEFT JOIN 
                            td_dido_settings dost 
                            ON td.client_id = dost.client_id 
                            AND td.device_id = dost.device_id 
                            AND td.device = dost.device
                        WHERE 
                            td.device_id = {device_id}
                            AND td.device = '{device}'
                            AND td.client_id = {client_id}
                        ORDER BY 
                            td.water_data_id DESC 
                        LIMIT 1;"""
        lastdata=custom_select_sql_query(custom_sql,None)


        # background_tasks.add_task(AlertLibrary.send_alert, client_id, device_id, device, json.dumps(lastdata, cls=DecimalEncoder))
        
        # await AlertLibrary.send_alert(client_id, device_id, device, json.dumps(lastdata, cls=DecimalEncoder))
        
        # await manager.send_personal_message("EMS",client_id, device_id, device, json.dumps(lastdata, cls=DecimalEncoder))
        twodata={"lastdata":lastdata}
        await sennd_ws_message("WFMS",client_id, device_id, device, json.dumps(twodata, cls=DecimalEncoder))
        # if organization_data is not None:
            # await send_last_client_data(organization_data['organization_id'])
        print("twodata",twodata)
       
        return json.dumps(lastdata, cls=DecimalEncoder)
    except Exception as e:
        raise ValueError("Could not fetch data",e)


async def send_last_client_data(organization_id, project_id):
    # try:
        # Lazy import inside the function
        from Library.WsConnectionManagerManyDeviceTypes import WsConnectionManagerManyDeviceTypes
        manager = WsConnectionManagerManyDeviceTypes()
        # background_tasks = BackgroundTasks()
        
        from routes.ws_routes import  sennd_ws_client_message    

        custom_sql2=f"""WITH LatestWaterData AS (
                            SELECT
                                w.water_data_id,
                                w.client_id,
                                w.device_id,
                                w.device,
                                w.tw,
                                w.flow_rate1,
                                w.total_flow1,
                                w.pressure,
                                w.runhr,
                                w.di_status,
                                w.do_status,
                                w.bat_v,
                                w.date,
                                w.time,
                                w.created_at,
                                w.updated_at,
                                ROW_NUMBER() OVER (PARTITION BY w.device_id ORDER BY w.created_at DESC) AS row_num
                            FROM
                                td_water_data w
                        )

                        SELECT
                            lwd.water_data_id,
                            lwd.client_id,
                            lwd.device_id,
                            lwd.device,
                            lwd.tw,
                            lwd.flow_rate1,
                            lwd.total_flow1,
                            lwd.pressure,
                            lwd.runhr,
                            lwd.di_status,
                            lwd.do_status,
                            lwd.date,
                            lwd.time,
                            lwd.created_at,
                            lwd.updated_at,
                            m.manage_project_device_id,
                            m.organization_id,
                            m.create_by,
                            md.device_name,
                            md.model
                        FROM
                            LatestWaterData lwd
                        INNER JOIN
                            md_manage_project_device m
                            ON lwd.device_id = m.device_id
                            AND lwd.device = m.device
                        INNER JOIN
                            md_device md
                            ON lwd.device_id = md.device_id
                            AND lwd.client_id = md.client_id
                        WHERE
                            m.organization_id =  {organization_id}
                            AND m.project_id = {project_id}
                            AND lwd.row_num = 1
                        ORDER BY
                            lwd.device_id;
                        """
        lastalldevicedata=custom_select_sql_query(custom_sql2,1)
        
        print("ZZZZZZZZZZZZZZZZZZZZZZ",lastalldevicedata)

        ladd={"last_all_device_data":lastalldevicedata}
        await sennd_ws_client_message("WFMS",project_id, json.dumps(ladd, cls=DecimalEncoder))
       
        return json.dumps(ladd, cls=DecimalEncoder)
    # except Exception as e:
    #     raise ValueError("Could not fetch data",e)