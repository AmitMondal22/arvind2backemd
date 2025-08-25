from db_model.MASTER_MODEL import insert_data,custom_select_sql_query,select_one_data,select_last_data, update_data
from utils.date_time_format import get_current_datetime,get_current_date,get_current_time
from fastapi import BackgroundTasks
from Library.DecimalEncoder import DecimalEncoder
# from Library import AlertLibrary
import json
from models import device_data_model
# from utils.week_date import weekdays_date
from datetime import datetime




async def get_weather_data(data:device_data_model.WaterDeviceData,client_id,device):
    try:
        device_data=select_one_data("md_device","device_id",f"client_id={client_id} AND device='{device}'")
        if device_data is None:
            raise ValueError("device not found")
        
        device_id=device_data["device_id"]
        current_datetime = get_current_datetime()
      
        date_obj = datetime.strptime(data.DT, "%Y.%m.%d")
        formatted_date = date_obj.strftime("%Y-%m-%d")
        
        time_obj = datetime.strptime(data.TIME, "%H:%M:%S")
        formatted_time = time_obj.strftime("%H:%M:%S")
        
        columns = "client_id, device_id, device,tw, flow_rate1, total_flow1,pressure, runhr, di_status, do_status,bat_v, date, time, created_at"
        
        value = f"{client_id}, {device_id}, '{device}',{data.TW}, {data.A1},{data.TOT1},{data.A2},{0.0},'{data.DO}' ,'{data.DO}',{data.BAT_V}, '{formatted_date}', '{formatted_time}', '{current_datetime}'"
        
        print("value",value)
        weather_data_id = insert_data("td_water_data", columns, value)
        
        
        
        
        if weather_data_id is None:
            raise ValueError("Weather data was not inserted")
        else:
            await send_last_weather_data(client_id, device_id,device)
            user_data = {"weather_data_id":weather_data_id, "device_id": device_id, "device": device}
        return user_data
    except Exception as e:
        raise ValueError("Could not fetch data",e)
    
    
async def update_device(device_id,imei,gateway_id):
    try:
        print(device_id,imei,gateway_id)
        columns={"gateway_id":gateway_id,"gateway_id":imei}
        condition = f"device = {device_id}"
        update_data("md_device",columns,condition)
        return True
    except Exception as e:
        raise ValueError("Could not fetch data",e)
    
  
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
        
        organization_data = select_last_data("md_manage_user_device", "organization_id",f"device_id = {device_id}","created_at")
        
       

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