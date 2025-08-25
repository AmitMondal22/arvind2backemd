from db_model.MASTER_MODEL import select_data,update_data,select_one_data,batch_insert_data,insert_data
from utils.date_time_format import get_current_datetime, get_current_date_time_utc
from utils.last12month import last_12_month
from routes.mqtt_routes import subscribe_topics



async def all_list_device(client_id):
    try:
        select="device_id, device, device_name, model"
        # select="device_id, device, do_channel, model, lat, lon, imei_no, last_maintenance, DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s') AS created_at, DATE_FORMAT(updated_at, '%Y-%m-%d %H:%i:%s') AS updated_at"
        condition=f"client_id={client_id}"
        data = select_data("md_device", select, condition,order_by="device_id ASC")
        return data
    except Exception as e:
        raise e
    
    
async def list_device(client_id, organization_id):
    try:
        select="d.device_id, d.device,d.device_name, d.do_channel, d.model, d.lat, d.lon, d.imei_no, d.last_maintenance, DATE_FORMAT(d.created_at, '%Y-%m-%d %H:%i:%s') AS created_at, DATE_FORMAT(d.updated_at, '%Y-%m-%d %H:%i:%s') AS updated_at"
        
        condition = f"d.device_id = mud.device_id AND d.client_id = mud.client_id AND mud.client_id = {client_id} AND mud.organization_id = {organization_id}"
        find_devices=select_data("md_device AS d, md_manage_user_device AS mud", select, condition,order_by="d.device_id ASC")
        print("find_devices>>>>>>>>>>>>>>>>>",find_devices)
        return find_devices
    except Exception as e:
        raise e
    
    

async def user_device_list(client_id, user_id, organization_id):
    try:
        select="d.device_id, d.device,d.device_name, d.do_channel, d.model, d.lat, d.lon, d.imei_no, d.last_maintenance, DATE_FORMAT(d.created_at, '%Y-%m-%d %H:%i:%s') AS created_at, DATE_FORMAT(d.updated_at, '%Y-%m-%d %H:%i:%s') AS updated_at"
        
        condition = f"d.device_id = mud.device_id AND d.client_id = mud.client_id AND mud.client_id = {client_id} AND mud.user_id = {user_id} AND mud.organization_id = {organization_id}"
        find_devices=select_data("md_device AS d, md_manage_user_device AS mud", select, condition,order_by="d.device ASC")
        print("find_devices>>>>>>>>>>>>>>>>>",find_devices)
        return find_devices
    except Exception as e:
        raise ValueError("Could not fetch data")
    
    
async def project_user_device_list(client_id, user_id, organization_id,project_id):
    try:
        select = "d.device_id, d.device, d.device_name, d.model, wd.created_at"
    
        condition = (
            f"d.device_id = mud.device_id "
            f"AND e.device_id = d.device_id "
            f"AND d.client_id = mud.client_id "
            f"AND mud.client_id = {client_id} "
            f"AND mud.user_id = {user_id} "
            f"AND mud.organization_id = {organization_id} "
            f"AND e.project_id = {project_id}"
        )
        
        # Define the tables with a subquery for the latest td_water_data
        tables = (
            "md_device AS d "
            "INNER JOIN md_manage_user_device AS mud ON d.device_id = mud.device_id AND d.client_id = mud.client_id "
            "INNER JOIN md_manage_project_device AS e ON e.device_id = d.device_id "
            "LEFT JOIN ("
            "    SELECT device_id, created_at "
            "    FROM td_water_data "
            "    WHERE (device_id, created_at) IN ("
            "        SELECT device_id, MAX(created_at) "
            "        FROM td_water_data "
            "        GROUP BY device_id"
            "    )"
            ") AS wd ON d.device_id = wd.device_id"
        )
        
        find_devices = select_data(tables, select, condition, order_by="d.device ASC")
        print("find_devices>>>>>>>>>>>>>>>>>", find_devices)
        print("find_devices>>>>>>>>>>>>>>>>>",find_devices)
        return find_devices
    except Exception as e:
        raise ValueError("Could not fetch data")

async def project_list_device(client_id, organization_id,project_id):
    try:
        select = "d.device_id, d.device, d.device_name, d.model, wd.created_at"
    
        # Define the tables with the subquery for the latest td_water_data
        tables = """md_device AS d
                    INNER JOIN md_manage_user_device AS mud ON d.device_id = mud.device_id AND d.client_id = mud.client_id
                    INNER JOIN md_manage_project_device AS e ON e.device_id = d.device_id
                    LEFT JOIN (
                        SELECT device_id, created_at
                        FROM td_water_data
                        WHERE (device_id, created_at) IN (
                            SELECT device_id, MAX(created_at)
                            FROM td_water_data
                            GROUP BY device_id
                        )
                    ) AS wd ON d.device_id = wd.device_id"""
        
        # Define the condition
        condition = f"mud.client_id = {client_id} AND mud.organization_id = {organization_id} AND e.project_id = {project_id}"
        
        # Execute the query using the select_data function
        find_devices = select_data(tables, select, condition, order_by="d.device ASC")
        print("find_devices>>>>>>>>>>>>>>>>>",find_devices)
        return find_devices
    except Exception as e:
        raise e
    

async def device_info(params,userdata):
    try:
        condition = f"client_id={userdata['client_id']} AND device_id = {params.device_id}"
        select="device_id, client_id, device, device_name, do_channel, model, lat, lon, imei_no, last_maintenance, DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s') AS created_at, DATE_FORMAT(updated_at, '%Y-%m-%d %H:%i:%s') AS updated_at"
        data = select_one_data("md_device",select, condition,order_by="device_id DESC")
        
        # select2="count(a.alert_id) alert, a.alert_type, a.unit_id,u.unit,u.unit_name"
        # condition2 = f"a.unit_id=u.unit_id AND a.client_id={userdata['client_id']} AND a.device_id = {params.device_id} GROUP BY a.alert_type, a.unit_id, u.unit, u.unit_name"
        # table2="td_alert AS a, md_unit AS u"
        # alert=select_data(table2,select2, condition2)
        
        return {"data":data}
        # return {"data":data, "data2":alert}
    except Exception as e:
        raise e


async def add_device(params):
    try:
        
        
        column="client_id, device, device_name, do_channel, model, lat, lon, imei_no, last_maintenance, created_at"
        
        rows_data = []
        for params_data in params:
            row_data = {
                "client_id": params_data.client_id,
                "device": params_data.device,
                "device_name": params_data.device_name,
                "do_channel": params_data.do_channel,
                "model": params_data.model,
                "lat": params_data.lat,
                "lon": params_data.lon,
                "imei_no": params_data.imei_no,
                "last_maintenance": params_data.last_maintenance,
                "created_at": get_current_datetime()  # Assuming get_current_datetime() returns the current datetime
            }
            rows_data.append(row_data)        
        batch_dataid=batch_insert_data("md_device", column, rows_data)
        print("batch_dataid---------------------", batch_dataid)
        
        
        await subscribe_topics()
        return batch_dataid
    except Exception as e:
        raise e
    

async def edit_device(params):
    try:
        condition = f"device_id = {params.device_id} AND client_id = {params.client_id}"
        columns={"device":params.device, "device_name":params.device_name, "do_channel":params.do_channel, "model":params.model, "lat":params.lat, "lon":params.lon, "imei_no":params.imei_no, "updated_at":get_current_datetime()}
        data = update_data("md_device", columns, condition)
        print(data)
        return data
    except Exception as e:
        raise e
    


async def manage_list_device(params):
    try:
        condition = f"a.client_id = {params.client_id}"
        
        # select="a.device_id,a.client_id, a.device, a.device_name,a.do_channel, a.model,a.lat, a.lon,  a.imei_no, a.last_maintenance,DATE_FORMAT(a.created_at, '%Y-%m-%d') AS device_created_at, DATE_FORMAT(a.updated_at, '%Y-%m-%d %H:%i:%s') AS device_updated_at, b.water_data_id, b.device_id AS b_device_id, b.flow_rate1,b.total_flow1,b.pressure,DATE_FORMAT(b.date, '%Y-%m-%d') AS date, TIME_FORMAT(b.time, '%H:%i:%s') AS time, DATE_FORMAT(b.created_at, '%Y-%m-%d %H:%i:%s') AS weather_data_created_at, DATE_FORMAT(b.updated_at, '%Y-%m-%d %H:%i:%s') AS weather_data_updated_at"
        
        # table="""md_device a LEFT JOIN (SELECT t1.*
        #             FROM td_water_data t1
        #             INNER JOIN (
        #                 SELECT device_id, MAX(CONCAT(date, ' ', time)) AS max_datetime
        #                 FROM td_weather_data
        #                 GROUP BY device_id, client_id
        #             ) t2 ON t1.device_id = t2.device_id AND CONCAT(t1.date, ' ', t1.time) = t2.max_datetime) b ON a.device_id = b.device_id """
        
        # order_by="a.device_id ASC"
        select="""a.device_id,
                    a.client_id,
                    a.device,
                    a.device_name,
                    a.do_channel,
                    a.model,
                    a.lat,
                    a.lon,
                    a.imei_no,
                    a.last_maintenance,
                    DATE_FORMAT(a.created_at, '%Y-%m-%d') AS device_created_at,
                    DATE_FORMAT(a.updated_at, '%Y-%m-%d %H:%i:%s') AS device_updated_at,
                    b.water_data_id,
                    b.device_id AS b_device_id,
                    b.flow_rate1,
                    b.total_flow1,
                    b.pressure,
                    DATE_FORMAT(b.date, '%Y-%m-%d') AS DATE,
                    TIME_FORMAT(b.time, '%H:%i:%s') AS TIME,
                    DATE_FORMAT(b.created_at, '%Y-%m-%d %H:%i:%s') AS weather_data_created_at,
                    DATE_FORMAT(b.updated_at, '%Y-%m-%d %H:%i:%s') AS weather_data_updated_at"""
        
        table="""md_device a
                    LEFT JOIN (
                        SELECT
                            t1.*
                        FROM
                            td_water_data t1
                        INNER JOIN (
                            SELECT device_id,
                                MAX(CONCAT(date, ' ', time)) AS max_datetime
                            FROM
                                td_water_data -- Fixed table name here
                            GROUP BY
                                device_id
                        ) t2
                        ON
                            t1.device_id = t2.device_id 
                            AND CONCAT(t1.date, ' ', t1.time) = t2.max_datetime
                    ) b
                    ON
                        a.device_id = b.device_id """
        
        order_by="a.device ASC"
        
        data = select_data(table, select,condition,order_by)
        print("????????????????>>>>>>>>>>>>>>>>",data)
        return data
    except Exception as e:
        raise e
    



    
# =========================================================

async def water_flow_data(params,user_data):
    try:
        select="water_data_id, client_id, device_id, device, tw, flow_rate1, total_flow1,pressure, runhr, di_status, do_status,bat_v, date, time, created_at, updated_at"
        condition=f"client_id = {user_data['client_id']} AND device_id = {params.device_id} AND date BETWEEN '{params.start_date}' AND '{params.end_date}'"
        data = select_data("td_water_data",select, condition,order_by="date DESC, time DESC")
        # print("weather_data>>>>>>>>>>>>>>>>>",data)
        return data
    except Exception as e:
        raise e
    
    
async def last_chart_data(params,user_data):
    try:
        select="flow_rate1,pressure, date, time, created_at, updated_at"
        condition=f"client_id = {user_data['client_id']} AND device_id = {params.device_id}"
        data = select_data("td_water_data",select, condition,order_by="date DESC, time DESC LIMIT 3000")
        # print("weather_data>>>>>>>>>>>>>>>>>",data)
        return data
    except Exception as e:
        raise e
    

async def monthly_report(params,user_data):
    try:
        condition = f"ed.client_id = {user_data['client_id']} AND ed.device_id = {params.device_id} AND ed.date BETWEEN '{params.start_date}' AND '{params.end_date}'"
        select="""ed.weather_data_id,
                    ed.device_id,
                    ed.device,
                    ed.tw,
                    ed.temperature,
                    ed.rainfall,
                    ed.rainfall_cumulative,
                    ed.atm_pressure,
                    ed.solar_radiation,
                    ed.humidity,
                    ed.wind_speed,
                    ed.wind_direction,
                    DATE_FORMAT(ed.date, '%Y-%m-%d') AS date,
                    TIME_FORMAT(ed.time, '%H:%i:%s') AS time,
                    daily_stats.avg_temperature,
                    daily_stats.avg_rainfall,
                    daily_stats.avg_rainfall_cumulative,
                    daily_stats.avg_atm_pressure,
                    daily_stats.avg_solar_radiation,
                    daily_stats.avg_humidity,
                    daily_stats.min_temperature,
                    daily_stats.min_rainfall,
                    daily_stats.min_rainfall_cumulative,
                    daily_stats.min_atm_pressure,
                    daily_stats.min_solar_radiation,
                    daily_stats.min_humidity,
                    daily_stats.max_temperature,
                    daily_stats.max_rainfall,
                    daily_stats.max_rainfall_cumulative,
                    daily_stats.max_atm_pressure,
                    daily_stats.max_solar_radiation,
                    daily_stats.max_humidity"""
        table=f"""td_weather_data AS ed
                        INNER JOIN (
                            SELECT
                                date,
                                MAX(time) AS max_time
                            FROM
                                td_weather_data
                            WHERE
                                client_id = {user_data['client_id']} AND device_id = {params.device_id}
                                AND date BETWEEN DATE_SUB(CURDATE(), INTERVAL 30 DAY) AND CURDATE()
                            GROUP BY
                                date
                        ) AS sub_ed ON ed.date = sub_ed.date AND ed.time = sub_ed.max_time
                 INNER JOIN (
                        SELECT
                            date,
                            AVG(temperature) AS avg_temperature,
                            AVG(rainfall) AS avg_rainfall,
                            AVG(rainfall_cumulative) AS avg_rainfall_cumulative,
                            AVG(atm_pressure) AS avg_atm_pressure,
                            AVG(solar_radiation) AS avg_solar_radiation,
                            AVG(humidity) AS avg_humidity,
                            MIN(temperature) AS min_temperature,
                            MIN(rainfall) AS min_rainfall,
                            MIN(rainfall_cumulative) AS min_rainfall_cumulative,
                            MIN(atm_pressure) AS min_atm_pressure,
                            MIN(solar_radiation) AS min_solar_radiation,
                            MIN(humidity) AS min_humidity,
                            MAX(temperature) AS max_temperature,
                            MAX(rainfall) AS max_rainfall,
                            MAX(rainfall_cumulative) AS max_rainfall_cumulative,
                            MAX(atm_pressure) AS max_atm_pressure,
                            MAX(solar_radiation) AS max_solar_radiation,
                            MAX(humidity) AS max_humidity
                        FROM
                            td_weather_data
                        WHERE
                            client_id = {user_data['client_id']} AND device_id = {params.device_id}
                            AND date BETWEEN DATE_SUB(CURDATE(), INTERVAL 30 DAY) AND CURDATE()
                        GROUP BY
                            date
                    ) AS daily_stats ON ed.date = daily_stats.date
                        """
        data = select_data(table,select, condition,order_by="date DESC, time DESC")
        return data
    except Exception as e:
        raise e


async def yearly_report(params,user_data):
    try:
        condition = f"ed.client_id = {user_data['client_id']} AND ed.device_id = {params.device_id}  AND ed.date BETWEEN '{params.start_date}' AND '{params.end_date}'"
        select="""ed.weather_data_id,
                    ed.device_id,
                    ed.device,
                    ed.tw,
                    ed.temperature,
                    ed.rainfall,
                    ed.rainfall_cumulative,
                    ed.atm_pressure,
                    ed.solar_radiation,
                    ed.humidity,
                    ed.wind_speed,
                    ed.wind_direction,
                    DATE_FORMAT(ed.date, '%Y-%m-%d') AS date,
                    TIME_FORMAT(ed.time, '%H:%i:%s') AS time,
                    -- Monthly averages
                    monthly_stats.avg_temperature,
                    monthly_stats.avg_rainfall,
                    monthly_stats.avg_rainfall_cumulative,
                    monthly_stats.avg_atm_pressure,
                    monthly_stats.avg_solar_radiation,
                    monthly_stats.avg_humidity,
                    -- Monthly minimums
                    monthly_stats.min_temperature,
                    monthly_stats.min_rainfall,
                    monthly_stats.min_rainfall_cumulative,
                    monthly_stats.min_atm_pressure,
                    monthly_stats.min_solar_radiation,
                    monthly_stats.min_humidity,
                    -- Monthly maximums
                    monthly_stats.max_temperature,
                    monthly_stats.max_rainfall,
                    monthly_stats.max_rainfall_cumulative,
                    monthly_stats.max_atm_pressure,
                    monthly_stats.max_solar_radiation,
                    monthly_stats.max_humidity"""
        table =f"""td_weather_data AS ed 
                        INNER JOIN (SELECT  
                                        MAX(weather_data_id) AS max_weather_data_id,  
                                        YEAR(date) AS year, 
                                        MONTH(date) AS month 
                                    FROM td_weather_data 
                                    WHERE client_id = {user_data['client_id']} 
                                    AND device_id = {params.device_id} 
                                    GROUP BY  YEAR(date), MONTH(date) ) AS sub_ed 
                        ON ed.weather_data_id = sub_ed.max_weather_data_id
                        INNER JOIN (
                            SELECT
                                YEAR(date) AS year,
                                MONTH(date) AS month,
                                AVG(temperature) AS avg_temperature,
                                AVG(rainfall) AS avg_rainfall,
                                AVG(rainfall_cumulative) AS avg_rainfall_cumulative,
                                AVG(atm_pressure) AS avg_atm_pressure,
                                AVG(solar_radiation) AS avg_solar_radiation,
                                AVG(humidity) AS avg_humidity,
                                MIN(temperature) AS min_temperature,
                                MIN(rainfall) AS min_rainfall,
                                MIN(rainfall_cumulative) AS min_rainfall_cumulative,
                                MIN(atm_pressure) AS min_atm_pressure,
                                MIN(solar_radiation) AS min_solar_radiation,
                                MIN(humidity) AS min_humidity,
                                MAX(temperature) AS max_temperature,
                                MAX(rainfall) AS max_rainfall,
                                MAX(rainfall_cumulative) AS max_rainfall_cumulative,
                                MAX(atm_pressure) AS max_atm_pressure,
                                MAX(solar_radiation) AS max_solar_radiation,
                                MAX(humidity) AS max_humidity
                            FROM td_weather_data
                            WHERE client_id = {user_data['client_id']} 
                            AND device_id = {params.device_id}
                            GROUP BY YEAR(date), MONTH(date)
                        ) AS monthly_stats
                        ON YEAR(ed.date) = monthly_stats.year AND MONTH(ed.date) = monthly_stats.month"""
                        
        data = select_data(table,select, condition,order_by="ed.date DESC, ed.time DESC")
        return data
    except Exception as e:
        raise e
    
    


async def daily_report(params,user_data):
    try:
        condition = f"""CONCAT(td.date, ' ', td.time) = (
                            SELECT MAX(CONCAT(date, ' ', time))
                            FROM td_weather_data
                            WHERE client_id = {user_data['client_id']} 
                            AND device_id = {params.device_id}
                            AND date BETWEEN '{params.start_date}' AND '{params.end_date}'
                            AND DATE(date) = DATE(td.date)
                            AND HOUR(time) = HOUR(td.time) )"""
                            
        table = f"""td_weather_data AS td
                        JOIN (
                            SELECT 
                                DATE(DATE) AS DATE,
                                HOUR(TIME) AS hour,
                                AVG(temperature) AS avg_temperature,
                                MIN(temperature) AS min_temperature,
                                MAX(temperature) AS max_temperature,
                                AVG(rainfall) AS avg_rainfall,
                                MIN(rainfall) AS min_rainfall,
                                MAX(rainfall) AS max_rainfall,
                                AVG(rainfall_cumulative) AS avg_rainfall_cumulative,
                                MIN(rainfall_cumulative) AS min_rainfall_cumulative,
                                MAX(rainfall_cumulative) AS max_rainfall_cumulative,
                                AVG(atm_pressure) AS avg_atm_pressure,
                                MIN(atm_pressure) AS min_atm_pressure,
                                MAX(atm_pressure) AS max_atm_pressure,
                                AVG(solar_radiation) AS avg_solar_radiation,
                                MIN(solar_radiation) AS min_solar_radiation,
                                MAX(solar_radiation) AS max_solar_radiation,
                                AVG(humidity) AS avg_humidity,
                                MIN(humidity) AS min_humidity,
                                MAX(humidity) AS max_humidity,
                                AVG(wind_speed) AS avg_wind_speed,
                                MIN(wind_speed) AS min_wind_speed,
                                MAX(wind_speed) AS max_wind_speed,
                                AVG(wind_direction) AS avg_wind_direction,
                                MIN(wind_direction) AS min_wind_direction,
                                MAX(wind_direction) AS max_wind_direction
                            FROM
                                td_weather_data
                            WHERE
                                client_id = {user_data['client_id']} AND device_id = {params.device_id} AND DATE BETWEEN '{params.start_date}' AND '{params.end_date}'
                            GROUP BY
                                DATE(DATE),
                                HOUR(TIME)
                        ) AS stats
                        ON DATE(td.date) = stats.DATE AND HOUR(td.time) = stats.hour"""
                        
        select=f"""td.weather_data_id,
                    td.device_id,
                    td.device,
                    td.tw,
                    td.temperature,
                    td.rainfall,
                    td.rainfall_cumulative,
                    td.atm_pressure,
                    td.solar_radiation,
                    td.humidity,
                    td.wind_speed,
                    td.wind_direction,
                    DATE_FORMAT(td.date, '%Y-%m-%d') AS date,
                    TIME_FORMAT(td.time, '%H:%i:%s') AS time,
                    stats.avg_temperature,
                    stats.min_temperature,
                    stats.max_temperature,
                    stats.avg_rainfall,
                    stats.min_rainfall,
                    stats.max_rainfall,
                    stats.avg_rainfall_cumulative,
                    stats.min_rainfall_cumulative,
                    stats.max_rainfall_cumulative,
                    stats.avg_atm_pressure,
                    stats.min_atm_pressure,
                    stats.max_atm_pressure,
                    stats.avg_solar_radiation,
                    stats.min_solar_radiation,
                    stats.max_solar_radiation,
                    stats.avg_humidity,
                    stats.min_humidity,
                    stats.max_humidity,
                    stats.avg_wind_speed,
                    stats.min_wind_speed,
                    stats.max_wind_speed,
                    stats.avg_wind_direction,
                    stats.min_wind_direction,
                    stats.max_wind_direction
                    stats.max_rainfall as rain_intensity"""
        data = select_data(table,select, condition,order_by="DATE(td.date) DESC, TIME(td.time) DESC")
                        
        return data
    except Exception as e:
        raise e
    
    
    

async def temperature(params,user_data):
    try:
        select="CONCAT(DATE_FORMAT(ed.date,'%Y-%m-%d'),'T',DATE_FORMAT(ed.time,'%H:%i:%s')) AS date_time, ed.date, ed.time, ed.temperature"
        condition=f"ed.client_id = {user_data['client_id']} AND ed.device_id = {params.device_id} AND ed.date ='{params.start_date}'"
        data = select_data("td_weather_data AS ed",select, condition,order_by="date DESC, time DESC")
        return data
    except Exception as e:
        raise e
    

async def rainfall_data(params,user_data):
    try:
        select="CONCAT(DATE_FORMAT(ed.date,'%Y-%m-%d'),'T',DATE_FORMAT(ed.time,'%H:%i:%s')) AS date_time, ed.date, ed.time, ed.rainfall, ed.rainfall_cumulative"
        condition=f"ed.client_id = {user_data['client_id']} AND ed.device_id = {params.device_id} AND ed.date ='{params.start_date}'"
        data = select_data("td_weather_data AS ed",select, condition,order_by="date DESC, time DESC")
        return data
    except Exception as e:
        raise e


async def atm_pressure_data(params,user_data):
    try:
        select="CONCAT(DATE_FORMAT(ed.date,'%Y-%m-%d'),'T',DATE_FORMAT(ed.time,'%H:%i:%s')) AS date_time, ed.date, ed.time, ed.atm_pressure"
        condition=f"ed.client_id = {user_data['client_id']} AND ed.device_id = {params.device_id} AND ed.date ='{params.start_date}'"
        data = select_data("td_weather_data AS ed",select, condition,order_by="date DESC, time DESC")
        return data
    except Exception as e:
        raise e
    
    

async def solar_radiation_data(params,user_data):
    try:
        select="CONCAT(DATE_FORMAT(ed.date,'%Y-%m-%d'),'T',DATE_FORMAT(ed.time,'%H:%i:%s')) AS date_time, ed.date, ed.time, ed.solar_radiation"
        condition=f"ed.client_id = {user_data['client_id']} AND ed.device_id = {params.device_id} AND ed.date ='{params.start_date}'"
        data = select_data("td_weather_data AS ed",select, condition,order_by="date DESC, time DESC")
        return data
    except Exception as e:
        raise e
    

async def humidity_data(params,user_data):
    try:
        select="CONCAT(DATE_FORMAT(ed.date,'%Y-%m-%d'),'T',DATE_FORMAT(ed.time,'%H:%i:%s')) AS date_time, ed.date, ed.time, ed.humidity"
        condition=f"ed.client_id = {user_data['client_id']} AND ed.device_id = {params.device_id} AND ed.date ='{params.start_date}'"
        data = select_data("td_weather_data AS ed",select, condition,order_by="date DESC, time DESC")
        return data
    except Exception as e:
        raise e
    

async def wind_speed_data(params,user_data):
    try:
        select="CONCAT(DATE_FORMAT(ed.date,'%Y-%m-%d'),'T',DATE_FORMAT(ed.time,'%H:%i:%s')) AS date_time, ed.date, ed.time, ed.wind_speed"
        condition=f"ed.client_id = {user_data['client_id']} AND ed.device_id = {params.device_id} AND ed.date ='{params.start_date}'"
        data = select_data("td_weather_data AS ed",select, condition,order_by="date DESC, time DESC")
        return data
    except Exception as e:
        raise e
    
async def wind_direction_data(params,user_data):
    try:
        # select="CONCAT(DATE_FORMAT(ed.date,'%Y-%m-%d'),'T',DATE_FORMAT(ed.time,'%H:%i:%s')) AS date_time,ed.date, ed.time, ed.wind_speed,ed.wind_direction"
        select="CONCAT(DATE_FORMAT(ed.date,'%Y-%m-%d'),'T',DATE_FORMAT(ed.time,'%H:%i:%s')) AS date_time,ed.date, ed.time,ed.wind_direction"
        condition=f"ed.client_id = {user_data['client_id']} AND ed.device_id = {params.device_id} AND ed.date ='{params.start_date}'"
        data = select_data("td_weather_data AS ed",select, condition,order_by="date DESC, time DESC")
        return data
    except Exception as e:
        raise e
    



async def organization_settings(client_id,user_id,params):
    try:
        columndata="organization_id, client_id, countries_id, states_id, regions_id, subregions_id, cities_id, address, create_by, created_at"
        insdata=f"{params.organization_id}, {params.client_id}, {params.countries_id}, {params.states_id}, {params.regions_id}, {params.subregions_id}, {params.cities_id}, '{params.address}', {user_id}, '{get_current_datetime()}'"
        st_view_organization=insert_data("st_ms_organization",columndata,insdata)
    
    
        res={"settings_organization":st_view_organization}
        return res
    except Exception as e:
        raise e
    

async def organization_settings_list(client_id,user_id,params):
    try:
        condition = f"""st_org.cities_id = mlc.id
                        AND st_org.states_id = mls.id
                        AND st_org.countries_id = mlco.id
                        AND st_org.regions_id = mlr.id
                        AND st_org.subregions_id = mlsr.id
                        AND st_org.client_id = {client_id}
                        AND st_org.organization_id = {params.organization_id}"""
        
        select="st_org.id_ms_organization,st_org.organization_id, st_org.client_id, st_org.countries_id, st_org.states_id, st_org.regions_id, st_org.subregions_id, st_org.cities_id, st_org.address, DATE_FORMAT(st_org.created_at, '%Y-%m-%d %H:%i:%s') AS created_at, mlc.name AS cityes, mls.name AS state, mlco.name AS countries, mlr.name AS subregions, mlsr.name AS regions"
        table="st_ms_organization AS st_org, md_lo_cities AS mlc, md_lo_states AS mls, md_lo_countries AS mlco, md_lo_regions AS mlr, md_lo_subregions AS mlsr"
        data = select_data(table,select, condition)
        return data
    except Exception as e:
        raise e
    
    

    

async def edit_organization_info(client_id,user_id,params):
    try:
        update_condition=f"client_id = {client_id} AND organization_id = {params.organization_id}"
        set_values={"countries_id":params.countries_id,"states_id":params.states_id,"regions_id":params.regions_id,"subregions_id":params.subregions_id,"cities_id":params.cities_id,"address":params.address,"create_by":user_id}
        edit_organization=update_data("st_ms_organization",set_values,update_condition)
        return edit_organization
    except Exception as e:
        raise e