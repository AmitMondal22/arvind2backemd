from fastapi import APIRouter, HTTPException, Response, Depends
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from utils.response import errorResponse, successResponse
from models import device_data_model
from controllers.device_to_server import WaterController,DeviceController
from typing import Optional
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder

import json

devices_routes = APIRouter()

@devices_routes.post("/device_auto_register")
async def post_device_auto_register(data: device_data_model.DeviceAutoRegister):
    try:
        controllerRes =  await DeviceController.device_auto_register(data)
        resdata = successResponse(controllerRes, message="Device registered successfully")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")
    
    
@devices_routes.post('/checked_devices')
async def post_checked_devices(data: device_data_model.CheckedDevices):
    try:
        controllerRes =  await DeviceController.checked_devices(data)
        resdata = successResponse(controllerRes, message="Device checked successfully")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")

# ==============================================================================
# # modifications
# ==============================================================================
@devices_routes.post('/weather_data')
async def post_weather_data(data: device_data_model.WaterDeviceData):
    try:
        controllerRes =  await WaterController.get_weather_data(data,data.CL_ID,data.UID)
        resdata = successResponse(controllerRes, message="data stored successfully")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")
    
@devices_routes.post('/weather_data_api')
async def post_weather_data(data: device_data_model.WeatherDeviceDataApi):
    try:
        
        
        device_data = device_data_model.WeatherDeviceData(
           
            CL_ID  =data.CL_ID,
            UID=data.UID,
            DT=data.DT,
            TM=data.TM,
            TW=data.TW,
            
            
            C1= data.TEMP, #TEMP
            T1=0.00,
            PULSE1=data.RAIN, #RAIN
            
            PULSE2= 0.00,
            C3=data.ATM_PRESS, #ATM_PRESS
            T3=  0.00,
            C6=data.SOLAR_RAD, #SOLAR_RAD
            T6=  0.00,
            C2= data.HUMID, #HUMID
            T2=  0.00,
            C4= data.WIND_SPD, #WIND_SPD
            T4=  0.00,
            C5= data.WIND_DIR, #WIND_DIR
            T5=  0.00,
            RUNHR = data.RUNHR
        )
        
        
        controllerRes =  await WaterController.get_weather_data(device_data,device_data.CL_ID,device_data.UID)
        resdata = successResponse(controllerRes, message="data stored successfully")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")
    
# ==============================================================================
# # modifications
# ==============================================================================

@devices_routes.post("/waterflow_data_wfms")
async def post_ws_data(data: device_data_model.WsDeviceData):
    try:
        await WaterController.send_last_weather_data(client_id=data.client_id, device_id=data.device_id, device=data.device)
        resdata = successResponse("success", message="data stored successfully")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")

    
    
@devices_routes.post("/waterflow_client_data_wfms")
async def post_ws_data(data: device_data_model.WsDeviceOrgData2):
    # try:
        await WaterController.send_last_client_data_project(data.organization_id, data.project_id)
        resdata = successResponse("success", message="data stored successfully")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)

from config.db import connect

@devices_routes.post("/device_thresholds")
async def upsert_device_threshold(data: device_data_model.OmsDeviceThreshold):
    try:
        conn = connect()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO oms_device_thresholds (device, min_val, max_val, high_threshold, low_threshold)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            min_val = VALUES(min_val),
            max_val = VALUES(max_val),
            high_threshold = VALUES(high_threshold),
            low_threshold = VALUES(low_threshold)
        ''', (data.device, data.min_val, data.max_val, data.high_threshold, data.low_threshold))
        conn.commit()
        resdata = successResponse("success", message="Device thresholds updated successfully")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except Exception as e:
        if 'conn' in locals():
            conn.rollback()
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

@devices_routes.get("/device_thresholds/{device}")
async def get_device_threshold(device: str):
    try:
        conn = connect()
        cursor = conn.cursor(dictionary=True)

        cursor.execute(
            'SELECT * FROM oms_device_thresholds WHERE device = %s',
            (device,)
        )
        record = cursor.fetchone()

        if not record:
            record = {
                "device": device,
                "min_val": 4,
                "max_val": 20,
                "high_threshold": None,
                "low_threshold": None
            }

        resdata = successResponse(record, message="success")

        # ✅ Convert datetime → JSON serializable
        json_compatible_data = jsonable_encoder(resdata)

        return JSONResponse(content=json_compatible_data, status_code=200)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
