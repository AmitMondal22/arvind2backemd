from fastapi import APIRouter, HTTPException, Response, Depends
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from utils.response import errorResponse, successResponse
from models import device_data_model
from controllers.device_to_server import WaterController,DeviceController
from typing import Optional

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
            
            # TEMP=float(data.TEMP),
            
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
        print(">>>>>>>>>>>>>>....",data)
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
        print(">>>>>>>>>>>>>>....",data)
        await WaterController.send_last_client_data_project(data.organization_id, data.project_id)
        resdata = successResponse("success", message="data stored successfully")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    # except ValueError as ve:
    #     raise HTTPException(status_code=400, detail=str(ve))
    # except Exception as e:
    #     raise HTTPException(status_code=500, detail="Internal server error")

