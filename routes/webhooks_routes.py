from fastapi import APIRouter, HTTPException,Request
from controllers.api import LoraApi
from typing import Dict, Any
import base64
from controllers.device_to_server import WaterController
from models import device_data_model


from utils.base64 import decode_base64
from utils.date_time_format import get_current_datetime_string

import json

webhooks_routes = APIRouter()




@webhooks_routes.post("/ws-webhooks")
@webhooks_routes.get("/ws-webhooks")
async def testing2(request: Request,event: str):
    # try:
    if event != "up":
      return {"status":"success"}   
    else:
        event =  await request.json()
        print(event)
        # Extract Device EUI and the uplink payload
        dev_eui = event.get("devEUI")
        # rssi = event.get("rxInfo")
        rxInfo = event.get("rxInfo")

        # Print RSSI value
        if rxInfo and len(rxInfo) > 0:
            rssi=rxInfo[0].get("rssi")
            # print(rxInfo[0].get("rssi"))
        else:
            rssi=0.0
        decodedev_eui=decode_base64(dev_eui)
        print(">>>>>>>>>>>>>>>>>>>>>>>decodedev_eui",decodedev_eui)

        uplink_data = event.get("data")
        print("uplink_data",uplink_data)
        decodeuplink_data=base64.b64decode(uplink_data).decode('utf-8')  
        data_list = decodeuplink_data.split(',')
        # 0         1        2        3     4   5   6    7         8          9        10         11          12  13    14   15
    #    clientid,VOLTAGE,CURRENT,REALPOWER,PF,KWH,RUNHR,frequency,UPLOADFLAG,DOMODE,sensorflag,log_sec_ref,sr_h,sr_m,ss_h,ss_m
    #    1,        0.00,    0.00,   0.00,  0.00,0.00,0.50, 0.00,       1,         1,     0,        30,        18,  0,   16,  30
                                                                                    # light status
    
    #  ['0.000', '0.00', '0.00', '0.00', '0.00', '0.50', '0.00', '1', '1', '0', '30', '18', '0', '16', '30']
        

        device_data = device_data_model.WeatherDeviceData(
            CLIENT_ID = data_list[0],
            UID=decodedev_eui,
            TW=rssi,  # TW is not provided in the data_list, so assign a default or calculated value
            VOLTAGE=float(data_list[1]),
            CURRENT=float(data_list[2]),
            REALPOWER=float(data_list[3]),
            PF=float(data_list[4]),
            KWH=float(data_list[5]),
            RUNHR=float(data_list[6]),
            FREQ=float(data_list[7]),
            UPLOADFLAG=int(data_list[8]),
            DOMODE=int(data_list[9]),
            SENSORFLAG=int(data_list[10])  # SENSORFLAG is not provided in the data_list, so assign a default or calculated value
        )
        
        
        # class WeatherDeviceData(BaseModel):    
        #         CL_ID:  Optional[int] = 0
        #         UID: str # device id
        #         DT: str
        #         TM: str
        #         TW: int
                
        #         C1: float #TEMP
        #         T1: Optional[float] = 0.00
        #         PULSE1: float #RAIN
                
        #         PULSE2: Optional[float] = 0.00
        #         C3: float #ATM_PRESS
        #         T3: Optional[float] = 0.00
        #         C6: float #SOLAR_RAD
        #         T6: Optional[float] = 0.00
        #         C2: float #HUMID
        #         T2: Optional[float] = 0.00
        #         C4: float #WIND_SPD
        #         T4: Optional[float] = 0.00
        #         C5: float #WIND_DIR
        #         T5: Optional[float] = 0.00
        #         RUNHR : Optional[float] = 0.00
        
        
        paydata=f"*R1, ,{get_current_datetime_string()},ZZ#"
        print("================================",paydata)
        # paydata=f"*R1, ,1,10,22,17,30,23,7,2034,16,07,33,ZZ#"
        
        # *R1, ,datalogtimeMin,SRHR,SRMM,SSHR,SSMM,DD,MM,YYYY,HR,MM,SS,DM,ZZ#

        #     Ex:
        #     *R1, ,1,10,22,17,30,23,7,2034,16,07,33,ZZ#
        await LoraApi.webhooks_send_downlink_test(decodedev_eui, paydata)
       
        

        await WaterController.get_weather_data(device_data,device_data.CL_ID,decodedev_eui)#device_data,client_id,decodedev_eui

      
        # print("?????????????????????????????????????")
        return {"status":"success"}
    # except Exception as e:
    #     raise e

