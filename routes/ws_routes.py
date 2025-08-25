from fastapi import APIRouter,WebSocket
from Library.WsConnectionManagerManyDeviceTypes import WsConnectionManagerManyDeviceTypes
from utils.response import errorResponse, successResponse
import json

ws_routes = APIRouter()
manager = WsConnectionManagerManyDeviceTypes()


@ws_routes.websocket("/water_station/{data_type}/{client_id}/{device_id}/{device}")
async def websocket_endpoint(websocket: WebSocket, data_type: str, client_id: str, device_id: str, device: str):
    await manager.connect(data_type,client_id, device_id, device, websocket)
    try:
        while True:
            data = await websocket.receive_text()
            await manager.send_personal_message(data_type,client_id, device_id, device, f"Message '{data}' received from user {data_type}-{client_id}-{device_id}-{device}")
    except Exception as e:
        manager.disconnect(websocket,data_type,client_id, device_id, device)
        print(f"Connection with user {data_type}-{client_id}-{device_id}-{device} closed.")
        
        
@ws_routes.websocket("/water_station_client/{data_type}/{client_id}")
async def websocket_endpoint(websocket: WebSocket, data_type: str, client_id: str):
    await manager.connect_client(data_type,client_id, websocket)
    try:
        while True:
            data = await websocket.receive_text()
            await manager.send_personal_client_message(data_type,client_id, f"Message '{data}' received from user {data_type}-{client_id}")
    except Exception as e:
        manager.disconnect_client(websocket,data_type,client_id)
        print(f"Connection with user {data_type}-{client_id} closed.")




@ws_routes.post("/ws/send_message/{data_type}/{client_id}/{device_id}/{device}/{message}")
async def send_message(data_type:str,client_id: int,device_id:int,device:str, message: str):
    await manager.send_personal_message(data_type, client_id, device_id, device, json.dumps(message))
    return {"message": "Message sent successfully"}




async def sennd_ws_message(data_type:str,client_id: int,device_id:int,device:str, message: str):
    print(">>>>",data_type,client_id,device_id,device)
    await manager.send_personal_message(data_type, client_id, device_id, device, json.dumps(message))
    return {"message": "Message sent successfully"}

async def sennd_ws_client_message(data_type:str,client_id: int, message: str):
    await manager.send_personal_client_message(data_type, client_id, json.dumps(message))
    return {"message": "Message sent successfully"}