from fastapi import APIRouter, HTTPException, Response,WebSocket,WebSocketDisconnect,Depends,Request

from controllers.admin import ClientController, ManageUserController, DeviceManageUserController,DeviceController
from controllers.unit import UnitController
from controllers.alert import AlertController
from controllers.settings import ClientSettingsController

from models.organization_model import AddOrganization, EditOrganizationData, DeleteOrganization,ListOrganization
from models.project_model import AddProject,EditProjectData,DeleteProject,ProjectDeviceAdd,ProjectDeviceDelete
from models.manage_user_model import AddUser, EditUser,DeleteUser,UserDeviceAdd,UserDeviceEdit,UserDeviceDelete,ListUsers,UserInfo,ClientId,DeviceInfo,DeviceListOrg,DeviceListOrgProject
from models.device_data_model import WeatherFlowData,AddAlert,DeviceAdd,DeviceEdit,EditAlert,DeleteAlert,TemperatureUsed, VoltageData,OrganizationSettings,OrganizationSettingsList,EditOrganization, ChartData
from models.client_settings import ClientScreenSettings, ClientScreenSettingsEdit

from Library.DecimalEncoder import DecimalEncoder
from Library.CustomEncoder import CustomEncoder
from Library import EmailLibrary
from Library.WsConnectionManager import WsConnectionManager

from db_model.MASTER_MODEL import select_one_data

from middleware.MyMiddleware import mw_client,mw_user,mw_user_client

from utils.response import errorResponse, successResponse
from typing import List
import json

api_client_routes = APIRouter()
manager = WsConnectionManager()

# ==========================================================================
# ==========================================================================

# both
@api_client_routes.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            await manager.send_personal_message(f"Received:{data}",websocket)
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        await manager.send_personal_message("Bye!!!",websocket)
        
#  send_message      
@api_client_routes.get("/send_message", dependencies=[Depends(mw_client)])
async def send_message(message: str):
    await manager.broadcast(message)
    return {"message": "Sent message: {}".format(message)}

# daynamic data
@api_client_routes.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: int):
    await manager.connect(user_id, websocket)
    try:
        while True:
            data = await websocket.receive_text()
            await manager.send_personal_message(user_id, f"Message '{data}' received from user {user_id}")
    except Exception as e:
        manager.disconnect(user_id)
        print(f"Connection with user {user_id} closed.")
        
# @api_client_routes.post("/send_message/{user_id}")
# async def send_message(user_id: int, message: str):
#     await manager.send_personal_message(user_id, message)
#     return {"message": "Message sent successfully"}


@api_client_routes.post("/send_message/{client_id}/{device_id}/{device}/{message}", dependencies=[Depends(mw_client)])
async def send_message(client_id: int,device_id:int,device:str, message: str):
    await manager.send_personal_message(client_id, device_id, device, json.dumps(message))
    return {"message": "Message sent successfully"}


@api_client_routes.websocket("/ws/{client_id}/{device_id}/{device}")
async def websocket_endpoint(websocket: WebSocket, client_id: str, device_id: str, device: str):
    await manager.connect(client_id, device_id, device, websocket)
    try:
        while True:
            data = await websocket.receive_text()
            await manager.send_personal_message(client_id, device_id, device, f"Message '{data}' received from user {client_id}-{device_id}-{device}")
    except Exception as e:
        manager.disconnect(websocket,client_id, device_id, device)
        print(f"Connection with user {client_id}-{device_id}-{device} closed.")


# ================================================================
# ================================================================
class SendEnergySocket:
    
    async def send_last_weather_data(client_id, device_id, device):
        try:
            # Lazy import inside the function
            # from Library import WsConnectionManager
            # manager = WsConnectionManager.WsConnectionManager()
            
            select="weather_data_id, client_id, device_id,tw, temperature, rainfall, rainfall_cumulative, atm_pressure, solar_radiation, humidity, wind_speed, wind_direction, runhr, date, time, DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s') AS created_at"
            condition = f"device_id = '{device_id}' AND device ='{device}' AND client_id = '{client_id}'"
            order_by="weather_data_id DESC"
                
            lastdata = select_one_data("td_weather_data", select, condition, order_by)
           
            await manager.send_personal_message(client_id, device_id, device, json.dumps(lastdata, cls=DecimalEncoder))
            
            # await manager.send_personal_message(client_id, device_id, device, json.dumps("hello134"))
            
            
            # await manager.send_personal_message(1, 1, "aa", json.dumps("hello world"))
            print("lastdata last energy data>>>>>>>>>>/////////",lastdata)
            return lastdata
        except Exception as e:
            raise ValueError("Could not fetch data")
# ================================================================
# ================================================================
  

  
# ==========================================================================
# ==========================================================================


@api_client_routes.post("/manage_organization/add", dependencies=[Depends(mw_client)])
async def add_organization(request: Request,organization:AddOrganization):
    try:
        data = ClientController.add_organization(organization)   
        resdata = successResponse(data, message="User registered successfully")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error")
    
    
    
    
@api_client_routes.post("/manage_organization/list", dependencies=[Depends(mw_user_client)])
async def list_organization(request: Request,params:ListOrganization):
    try:
        # print(params)
        user_data=request.state.user_data
        data = ClientController.list_organization(params,user_data)
        resdata = successResponse(data, message="List of organizations")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error")
    
@api_client_routes.post("/manage_organization/edit", dependencies=[Depends(mw_client)])
async def edit_organization(request: Request,organization:EditOrganizationData):
    try:
        data = ClientController.edit_organization(organization)
        resdata = successResponse(data, message="List of organizations")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error ")
    

@api_client_routes.post("/manage_organization/delete", dependencies=[Depends(mw_client)])
async def delete_organization(request: Request,organization:DeleteOrganization):
    try:
        data = ClientController.delete_organization(organization)
        resdata = successResponse(data, message="Organization deleted successfully")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error ")



# ==========================================================================
# ==========================================================================


@api_client_routes.post("/manage_projects/add", dependencies=[Depends(mw_client)])
async def add_projects(request: Request,projectdata:AddProject):
    try:
        user_credentials = request.state.user_data
        data = ClientController.add_projects(projectdata,user_credentials)   
        resdata = successResponse(data, message="Project registered successfully")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error")
    
@api_client_routes.post("/manage_projects/list", dependencies=[Depends(mw_user_client)])
async def list_project(request: Request):
    try:
        # print(params)
        user_data=request.state.user_data
        data = ClientController.list_project(user_data)
        resdata = successResponse(data, message="List of projects")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error")
    
    
    
@api_client_routes.post("/manage_projects/edit", dependencies=[Depends(mw_client)])
async def edit_projects(request: Request,project:EditProjectData):
    try:
        user_data=request.state.user_data
        data = ClientController.edit_projects(project,user_data)
        resdata = successResponse(data, message="List of organizations")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error ")
    
    
    
@api_client_routes.post("/manage_projects/delete", dependencies=[Depends(mw_client)])
async def delete_manage_projects(request: Request,project:DeleteProject):
    try:
        data = ClientController.delete_manage_projects(project)
        resdata = successResponse(data, message="Project deleted successfully")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error ")



# ==========================================================================
# ==========================================================================
@api_client_routes.post("/manage_organization_project/list", dependencies=[Depends(mw_user_client)])
async def manage_organization_project(request: Request):
    try:
        # print(params) 
        user_data=request.state.user_data
        data = ClientController.manage_organization_project(user_data)
        resdata = successResponse(data, message="List of organizations nested in projects")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error")
    
    
    
@api_client_routes.post("/manage_project/project_add_device", dependencies=[Depends(mw_client)])
async def project_add_device(request: Request,project_device:ProjectDeviceAdd):
    try:
        user_data=request.state.user_data
        data = DeviceManageUserController.project_add_device(project_device,user_data)
        resdata = successResponse(data, message="Device added successfully")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error")
    
    
@api_client_routes.post("/manage_project/project_list_device", dependencies=[Depends(mw_client)])
async def project_list_device(request: Request):
    try:
        user_data=request.state.user_data
        data = DeviceManageUserController.project_list_device(user_data)
        resdata = successResponse(data, message="Device added successfully")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error")
    
    

    
    
    
@api_client_routes.post("/manage_project/project_delete_device", dependencies=[Depends(mw_client)])
async def project_delete_device(request: Request,project_device:ProjectDeviceDelete):
    try:
        user_data=request.state.user_data
        data = DeviceManageUserController.project_delete_device(project_device)
        resdata = successResponse(data, message="Device added successfully")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error")

# ==========================================================================
# ==========================================================================



@api_client_routes.post("/manage_user/add", dependencies=[Depends(mw_client)])
async def add_user(request: Request,user:AddUser):
    try:
        data = ManageUserController.add_user(user)   
        resdata = successResponse(data, message="User registered successfully")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error")
    
@api_client_routes.post("/manage_user/list", dependencies=[Depends(mw_client)])
async def list_user(request: Request,params:ListUsers):
    try:
        data = ManageUserController.list_user(params)
        resdata = successResponse(data, message="List of users")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error")
    
@api_client_routes.post("/manage_user/list_user", dependencies=[Depends(mw_client)])
# @api_client_routes.get("/manage_user/list_user/{user_id}")
async def list_user(request: Request,params:UserInfo):
    try:
        data = ManageUserController.user_info(params)
        resdata = successResponse(data, message="List of users")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error")
    
@api_client_routes.post("/manage_user/edit", dependencies=[Depends(mw_client)])
async def edit_user(request: Request,user:EditUser):
    try:
        data = ManageUserController.edit_user(user)
        resdata = successResponse(data, message="List of users")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error ")


@api_client_routes.post("/manage_user/delete", dependencies=[Depends(mw_client)])
async def delete_user(request: Request,user:DeleteUser):
    try:
        data = ManageUserController.delete_user(user)
        resdata = successResponse(data, message="User deleted successfully")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error ")
    
# =================================================================================================
# =================================================================================================



@api_client_routes.post("/manage_user/add_device", dependencies=[Depends(mw_client)])
async def add_device(request: Request,user:UserDeviceAdd):
    try:
        data = DeviceManageUserController.add_device(user)
        resdata = successResponse(data, message="Device added successfully")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error")
    



    
    
@api_client_routes.post("/manage_user/list_user_device", dependencies=[Depends(mw_client)])
async def list_user_device(request: Request,params:ClientId):
    try:
        data = DeviceManageUserController.list_user_device(params)
        resdata = successResponse(data, message="List of users")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error")
    
    
@api_client_routes.post("/manage_user/edit_user_device", dependencies=[Depends(mw_client)])
async def edit_user_device(request: Request,user:UserDeviceEdit):
    try:
        data = DeviceManageUserController.edit_device(user)
        resdata = successResponse(data, message="List of user devices")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error ")
    
    
@api_client_routes.post("/manage_user/delete_user_device", dependencies=[Depends(mw_client)])
async def delete_user_device(request: Request,user:UserDeviceDelete):
    try:
        data = DeviceManageUserController.delete_device(user)
        resdata = successResponse(data, message="List of users")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error ")
    
# =================================================================================================
# =================================================================================================

@api_client_routes.post("/devices/list", dependencies=[Depends(mw_user_client)])
async def list_device(request: Request,params:DeviceListOrg):
# async def list_device(request: Request):
    try:
        user_credentials = request.state.user_data
        client_id=user_credentials['client_id']
        if user_credentials["user_type"] == "U":
            user_id=user_credentials["user_id"]
            organization_id=user_credentials["organization_id"]
            data= await DeviceController.user_device_list(client_id, user_id, organization_id)
        else:
             data= await DeviceController.list_device(client_id, params.organization_id)
            # data = await DeviceController.list_device(client_id)
        resdata = successResponse(data, message="List of devices")
        return Response(content=json.dumps(resdata,cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error")
    
@api_client_routes.post("/project/devices/list", dependencies=[Depends(mw_user_client)])
async def list_device(request: Request,params:DeviceListOrgProject):
# async def list_device(request: Request):
    try:
        user_credentials = request.state.user_data
        client_id=user_credentials['client_id']
        project_id=params.project_id
        if user_credentials["user_type"] == "U":
            user_id=user_credentials["user_id"]
            organization_id=user_credentials["organization_id"]
            data= await DeviceController.project_user_device_list(client_id, user_id, organization_id,project_id)
        else:
             data= await DeviceController.project_list_device(client_id, params.organization_id,project_id)
            # data = await DeviceController.list_device(client_id)
        resdata = successResponse(data, message="List of devices")
        return Response(content=json.dumps(resdata,cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error")
    
    
@api_client_routes.post("/devices_list", dependencies=[Depends(mw_user_client)])
async def list_device(request: Request):
# async def list_device(request: Request):
    try:
        user_credentials = request.state.user_data
        client_id=user_credentials['client_id']
        if user_credentials["user_type"] == "U":
            user_id=user_credentials["user_id"]
            organization_id=user_credentials["organization_id"]
            data= await DeviceController.user_device_list(client_id, user_id, organization_id)
        else:
             data= await DeviceController.all_list_device(client_id)
            # data = await DeviceController.list_device(client_id)
        resdata = successResponse(data, message="List of devices")
        return Response(content=json.dumps(resdata,cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error")
    
    
    
@api_client_routes.post("/devices/device_info", dependencies=[Depends(mw_user_client)])
async def list_device(request: Request,params:DeviceInfo):
    try:
        userdata=request.state.user_data
        data = await DeviceController.device_info(params,userdata)
        resdata = successResponse(data, message="List of devices")
        return Response(content=json.dumps(resdata,cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error")



# @api_client_routes.post("/manage/devices/add")
@api_client_routes.post("/manage/devices/add", dependencies=[Depends(mw_client)])
async def add_device(request: Request,params:List[DeviceAdd]):
    try:
        data = await DeviceController.add_device(params)
        resdata = successResponse(data, message="Device added successfully")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")
    
    
@api_client_routes.post("/manage/devices/edit", dependencies=[Depends(mw_client)])
async def edit_device(request: Request,params:DeviceEdit):
    try:
        data = await DeviceController.edit_device(params)
        resdata = successResponse(data, message="Device edited successfully")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")



@api_client_routes.post("/manage/devices/list", dependencies=[Depends(mw_client)])
async def list_device(request: Request,params:ClientId):
    try:
        data = await DeviceController.manage_list_device(params)
        resdata = successResponse(data, message="List of devices")
        return Response(content=json.dumps(resdata, cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error")

# =================================================================================================
                # graphical_view edit and change
# =================================================================================================
# @api_client_routes.post("/devices/energy_data", dependencies=[Depends(mw_user_client)])
# async def energy_data(request: Request,params:WeatherFlowData):
#     try:
#         data = ClientController.energy_data(params)
#         resdata = successResponse(data, message="devices Data")
#         return Response(content=json.dumps(resdata,cls=DecimalEncoder), media_type="application/json", status_code=200)
#     except ValueError as ve:
#         # If there's a ValueError, return a 400 Bad Request with the error message
#         raise HTTPException(status_code=400, detail=str(ve))
#     except Exception as e:
#         # For any other unexpected error, return a 500 Internal Server Error
#         raise HTTPException(status_code=500, detail="Internal server error")
    
    
@api_client_routes.post("/report/water_flow_data", dependencies=[Depends(mw_user_client)])
async def water_flow_data(request: Request,params:WeatherFlowData):
    try:
        user_data=request.state.user_data
        data = await DeviceController.water_flow_data(params,user_data)
        resdata = successResponse(data, message="weather Data")
        print(">>>>>>>>>>>>>>>>>>>>>",resdata)
        return Response(content=json.dumps(resdata,cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error")
    
    
    
@api_client_routes.post("/report/last1000data", dependencies=[Depends(mw_user_client)])
async def last_chart_data(request: Request,params:ChartData):
    try:
        user_data=request.state.user_data
        data = await DeviceController.last_chart_data(params,user_data)
        resdata = successResponse(data, message="weather Data")
        print(">>>>>>>>>>>>>>>>>>>>>",resdata)
        return Response(content=json.dumps(resdata,cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error")
    
    


@api_client_routes.post('/report/monthly_report', dependencies=[Depends(mw_user_client)])
async def monthly_report(request: Request,params:WeatherFlowData):
    try:
        userdata=request.state.user_data
        data = await DeviceController.monthly_report(params,userdata)
        resdata = successResponse(data, message="Monthly report")
        return Response(content=json.dumps(resdata,cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")
    

@api_client_routes.post('/report/yearly_report', dependencies=[Depends(mw_user_client)])
async def yearly_report(request: Request,params:WeatherFlowData):
    try:
        userdata=request.state.user_data
        data = await DeviceController.yearly_report(params,userdata)
        resdata = successResponse(data, message="Monthly report")
        return Response(content=json.dumps(resdata,cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")
    
@api_client_routes.post('/report/daily_report', dependencies=[Depends(mw_user_client)])
async def daily_report(request: Request,params:WeatherFlowData):
    try:
        userdata=request.state.user_data
        data = await DeviceController.daily_report(params,userdata)
        resdata = successResponse(data, message="Monthly report")
        return Response(content=json.dumps(resdata,cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")
    
    
    
    
    # =================================================================================================
    
@api_client_routes.post("/devices/graphical_view/temperature", dependencies=[Depends(mw_user_client)])
async def temperature(request: Request,params:TemperatureUsed):
    try:
        user_data=request.state.user_data
        data = await DeviceController.temperature(params,user_data)
        resdata = successResponse(data, message="Temperature Data")
        return Response(content=json.dumps(resdata,cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")
    


@api_client_routes.post("/devices/graphical_view/rainfall", dependencies=[Depends(mw_user_client)])
async def rainfall_data(request: Request,params:TemperatureUsed):
    try:
        user_data=request.state.user_data
        data = await DeviceController.rainfall_data(params,user_data)
        resdata = successResponse(data, message="Rainfall Data")
        return Response(content=json.dumps(resdata,cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")


@api_client_routes.post("/devices/graphical_view/atm_pressure", dependencies=[Depends(mw_user_client)])
async def atm_pressure(request: Request,params:TemperatureUsed):
    try:
        user_data=request.state.user_data
        data = await DeviceController.atm_pressure_data(params,user_data)
        resdata = successResponse(data, message="Atomic pressure Data")
        return Response(content=json.dumps(resdata,cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")
    

@api_client_routes.post("/devices/graphical_view/solar_radiation", dependencies=[Depends(mw_user_client)])
async def solar_radiation(request: Request,params:TemperatureUsed):
    try:
        user_data=request.state.user_data
        data = await DeviceController.solar_radiation_data(params,user_data)
        resdata = successResponse(data, message="Total solar radiation Data")
        return Response(content=json.dumps(resdata,cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")
    
@api_client_routes.post("/devices/graphical_view/humidity", dependencies=[Depends(mw_user_client)])
async def humidity(request: Request,params:TemperatureUsed):
    try:
        user_data=request.state.user_data
        data = await DeviceController.humidity_data(params,user_data)
        resdata = successResponse(data, message="Total humidity Data")
        return Response(content=json.dumps(resdata,cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")
    
@api_client_routes.post("/devices/graphical_view/wind_direction", dependencies=[Depends(mw_user_client)])
async def wind_direction(request: Request,params:TemperatureUsed):
    try:
        user_data=request.state.user_data
        data = await DeviceController.wind_direction_data(params,user_data)
        resdata = successResponse(data, message="wind direction Data")
       
        return Response(content=json.dumps(resdata,cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")
    
@api_client_routes.post("/devices/graphical_view/wind_speed", dependencies=[Depends(mw_user_client)])
async def wind_speed(request: Request,params:TemperatureUsed):
    try:
        user_data=request.state.user_data
        data = await DeviceController.wind_speed_data(params,user_data)
        resdata = successResponse(data, message="wind speed Data")
        return Response(content=json.dumps(resdata,cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")


# =================================================================================================
# =================================================================================================










# =================================================================================================

            # Unit And Alert  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# =================================================================================================



# @api_client_routes.post("/unit/list", dependencies=[Depends(mw_client)])
@api_client_routes.post("/unit/list")
async def list_unit(request: Request):
    try:
        data = await UnitController.list_unit()
        resdata = successResponse(data, message="List of units")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error")
    

@api_client_routes.post("/alert/add", dependencies=[Depends(mw_client)])
async def add_alert(request: Request,alert:List[AddAlert]):
    try:
        data = AlertController.add_alert(alert)
        resdata = successResponse(data, message="Alert added successfully")
        print(">>>>>>>>>>>>>>>>>>>>>",resdata)
        return Response(content=json.dumps(resdata,cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error")

@api_client_routes.post("/alert/list", dependencies=[Depends(mw_user_client)])
async def list_alert(request: Request,params:ClientId):
    try:
        user_data=request.state.user_data
        data = await AlertController.list_alert(params,user_data)
        resdata = successResponse(data, message="List of alerts")
        return Response(content=json.dumps(resdata,cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error")
    
@api_client_routes.post("/alert/edit", dependencies=[Depends(mw_client)])
async def edit_alert(request: Request,params:EditAlert):
    try:
        data = await AlertController.edit_alert(params)
        resdata = successResponse(data, message="Alert edited successfully")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error")

@api_client_routes.post("/alert/delete", dependencies=[Depends(mw_client)])
async def delete_alert(request: Request,params:DeleteAlert):
    try:
        data = await AlertController.delete_alert(params)
        if data > 0:
            resdata = successResponse(data, message="Alert deleted successfully")
            return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
        else:
            resdata = errorResponse(message="Alert not deleted successfully")
            return Response(content=json.dumps(resdata), media_type="application/json", status_code=404)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error")
    
    
    
# ====================================================================================
# organization_settings
# ====================================================================================


@api_client_routes.post("/organization_settings", dependencies=[Depends(mw_client)])
async def organization_settings(request: Request,params:OrganizationSettings):
    try:
        userdata=request.state.user_data
        client_id=userdata['client_id']
        user_id=userdata["user_id"]
        data = await DeviceController.organization_settings(client_id,user_id,params)
        resdata = successResponse(data, message="Organization settings")
        return Response(content=json.dumps(resdata,cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")
    
@api_client_routes.post("/organization_settings/list", dependencies=[Depends(mw_client)])
async def organization_settings_list(request: Request,params:OrganizationSettingsList):
    try:
        userdata=request.state.user_data
        client_id=userdata['client_id']
        user_id=userdata["user_id"]
        data = await DeviceController.organization_settings_list(client_id,user_id,params)
        resdata = successResponse(data, message="Organization settings")
        return Response(content=json.dumps(resdata,cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")
    

    
    

    
@api_client_routes.post("/organization_settings/edit_organization_info", dependencies=[Depends(mw_client)])
async def edit_organization_info(request: Request,params:EditOrganization):
    try:
        userdata=request.state.user_data
        client_id=userdata['client_id']
        user_id=userdata["user_id"]
        data = await DeviceController.edit_organization_info(client_id,user_id,params)
        resdata = successResponse(data, message="Organization settings")
        return Response(content=json.dumps(resdata,cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")




    
# ===========================================================
# change screen settings
# ===========================================================

@api_client_routes.post("/settings/client_screen_settings", dependencies=[Depends(mw_client)])
async def client_screen_settings(request: Request,params:ClientScreenSettings):
    try:
        userdata=request.state.user_data
        data = await ClientSettingsController.client_screen_settings(userdata,params)
        resdata = successResponse(data, message="Organization settings")
        return Response(content=json.dumps(resdata,cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")
    
@api_client_routes.post("/settings/client_screen_settings_edit", dependencies=[Depends(mw_client)])
async def client_screen_settings_edit(request: Request,params:ClientScreenSettingsEdit):
    try:
        userdata=request.state.user_data
        data = await ClientSettingsController.client_screen_settings_edit(userdata,params)
        resdata = successResponse(data, message="Organization settings add and edit successfully")
        return Response(content=json.dumps(resdata,cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")
    
    
    
    

    
