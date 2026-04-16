from fastapi import APIRouter, HTTPException, Response, Depends, Request
from middleware.MyMiddleware import mw_auth, mw_client, mw_user_client
from controllers.management.GetwayController import GatewayController
from models.gateway_model import AddGateway, EditGateway, DeleteGateway
from utils.response import successResponse, errorResponse
from Library.DecimalEncoder import DecimalEncoder
import json

management_gateway_routes = APIRouter()

@management_gateway_routes.post("/add", dependencies=[Depends(mw_client)])
async def add_gateway(request: Request, gateway: AddGateway):
    try:
        user_data = request.state.user_data
        data = GatewayController.add_gateway(gateway, user_data)
        resdata = successResponse(data, message="Gateway added successfully")
        return Response(content=json.dumps(resdata, cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@management_gateway_routes.post("/list", dependencies=[Depends(mw_client)])
async def list_gateway(request: Request):
    try:
        data = GatewayController.list_gateway()
        resdata = successResponse(data, message="List of gateways")
        return Response(content=json.dumps(resdata, cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@management_gateway_routes.post("/edit", dependencies=[Depends(mw_client)])
async def edit_gateway(request: Request, gateway: EditGateway):
    try:
        user_data = request.state.user_data
        data = GatewayController.edit_gateway(gateway, user_data)
        
        # Build and send MQTT payload
        from routes.mqtt_routes import mqtt_client
        topic = f"/ARVIND/{gateway.gateway_id}"
        pubdata = f"*LORACFG,{gateway.start_id},{gateway.max_id},{gateway.retry},{gateway.id}#"
        mqtt_client.publish(topic, pubdata, qos=1)
        print("Publishing to config:", topic, pubdata)

        resdata = successResponse(data, message="Gateway edited successfully")
        return Response(content=json.dumps(resdata, cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@management_gateway_routes.post("/delete", dependencies=[Depends(mw_client)])
async def delete_gateway(request: Request, gateway: DeleteGateway):
    try:
        data = GatewayController.delete_gateway(gateway)
        resdata = successResponse(data, message="Gateway deleted successfully")
        return Response(content=json.dumps(resdata, cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
