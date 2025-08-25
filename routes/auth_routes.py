from fastapi import APIRouter, HTTPException, Response,Depends,Request
from controllers.auth import AuthController
from models import auth_model
from utils.response import successResponse
from Library.DecimalEncoder import DecimalEncoder
import json
from middleware.MyMiddleware import mw_client

auth_routes = APIRouter()


@auth_routes.post("/register")
async def register(user: auth_model.Register):
    try:
        data = await AuthController.register(user)   
        resdata = successResponse(data, message="User registered successfully")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error")
    
    
@auth_routes.post("/login")
async def login(user: auth_model.Login):
    try:
        data = await AuthController.login(user)
        resdata = successResponse(data, message="User logged in successfully")
        return Response(content=json.dumps(resdata,cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    # except Exception as 
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error")
    




# Apply middleware to specific routes
@auth_routes.get("/protected-route", dependencies=[Depends(mw_client)])
async def protected_route(request: Request):
    user_id = request.state.user_data
    username=user_id['user_name']
    return {"message": f"Authenticated user: {user_id['user_name']}"}




# Protected POST route
@auth_routes.post("/protected-route", dependencies=[Depends(mw_client)])
async def post_protected_route(request: Request,body: auth_model.Login):
    # Authorization check performed in the middleware
    print(body)
    user_data = request.state.user_data
    return {"message": f"Authenticated user (POST): {user_data['user_name']}"}