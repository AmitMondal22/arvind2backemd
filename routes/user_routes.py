from fastapi import APIRouter, HTTPException
from controllers.user import UserController

user_rutes = APIRouter()


@user_rutes.get("/users/{user_id}")
async def get_user(user_id: int):
    try:
        return UserController.get_user(user_id)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")

@user_rutes.post("/users/")
async def create_user(user_data: dict):
    try:
        return UserController.create_user(user_data)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")
    
@user_rutes.get("/users/")
async def get_users():
    try:
        return UserController.get_users()
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")