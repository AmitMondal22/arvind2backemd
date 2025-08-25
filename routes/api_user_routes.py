from fastapi import APIRouter, HTTPException, Response, UploadFile,Depends,Request,Form,File
from models import auth_model
from utils.response import errorResponse, successResponse
from pathlib import Path
from datetime import datetime
from fastapi.responses import FileResponse

from controllers.device_to_server import DeviceController
from models import device_data_model
from Library.DecimalEncoder import DecimalEncoder
import re
import time
import json
import shutil
import hashlib
from middleware.MyMiddleware import mw_client,mw_user,mw_user_client
from controllers.super_admin import UserInfoClass
from models.auth_model import ChangePassword

user_routes = APIRouter()



ALLOWED_EXTENSIONS = {"jpg", "jpeg", "png", "gif"}
MAX_SIZE_MB = 2

def allowed_file(filename: str) -> bool:
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def sanitize_filename(filename: str) -> str:
    # Remove spaces and special characters, replace with underscores
    filename = re.sub(r'\s+', '_', filename)  # Replace spaces with underscores
    filename = re.sub(r'[^\w\._]', '', filename)  # Remove any non-alphanumeric characters
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    return f"{timestamp}_{filename}"



# @user_routes.post("/client_logo_upload",dependencies=[Depends(mw_client)])
@user_routes.post("/client_logo_upload",dependencies=[Depends(mw_user_client)])
async def upload_file(
    request: Request,
    client_name: str = Form(...),
    client_address: str = Form(...),
    client_mobile: str = Form(...),
    client_email: str = Form(...),
    file: UploadFile=File(None)
    ):
    user_data=request.state.user_data
    if file is None:
        
        dupadetdata=await UserInfoClass.upload_update_client_logo(user_data,client_name,client_address,client_mobile,client_email,None)
        return {dupadetdata}
    
    else:
        
        UPLOAD_DIR = Path("upload/client_image")
        UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        if not allowed_file(file.filename):
            raise HTTPException(status_code=400, detail="Invalid file type")

        # Check file size
        contents = await file.read()
        if len(contents) > MAX_SIZE_MB * 1024 * 1024:
            raise HTTPException(status_code=400, detail="File size exceeds 2 MB")
        
        file.seek(0)  # Reset file pointer to the beginning

        UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        sanitized_filename = sanitize_filename(file.filename)
        file_location = UPLOAD_DIR / sanitized_filename
        
        if user_data['logo'] is not None:
            try:
                file_path = UPLOAD_DIR / user_data['logo']
                if file_path.is_file():
                    file_path.unlink()
            except Exception as e:
                pass

        try:
            with file_location.open("wb") as buffer:
                buffer.write(contents)
            
            await UserInfoClass.upload_update_client_logo(user_data,client_name,client_address,client_mobile,client_email,sanitized_filename)
            return {"filename": sanitized_filename, "location": str(file_location)}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to save file: {str(e)}")



    
    
    
@user_routes.post("/device/list")
async def get_device_list(user_id: device_data_model.UserDeviceList):
    try:
        data = await DeviceController.user_device_list(user_id)
        resdata = successResponse(data, message="List of devices")
        return Response(content=json.dumps(resdata, cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")

@user_routes.get("/view-image/{filename}",dependencies=[Depends(mw_user_client)])
async def view_image(filename: str):
    UPLOAD_DIR = Path("upload/client_image")
    file_path = UPLOAD_DIR / filename
    if not file_path.is_file():
        raise HTTPException(status_code=404, detail="File not found.")
    return FileResponse(file_path)


@user_routes.post("/change_password",dependencies=[Depends(mw_user_client)])
async def change_password( request: Request,params:ChangePassword):
    try:
        user_data=request.state.user_data
        data = await UserInfoClass.change_password(user_data,params)
        resdata = successResponse(data, message="Password changed successfully")
        return Response(content=json.dumps(resdata, cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        # If there's a ValueError, return a 400 Bad Request with the error message
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # For any other unexpected error, return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail="Internal server error")