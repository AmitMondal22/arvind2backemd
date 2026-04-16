from pydantic import BaseModel, Field, constr, validator
from datetime import date
from typing import List

class AddUser(BaseModel):
    name: str
    email: str
    mobile: str
    organization_id: int
    user_type: str
    client_id: int

class EditUser(BaseModel):
    name: str
    email: str
    mobile: str
    organization_id: int
    user_type: str
    client_id: int
    user_id: int
    
class DeleteUser(BaseModel):
    user_id: int
    
    
class UserDeviceAdd(BaseModel):
    client_id: int
    organization_id: int
    user_id: int
    device_id: int
    device: str
    created_by: int
    
    
class UserDeviceEdit(BaseModel):
    client_id: int
    organization_id: int
    user_id: int
    device_id: int
    device: str
    created_by: int
    manage_user_device_id: int

class UserDeviceDelete(BaseModel):
    client_id: int
    manage_user_device_id: int
    
class ListUsers(BaseModel):
    # organization_id: int
    client_id: int

class UserInfo(BaseModel):
    user_id: int
    client_id: int
    
class ClientId(BaseModel):
    client_id: int
    
class DeviceInfo(BaseModel):
    client_id: int
    device_id: int
    
    
class DeviceStatusUpdate(BaseModel):
    client_id: int
    device_id: List[int]  
    
    
class DeviceListOrg(BaseModel):
    organization_id: int
    

class DeviceListOrgProject(BaseModel):
    organization_id: int
    project_id: int

class DeviceListOrgProjectType(BaseModel):
    organization_id: int
    project_id: int
    device_type: str
    

class UserDeviceList(BaseModel):
    client_id: int
    organization_id: int
    

