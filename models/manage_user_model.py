from pydantic import BaseModel, Field, constr, validator
from datetime import date

class AddUser(BaseModel):
    name: str
    email: str
    password: str
    confirm_password: str
    organization_id: int

    @validator('confirm_password')
    def passwords_match(cls, v, values, **kwargs):
        if 'password' in values and v != values['password']:
            raise ValueError('Passwords do not match')
        return v

class EditUser(BaseModel):
    name: str
    email: str
    # password: str
    # confirm_password: str
    organization_id: int
    user_id: int

    # @validator('confirm_password')
    # def passwords_match(cls, v, values, **kwargs):
    #     if 'password' in values and v != values['password']:
    #         raise ValueError('Passwords do not match')
    #     return v
    
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
    
    
class DeviceListOrg(BaseModel):
    organization_id: int
    

class DeviceListOrgProject(BaseModel):
    organization_id: int
    project_id: int
    

class UserDeviceList(BaseModel):
    client_id: int
    organization_id: int
    

