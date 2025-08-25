from pydantic import BaseModel, Field, constr, validator
from datetime import date

class AddProject(BaseModel):
    organization_id: int
    project_name: str


class EditProjectData(BaseModel):
    organization_id: int
    project_id: int
    project_name: str
    
class DeleteProject(BaseModel):
    project_id: int
    # created_by: int
    
class ListOrganization(BaseModel):
    client_id: int
    
class ProjectDeviceAdd(BaseModel):
    project_id: int
    organization_id: int
    device_id: int
    device: str
    
class ProjectDeviceDelete(BaseModel):
    manage_project_device_id: int
    