from pydantic import BaseModel, Field, constr, validator
from datetime import date

class AddOrganization(BaseModel):
    client_id: int
    organization_name: str
    created_by: int


class EditOrganizationData(BaseModel):
    client_id: int
    organization_id: int
    organization_name: str
    created_by: int
    
class DeleteOrganization(BaseModel):
    client_id: int
    organization_id: int
    # created_by: int
    
class ListOrganization(BaseModel):
    client_id: int