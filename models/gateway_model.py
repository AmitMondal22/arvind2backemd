from pydantic import BaseModel, Field
from typing import Optional

class AddGateway(BaseModel):
    gateway_id: str = Field(..., min_length=10, max_length=10, pattern=r'^[A-Za-z0-9]+$')
    start_id: int
    max_id: int
    retry: int

class EditGateway(BaseModel):
    id: int
    gateway_id: str = Field(..., min_length=10, max_length=10, pattern=r'^[A-Za-z0-9]+$')
    start_id: int
    max_id: int
    retry: int

class DeleteGateway(BaseModel):
    id: int
    
