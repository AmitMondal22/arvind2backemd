from pydantic import BaseModel, Field, constr, validator
from datetime import date,datetime,time
import re
from typing import Optional


class SubRegionsM(BaseModel):
    region_id: Optional[int] = None
    
    
class CountryRequest(BaseModel):
    country_id: int = Field(..., description="ID of the country to fetch states for")

class StateRequest(BaseModel):
    state_id: int = Field(..., description="ID of the state to fetch cities for")