
from pydantic import BaseModel, Field, constr, validator
from datetime import date,datetime,time
import re
from typing import Optional,List

class EnergyUsageBilling(BaseModel):
    report_type: str
    device_id: int
    start_date_time: str
    end_date_time: Optional[str]
    
    @validator('report_type')
    def validate_report_type(cls, v):
        valid_report_type = {"M", "Y", "C"}
        if v not in valid_report_type:
            raise ValueError('Invalid report type')
        return v