from pydantic import BaseModel, Field, constr, field_validator,FieldValidationInfo
from datetime import date,datetime,time
from typing import Optional, List 



class MqttWfmsDO(BaseModel):
    device: str
    device_id: int
    do_no: int
    do_status: int


class DigitalOutput(BaseModel):
    do_no: int
    do_status: int

class MqttAllWfmsDO(BaseModel):
    device: str
    device_id: int
    do: List[DigitalOutput]
    
class MqttPublishDeviceSchedule(BaseModel):
    schedule_id:Optional[int]=0
    organization_id: Optional[int]=None
    device_id: Optional[int]=None
    device: str
    do_type: int
    do_no: int
    one_on_time: str
    one_off_time: str
    two_on_time: str
    two_off_time: str
    datalog_sec: int
    
class MqttReadSchedule(BaseModel):
    device: str
    do_no: int
    
class ResetMqttPublishDeviceSchedule(BaseModel):
    organization_id: int
    device_id: int
    device: str
   
class MqttPublishDeviceScheduleList(BaseModel):
    organization_id: int
    device_id: int
    device: str
    do_no: int
   