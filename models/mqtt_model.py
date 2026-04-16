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
    
#     device: str
#     do_type: int
#     do_no: int
#     one_on_time: str
#     one_off_time: str
#     two_on_time: str
#     two_off_time: str
#     datalog_sec: int
    
class MqttPublishDeviceSchedule(BaseModel):
    schedule_id: Optional[int] = None
    organization_id: Optional[int] = None
    device_id: Optional[int] = None
    device: str
    do_type: int    
    do_no: int
    slot: Optional[int] = None  # New field for slot number
    one_on_time: Optional[time] = None
    one_off_time: Optional[time] = None
    two_on_time: Optional[time] = None
    two_off_time: Optional[time] = None
    datalog_sec: Optional[int] = None
    days: Optional[str] = None  # New field for days of the week
    status: Optional[int] = 1   # 1=enabled, 0=disabled
    
class MqttReadSchedule(BaseModel):
    device: str
    do_no: int
    request_type: int
    device_id: str
    slot: Optional[int] = None  # New field for slot number
    do_type: Optional[int] = None
    one_on_time: Optional[time] = None
    one_off_time: Optional[time] = None
    days: Optional[str] = None
    status: Optional[int] = 1

class MqttReadLastData(BaseModel):
    device: str
    device_id: int
    request_type: int
    
class ResetMqttPublishDeviceSchedule(BaseModel):
    organization_id: int
    device_id: int
    device: str
   
class MqttPublishDeviceScheduleList(BaseModel):
    organization_id: int
    device_id: int
    device: str
    do_no: int
    slot: Optional[int] = None
   