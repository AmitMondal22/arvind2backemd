from pydantic import BaseModel, Field, constr, validator
from datetime import date,datetime,time
import re
from typing import Optional,List


class WaterDeviceData(BaseModel):
    UID: str # device id
    DT: str
    # TM: str
    TIME: str
    TW: Optional[float] = 0.00
    A1: Optional[float] = 0.00
    A2: Optional[float] = 0.00
    TOT1: Optional[float] = 0.00
    TOT2: Optional[float] = 0.00
    # FR1: Optional[float] = 0.00
    # DI: Optional[str] = ''
    # IO_ID: Optional[str] = ''
    DO: Optional[str] = ''
    BAT_V: Optional[float] = 0.00
    
    
    
#     {
#   "TYPE": "N",
#   "HEADER": "IO",
#   "UID": "ABC123",
#   "DT": "2024.12.18",         // GPRS Date in YYYY.MM.DD format
#   "TIME": "15:30:45",         // GPRS Time in HH:MM:SS format
#   "TW": "21",
#   "A1": 12.57,
#   "TOT1": 12345,
#   "DO": "10101010",
#   "BAT(V)": 3.99,
#   "ContCode": "91",
#   "IO_ID": "0"
# }
    
class WeatherDeviceDataApi(BaseModel):
    CL_ID:  int
    UID: str # device id
    DT: str
    TM: str
    TW: int
    TEMP: Optional[float] = 0.00
    RAIN: Optional[float] = 0.00
    RAIN_CUM: Optional[float] = 0.00
    ATM_PRESS: Optional[float] = 0.00
    SOLAR_RAD: Optional[float] = 0.00
    HUMID: Optional[float] = 0.00
    WIND_SPD: Optional[float] = 0.00
    WIND_DIR: Optional[float] = 0.00
    RUNHR : Optional[float] = 0.00
    
#     {
#   "TYPE": "N", // Assuming logType = 0
#   "UID": "12345", // Example UID
#   "DT": "2024-07-25", // Example date
#   "TM": "12:34:56", // Example time
#   "TW": 80, // Example signal strength
#   "C1N": "Temperature",
#   "C1U": "Celsius",
#   "C1": 25.50,
#   "C2N": "Pressure",
#   "C2U": "Bar",
#   "C2": 1.23,
#   "C3N": "Humidity",
#   "C3U": "Percent",
#   "C3": 45.67,
#   "C4N": "Flow Rate",
#   "C4U": "Liters/Minute",
#   "C4": 30.12,
#   "C5N": "Voltage",
#   "C5U": "Volts",
#   "C5": 220.5,
#   "C6N": "Current",
#   "C6U": "Amperes",
#   "C6": 10.75,
#   "PULSE1": 1500.0,
#   "PULSE2": 3000.0,
#   "DI1": true,  // Digital Input 1 state, true indicates 'ON'
#   "DI2": false  // Digital Input 2 state, false indicates 'OFF'

# t1 to t6 di1,di2 totalizer
# }



# {"TYPE":"N",
# "UID":"ABCDE01001",
# "DT":"04/08/24",
# "TM":"15:06",
# "TW":17,
# "C1N":"CH1",
# "C1U":"NA",
# "C1":0,
# "T1":0,
# "T1U":"NA",
# "C2N":"CH2",
# "C2U":"NA",
# "C2":0,
# "T2":0,
# "T2U":"NA",
# "C3N":"CH3",
# "C3U":"NA",
# "C3":0,
# "T3":0,
# "T3U":"NA",
# "C4N":"CH4",
# "C4U":"NA",
# "C4":0,
# "T4":0,
# "T4U":"NA",
# "C5N":"CH5",
# "C5U":"NA",
# "C5":0,
# "T5":0,
# "T5U":"NA",
# "C6N":"CH6",
# "C6U":"NA",
# "C6":0,
# "T6":0,
# "T6U":"NA",
# "DI1N":"D0",
# "DI1U":"Unit1",
# "DI1":0,
# "PULSE1":0,
# "DI2N":"D1",
# "DI2U":"Unit2",
# "DI2":0,
# "PULSE2":0}
    
    
    


class DeviceAutoRegister(BaseModel):
    # client_id: int
    ib_id: int
    # device_id: int
    do_channel:int
    model:str
    lat:str
    lon:str
    imei_no:str


class CheckedDevices(BaseModel):
    device:str
    
class WeatherFlowData(BaseModel):
    client_id: int
    device_id: int
    device: str
    start_date: date
    end_date: date
    
class ChartData(BaseModel):
    client_id: int
    device_id: int
    device: str
    
    
class TemperatureUsed(BaseModel):
    device_id: int
    device: str
    # type: str
    start_date: date
    # end_date: date
    # start_date_time: datetime = Field(..., alias="start_date_time", description="Format: '%Y-%m-%d %H:%M:%S'")
    # start_date_time: datetime
    # end_date_time: datetime
    
    
class VoltageData(BaseModel):
    client_id: int
    device_id: int
    device: str
    start_date_time: datetime
    end_date_time: datetime
    
class WsEnergyData(BaseModel):
    client_id: int
    device_id: int
    device: str
    
    
    
# ==========================================
# ==========================================



class UpsDeviceData(BaseModel):
    client_id: int
    device_id: int
    device: str
    do_channel: int
    device_location: str
    device_output_current: float
    device_input_current: float
    
    
    
# ========================================

class AddAlert(BaseModel):
    client_id: int
    organization_id: int
    device_id: int
    device: str
    unit_id: int
    alert_type: str
    alert_status: str
    alert_status: str
    alert_value: float
    alert_email : str
    create_by: int
    @validator('alert_type')
    def validate_alert_type(cls, v):
        valid_alert_types = {"3H", "2L", "1CL", "4CH"}
        if v not in valid_alert_types:
            raise ValueError('Invalid alert type')
        return v
    @validator('alert_status')
    def validate_alert_status(cls, v):
        valid_alert_status = {"Y", "N"}
        if v not in valid_alert_status:
            raise ValueError('Invalid alert status')
        return v
    @validator('alert_email')
    def validate_email(cls, alert_email):
        # Regular expression for basic email validation
        pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
        if not re.match(pattern, alert_email):
            raise ValueError("Invalid email address")
        return alert_email
    
class EditAlert(BaseModel):
    alert_id: int
    client_id: int
    organization_id: int
    device_id: int
    device: str
    unit_id: int
    alert_type: str
    alert_status: str
    alert_status: str
    alert_value: float
    alert_email : str
    create_by: int
    @validator('alert_type')
    def validate_alert_type(cls, v):
        valid_alert_types = {"3H", "2L", "1CL", "4CH"}
        if v not in valid_alert_types:
            raise ValueError('Invalid alert type')
        return v
    @validator('alert_status')
    def validate_alert_status(cls, v):
        valid_alert_status = {"Y", "N"}
        if v not in valid_alert_status:
            raise ValueError('Invalid alert status')
        return v
    @validator('alert_email')
    def validate_email(cls, alert_email):
        # Regular expression for basic email validation
        pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
        if not re.match(pattern, alert_email):
            raise ValueError("Invalid email address")
        return alert_email

class DeleteAlert(BaseModel):
    alert_id: int
    client_id: int
    organization_id: int
    device_id: int
    
    
class DeviceAdd(BaseModel):
    client_id: int
    device: str
    device_name: str
    do_channel: int
    model: str
    lat: str
    lon: str
    imei_no: str
    # device_type: str
    # meter_type: str
    last_maintenance: date
    # @validator('meter_type')
    # def validate_meter_type(cls, v):
    #     validate_meter_type = {"ENSF", "ENTF"}
    #     if v not in validate_meter_type:
    #         raise ValueError('Invalid alert status')
    #     return v
    # @validator('device_type')
    # def validate_device_type(cls, v):
    #     valid_device_type = {"EN", "UPS"}
    #     if v not in valid_device_type:
    #         raise ValueError('Invalid alert status')
    #     return v

class DeviceEdit(BaseModel):
    device_id:int
    client_id: int
    device: str
    device_name: str
    do_channel: int
    model: str
    lat: str
    lon: str
    imei_no: str
    # device_type: str
    # meter_type: str
    # @validator('meter_type')
    # def validate_meter_type(cls, v):
    #     valid_meter_type = {"ENSF", "ENTF"}
    #     if v not in valid_meter_type:
    #         raise ValueError('Invalid alert status')
    #     return v
    # @validator('device_type')
    # def validate_device_type(cls, v):
    #     valid_device_type = {"EN", "UPS"}
    #     if v not in valid_device_type:
    #         raise ValueError('Invalid alert status')
    #     return v
    
class UserDeviceList(BaseModel):
    client_id: int
    device_id: int
    device: str
    user_id: int
    organization_id:int



class WsDeviceData(BaseModel):
    client_id: int
    device_id: int
    device: str
    
class WsDeviceOrgData(BaseModel):
    organization_id: int
    
    
class WsDeviceOrgData2(BaseModel):
    organization_id: int
    project_id: int


# class BllingData(BaseModel):
#     billing_type: str
#     billing_price: float
#     billing_status: str
#     billing_day: int

class OrganizationSettings(BaseModel):
    organization_id: int
    client_id: int
    countries_id: int
    states_id: int
    regions_id: int
    subregions_id: int
    cities_id: int
    address: str
    created_by: int
    # billing_data: List[BllingData]
    
class OrganizationSettingsList(BaseModel):
    organization_id: int
    
# class AddBill(BaseModel):
#     organization_id: int
#     billing_type: str
#     billing_price: float
#     billing_day: int
    
class EditOrganization(BaseModel):
    organization_id: int
    countries_id: int
    states_id: int
    regions_id: int
    subregions_id: int
    cities_id: int
    address: str


class UserData(BaseModel):
    name: str
    # email: EmailStr
    age: Optional[int] = None