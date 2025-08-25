from pydantic import BaseModel, Field, constr, validator
from enum import Enum
from typing import Optional,List

class YNEnum(str, Enum):
    Y = 'Y'
    N = 'N'


class ClientScreenSettings(BaseModel):
    organization_id: int
    
class ClientScreenSettingsEdit(BaseModel):
    id_view_organization: Optional[int] = None
    user_type : str
    organization_id : int  
    gv_energy_used : YNEnum 
    gv_voltage :  YNEnum 
    gv_current :  YNEnum
    gv_power :  YNEnum
    mn_add_organization : YNEnum  
    mn_device_management :  YNEnum
    mn_user_management :  YNEnum
    en_tab_device_info :  YNEnum
    en_tab_create_alert :  YNEnum
    en_tab_scheduling :  YNEnum
    en_tab_report_analysi : YNEnum
    