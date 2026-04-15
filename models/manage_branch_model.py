from pydantic import BaseModel, conint
from typing import Optional, List

# Branch Models
class AddBranch(BaseModel):
    client_id: int
    organization_id: int
    project_id: int
    branch_name: str
    branch_number: str

class EditBranch(BaseModel):
    client_id: int
    branch_id: int
    branch_number: str
    organization_id: int
    project_id: int
    branch_name: str

class DeleteBranch(BaseModel):
    branch_id: int

class ListBranch(BaseModel):
    client_id: int
    organization_id: Optional[int] = None
    project_id: Optional[int] = None

class AvailableBranchNumbers(BaseModel):
    client_id: int

class BranchConfigGet(BaseModel):
    client_id: int
    branch_id: int

# Branch-Level Switch (applies to ALL devices in branch)
class BranchSwitchAll(BaseModel):
    client_id: int
    branch_id: int
    do_no: int
    value: int     # 1=ON, 0=OFF

# Branch-Level Schedule Save (applies to ALL devices in branch)
class BranchScheduleSaveAll(BaseModel):
    client_id: int
    branch_id: int
    organization_id: Optional[int] = None
    do_type: int        # 0=Auto, 1=Manual
    do_no: int          # valve number 1-6
    one_on_time: str    # "HH:mm:ss"
    one_off_time: str   # "HH:mm:ss"
    two_on_time: Optional[str] = "00:00:00"
    two_off_time: Optional[str] = "00:00:00"
    datalog_sec: Optional[int] = 120
    days: Optional[str] = "sun,mon,tue,wed,thu,fri,sat"
    slot: Optional[int] = 0       # 0, 1, 2
    status: Optional[int] = 1     # 1=enabled, 0=disabled

# Branch-Level Schedule Reset (applies to ALL devices in branch)
class BranchScheduleResetAll(BaseModel):
    client_id: int
    branch_id: int
    do_no: int
