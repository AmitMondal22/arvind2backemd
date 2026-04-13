from fastapi import APIRouter, HTTPException, Response, Depends, Request
import json
from controllers.admin import ManageBranchController
from models.manage_branch_model import (
    AddBranch, EditBranch, DeleteBranch, ListBranch, AvailableBranchNumbers,
    BranchConfigGet, BranchSwitchAll, BranchScheduleSaveAll, BranchScheduleResetAll
)
from middleware.MyMiddleware import mw_client, mw_user_client
from utils.response import successResponse
from Library.DecimalEncoder import DecimalEncoder

manage_branch_routes = APIRouter()

# --- Available Branch Numbers (from md_device) ---

@manage_branch_routes.post("/manage_branch/available_branch_numbers", dependencies=[Depends(mw_user_client)])
async def available_branch_numbers(request: Request, params: AvailableBranchNumbers):
    try:
        data = ManageBranchController.get_available_branch_numbers(params)
        resdata = successResponse(data, message="Available branch numbers")
        return Response(content=json.dumps(resdata, cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")

# --- Branch CRUD ---

@manage_branch_routes.post("/manage_branch/add", dependencies=[Depends(mw_client)])
async def add_branch(request: Request, branch: AddBranch):
    try:
        data = ManageBranchController.edit_branch(branch)
        resdata = successResponse(data, message="Branch updated successfully")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")

@manage_branch_routes.post("/manage_branch/list", dependencies=[Depends(mw_user_client)])
async def list_branch(request: Request, params: ListBranch):
    try:
        data = ManageBranchController.list_branch(params)
        resdata = successResponse(data, message="List of branches")
        return Response(content=json.dumps(resdata, cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")

@manage_branch_routes.post("/manage_branch/edit", dependencies=[Depends(mw_client)])
async def edit_branch(request: Request, branch: EditBranch):
    try:
        data = ManageBranchController.edit_branch(branch)
        resdata = successResponse(data, message="Branch edited successfully")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")

@manage_branch_routes.post("/manage_branch/delete", dependencies=[Depends(mw_client)])
async def delete_branch(request: Request, branch: DeleteBranch):
    try:
        data = ManageBranchController.delete_branch(branch)
        resdata = successResponse(data, message="Branch deleted successfully")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")


# --- Branch Config (Full Control Panel) ---

@manage_branch_routes.post("/manage_branch/get_config", dependencies=[Depends(mw_user_client)])
async def get_branch_config(request: Request, params: BranchConfigGet):
    try:
        data = ManageBranchController.get_branch_config(params)
        resdata = successResponse(data, message="Branch config loaded")
        return Response(content=json.dumps(resdata, cls=DecimalEncoder, default=str), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")


# --- Branch-Level Switch ALL Devices ---

@manage_branch_routes.post("/manage_branch/switch_all", dependencies=[Depends(mw_user_client)])
async def switch_branch_all(request: Request, params: BranchSwitchAll):
    """Toggle a valve ON/OFF for ALL devices in the branch via MQTT"""
    try:
        from routes.mqtt_routes import mqtt_client, encode_do_to_frame

        result = ManageBranchController.switch_branch_all(params)
        devices = result['devices']

        success_count = 0
        errors = []
        for dev in devices:
            try:
                if dev['gateway_id']:
                    do_states = [0] * 7
                    do_states[params.do_no - 1] = params.value
                    frame = encode_do_to_frame(dev['device'], do_states)
                    mqtt_client.publish(f"/ST/{dev['gateway_id']}", frame, qos=0)
                    success_count += 1
            except Exception as ex:
                errors.append({"device": dev['device'], "error": str(ex)})

        resdata = successResponse({
            "success_count": success_count,
            "total_devices": len(devices),
            "errors": errors
        }, message=f"Valve {params.do_no} {'ON' if params.value else 'OFF'} sent to {success_count}/{len(devices)} devices")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- Branch-Level Schedule Save ALL Devices ---

@manage_branch_routes.post("/manage_branch/schedule_save_all", dependencies=[Depends(mw_user_client)])
async def schedule_save_all(request: Request, params: BranchScheduleSaveAll):
    """Save schedule for a valve on ALL devices in the branch"""
    try:
        from routes.mqtt_routes import mqtt_client, days_to_mask, insert_updatesheduling
        from db_model.MASTER_MODEL import select_one_data
        from datetime import time as dt_time

        user_data = request.state.user_data
        result = ManageBranchController.schedule_branch_all(params)
        devices = result['devices']

        on_parts = params.one_on_time.split(':')
        off_parts = params.one_off_time.split(':')
        one_on_hr = int(on_parts[0])
        one_on_min = int(on_parts[1])
        one_off_hr = int(off_parts[0])
        one_off_min = int(off_parts[1])

        success_count = 0
        errors = []

        for dev in devices:
            try:
                device_uid = dev['device']
                device_id = dev['device_id']

                device_id_int = int(device_uid)
                rxUID_hex = f"{device_id_int:04X}"
                channel = params.do_no - 1
                do_type_mapped = 4 if params.do_type == 0 else 5

                byte2 = ((do_type_mapped & 0x0F) << 4) | (channel & 0x0F)
                on_hr_hex = f"{one_on_hr:02X}"
                on_min_hex = f"{one_on_min:02X}"
                off_hr_hex = f"{one_off_hr:02X}"
                off_min_hex = f"{one_off_min:02X}"

                days_mask = days_to_mask(params.days or "sun,mon,tue,wed,thu,fri,sat")
                days_hex = f"{days_mask:02X}"

                hex_payload = f"{rxUID_hex}{byte2:02X}{on_hr_hex}{on_min_hex}{off_hr_hex}{off_min_hex}{days_hex}"
                pubdata = f"*LC,{hex_payload}#"

                if dev['gateway_id']:
                    mqtt_client.publish(f"/ST/{dev['gateway_id']}", pubdata, qos=1)

                class ScheduleData:
                    pass
                sd = ScheduleData()
                sd.device = device_uid
                sd.device_id = device_id
                sd.do_type = params.do_type
                sd.do_no = params.do_no
                sd.one_on_time = dt_time(one_on_hr, one_on_min, 0)
                sd.one_off_time = dt_time(one_off_hr, one_off_min, 0)
                sd.two_on_time = dt_time(0, 0, 0)
                sd.two_off_time = dt_time(0, 0, 0)
                sd.datalog_sec = params.datalog_sec or 120
                sd.days = params.days or "sun,mon,tue,wed,thu,fri,sat"

                await insert_updatesheduling(user_data, sd)
                success_count += 1

            except Exception as ex:
                errors.append({"device": dev['device'], "error": str(ex)})

        resdata = successResponse({
            "success_count": success_count,
            "total_devices": len(devices),
            "errors": errors
        }, message=f"Schedule saved to {success_count}/{len(devices)} devices")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- Branch-Level Schedule Reset ALL Devices ---

@manage_branch_routes.post("/manage_branch/schedule_reset_all", dependencies=[Depends(mw_user_client)])
async def schedule_reset_all(request: Request, params: BranchScheduleResetAll):
    """Reset schedule for a valve on ALL devices in the branch"""
    try:
        from routes.mqtt_routes import mqtt_client

        result = ManageBranchController.reset_branch_schedule_all(params)
        devices = result['devices']

        success_count = 0
        errors = []

        for dev in devices:
            try:
                if dev['gateway_id']:
                    pubdata = f"*TORST,{dev['device']}#"
                    mqtt_client.publish(f"/WFMS/{dev['device']}", pubdata, qos=0)
                    success_count += 1
            except Exception as ex:
                errors.append({"device": dev['device'], "error": str(ex)})

        resdata = successResponse({
            "success_count": success_count,
            "total_devices": len(devices),
            "errors": errors
        }, message=f"Schedule reset for {success_count}/{len(devices)} devices")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
