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
        data = ManageBranchController.add_branch(branch)
        resdata = successResponse(data, message="Branch added successfully")
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

    try:
        from routes.mqtt_routes import mqtt_client, encode_gc_frame

        result = ManageBranchController.switch_branch_all(params)
        branch_number = result['branch_number']
        gateway_ids = result['gateway_ids']

        do_states = [0] * 8
        do_states[params.do_no - 1] = params.value

        # Encode one GC frame using branch_number as the group ID
        frame = encode_gc_frame(branch_number, do_states)

        success_count = 0
        errors = []

        # Send same frame to each unique gateway (no per-device loop)
        for gw_id in gateway_ids:
            try:
                mqtt_client.publish(f"/ST/{gw_id}", frame, qos=1)
                success_count += 1
            except Exception as ex:
                errors.append({"gateway_id": gw_id, "error": str(ex)})

        resdata = successResponse({
            "success_count": success_count,
            "total_gateways": len(gateway_ids),
            "branch_number": branch_number,
            "command": frame,
            "errors": errors
        }, message=f"Valve {params.do_no} {'ON' if params.value else 'OFF'} sent to {success_count}/{len(gateway_ids)} gateways")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- Branch-Level Schedule Save ALL Devices ---

@manage_branch_routes.post("/manage_branch/schedule_save_all", dependencies=[Depends(mw_user_client)])
async def schedule_save_all(request: Request, params: BranchScheduleSaveAll):
    try:
        from routes.mqtt_routes import mqtt_client, days_to_mask

        user_data = request.state.user_data

        # 1) Get branch info + unique gateways
        result = ManageBranchController.schedule_branch_all(params)
        branch_number = ManageBranchController._get_branch_number(params.branch_id, params.client_id)

        # 2) Parse time
        on_parts = params.one_on_time.split(':')
        off_parts = params.one_off_time.split(':')
        one_on_hr = int(on_parts[0])
        one_on_min = int(on_parts[1])
        one_off_hr = int(off_parts[0])
        one_off_min = int(off_parts[1])

        # 3) Build GC hex payload (exact format)
        # ─── Group ID: 5-char ASCII from branch_number ───
        group_id = str(branch_number)[-5:] if len(str(branch_number)) >= 5 else str(branch_number).rjust(5, '0')
        group_id_hex = ''.join(f"{ord(c):02X}" for c in group_id)

        # ─── Byte 5: DO Type + Channel ───
        channel = (params.do_no - 1) & 0x0F
        do_type_mapped = 4 if params.do_type == 0 else 5
        byte5 = ((do_type_mapped & 0x0F) << 4) | (channel & 0x0F)

        # ─── Byte 6: Enable (bit7) + Slot (bit0-1) ───
        slot = params.slot if params.slot is not None else 0
        enable = params.status if params.status is not None else 1
        byte6 = ((1 if enable else 0) << 7) | (slot & 0x03)

        # ─── Time HEX ───
        on_hr_hex = f"{one_on_hr:02X}"
        on_min_hex = f"{one_on_min:02X}"
        off_hr_hex = f"{one_off_hr:02X}"
        off_min_hex = f"{one_off_min:02X}"

        # ─── Days mask ───
        days_mask = days_to_mask(params.days or "sun,mon,tue,wed,thu,fri,sat")
        days_hex = f"{days_mask:02X}"

        # ─── Final payload (12 bytes) ───
        hex_payload = (
            f"{group_id_hex}"     # 5 bytes (group ID)
            f"{byte5:02X}"        # 1 byte  (DO Type + Channel)
            f"{byte6:02X}"        # 1 byte  (Enable + Slot)
            f"{on_hr_hex}"        # 1 byte
            f"{on_min_hex}"       # 1 byte
            f"{off_hr_hex}"       # 1 byte
            f"{off_min_hex}"      # 1 byte
            f"{days_hex}"         # 1 byte
        )

        gc_command = f"*GC,{hex_payload}#"

        # 4) Get unique gateways and send GC command once per gateway
        from db_model.MASTER_MODEL import custom_select_sql_query
        gw_sql = f"""SELECT DISTINCT gateway_id 
                     FROM md_device 
                     WHERE branch_number = '{branch_number}' 
                       AND client_id = {params.client_id}
                       AND gateway_id IS NOT NULL 
                       AND gateway_id != ''"""
        gateways = custom_select_sql_query(gw_sql, 1)

        success_count = 0
        errors = []

        if gateways:
            for gw in gateways:
                try:
                    mqtt_client.publish(f"/ST/{gw['gateway_id']}", gc_command, qos=1)
                    success_count += 1
                except Exception as ex:
                    errors.append({"gateway_id": gw['gateway_id'], "error": str(ex)})

        # 5) Upsert device_group_schedule (branch-level only, no per-device loop)
        try:
            ManageBranchController.upsert_group_schedule(params, user_data['user_id'])
        except Exception as gse:
            print("Error upserting group schedule:", gse)

        resdata = successResponse({
            "success_count": success_count,
            "total_gateways": len(gateways) if gateways else 0,
            "branch_number": branch_number,
            "command": gc_command,
            "errors": errors
        }, message=f"Schedule sent to {success_count} gateway(s)")
        return Response(content=json.dumps(resdata), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- Branch-Level Schedule Reset ALL Devices ---

@manage_branch_routes.post("/manage_branch/schedule_reset_all", dependencies=[Depends(mw_user_client)])
async def schedule_reset_all(request: Request, params: BranchScheduleResetAll):
    """Reset schedule for a valve on ALL devices in the branch.
       - Deletes device_group_schedule record
       - Deletes device_schedule records for each device+valve
       - Sends reset MQTT command to each device
    """
    try:
        from routes.mqtt_routes import mqtt_client
        from db_model.MASTER_MODEL import delete_data as db_delete_data

        result = ManageBranchController.reset_branch_schedule_all(params)
        devices = result['devices']

        success_count = 0
        errors = []

        for dev in devices:
            try:
                # Delete device_schedule for this device + valve
                try:
                    del_condi = f"device = '{dev['device']}' AND do_no = {params.do_no} AND client_id = {params.client_id}"
                    db_delete_data("device_schedule", del_condi)
                except Exception as del_ex:
                    print(f"Error deleting schedule for device {dev['device']}: {del_ex}")

                # Send MQTT reset command
                if dev['gateway_id']:
                    # Send per-device reset
                    device_id_int = int(dev['device']) if dev['device'].isdigit() else 0
                    rxUID_hex = f"{device_id_int:04X}"
                    channel = params.do_no - 1
                    ch_hex = f"{channel:02X}"
                    reset_cmd = f"*TORST,{rxUID_hex}{ch_hex}#"
                    mqtt_client.publish(f"/ST/{dev['gateway_id']}", reset_cmd, qos=1)
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

