from db_model.MASTER_MODEL import select_data, insert_data, update_data, delete_data
from utils.date_time_format import get_current_datetime

def add_branch(branch):
    try:
        current_datetime = get_current_datetime()
        columns = "client_id, organization_id, project_id, branch_name, created_at"
        value = f"{branch.client_id}, {branch.organization_id}, {branch.project_id}, '{branch.branch_name}', '{current_datetime}'"
        branch_id = insert_data("manage_branch", columns, value)
        if branch_id is None:
            raise ValueError("Branch creation failed")
        return {"branch_id": branch_id, "branch_name": branch.branch_name}
    except Exception as e:
        raise e

def list_branch(params):
    try:
        select = "b.branch_id, b.client_id, b.organization_id, b.project_id, b.branch_name, DATE_FORMAT(b.created_at, '%Y-%m-%d %H:%i:%s') AS created_at, o.organization_name, p.project_name, (SELECT COUNT(*) FROM manage_branch_device bd WHERE bd.branch_id = b.branch_id) AS device_count"
        table = "manage_branch as b LEFT JOIN md_organization as o ON b.organization_id = o.organization_id LEFT JOIN md_project as p ON b.project_id = p.project_id"
        
        condition = f"b.client_id = {params.client_id}"
        if hasattr(params, 'organization_id') and params.organization_id:
            condition += f" AND b.organization_id = {params.organization_id}"
        if hasattr(params, 'project_id') and params.project_id:
            condition += f" AND b.project_id = {params.project_id}"
            
        data = select_data(table, select, condition)
        return data
    except Exception as e:
        raise e

def edit_branch(branch):
    try:
        condition = f"branch_id = {branch.branch_id} AND client_id = {branch.client_id}"
        columns = {
            "organization_id": branch.organization_id,
            "project_id": branch.project_id,
            "branch_name": branch.branch_name
        }
        data = update_data("manage_branch", columns, condition)
        return {"success": True}
    except Exception as e:
        raise e

def delete_branch(branch):
    try:
        # First delete mapped branch devices
        device_condition = f"branch_id = {branch.branch_id}"
        delete_data("manage_branch_device", device_condition)
        
        # Then delete the branch
        condition = f"branch_id = {branch.branch_id}"
        data = delete_data("manage_branch", condition)
        return {"success": bool(data)}
    except Exception as e:
        raise e


# Branch Device Assignment
def add_branch_device(params):
    try:
        current_datetime = get_current_datetime()
        # Find device details to insert the device UID
        from db_model.MASTER_MODEL import select_one_data
        
        inserted_count = 0
        for device_id in params.device_ids:
            device_info = select_one_data("md_device", "device", f"device_id = {device_id} AND client_id = {params.client_id}")
            if device_info:
                device_uid = device_info['device']
                
                # Check if already assigned to avoid duplicates
                check_existing = select_data("manage_branch_device", "branch_device_id", f"branch_id = {params.branch_id} AND device_id = {device_id}")
                
                if not check_existing:
                    columns = "client_id, branch_id, device_id, device, created_at"
                    value = f"{params.client_id}, {params.branch_id}, {device_id}, '{device_uid}', '{current_datetime}'"
                    insert_data("manage_branch_device", columns, value)
                    inserted_count += 1
                
        return {"inserted_count": inserted_count}
    except Exception as e:
        raise e

def list_branch_device(params):
    try:
        select = "bd.branch_device_id, bd.branch_id, bd.device_id, bd.device, DATE_FORMAT(bd.created_at, '%Y-%m-%d %H:%i:%s') AS created_at, d.device_name, d.model, 'online' AS status"
        table = "manage_branch_device as bd LEFT JOIN md_device as d ON bd.device_id = d.device_id"
        condition = f"bd.branch_id = {params.branch_id} AND bd.client_id = {params.client_id}"
        data = select_data(table, select, condition)
        return data
    except Exception as e:
        raise e

def delete_branch_device(params):
    try:
        condition = f"branch_device_id = {params.branch_device_id} AND branch_id = {params.branch_id}"
        data = delete_data("manage_branch_device", condition)
        return {"success": bool(data)}
    except Exception as e:
        raise e


# Branch Config - Full control panel data
def get_branch_config(params):
    """Returns branch info + all devices with their valve scheduling states"""
    try:
        from db_model.MASTER_MODEL import select_one_data, custom_select_sql_query

        # 1) Get branch info
        branch_select = "b.branch_id, b.branch_name, b.organization_id, b.project_id, o.organization_name, p.project_name"
        branch_table = "manage_branch as b LEFT JOIN md_organization as o ON b.organization_id = o.organization_id LEFT JOIN md_project as p ON b.project_id = p.project_id"
        branch_condition = f"b.branch_id = {params.branch_id} AND b.client_id = {params.client_id}"
        branch_rows = select_data(branch_table, branch_select, branch_condition)
        if not branch_rows:
            raise ValueError("Branch not found")
        branch_info = branch_rows[0]

        # 2) Get all devices in this branch
        dev_select = "bd.branch_device_id, bd.device_id, bd.device, d.device_name, d.model, d.device_status"
        dev_table = "manage_branch_device as bd LEFT JOIN md_device as d ON bd.device_id = d.device_id"
        dev_condition = f"bd.branch_id = {params.branch_id} AND bd.client_id = {params.client_id}"
        devices = select_data(dev_table, dev_select, dev_condition)
        if devices is None:
            devices = []

        # 3) For each device, get valve scheduling for valves 1-6
        active_device_count = 0
        active_valve_count = 0
        device_list = []

        for dev in devices:
            device_uid = dev.get('device', '')
            device_id = dev.get('device_id', 0)
            is_online = str(dev.get('device_status', '')).upper() == 'ONLINE'
            if is_online:
                active_device_count += 1

            valves = {}
            for valve_no in range(1, 7):
                schedule_sql = f"""
                    SELECT schedule_id, do_type, do_no,
                           one_on_time, one_off_time,
                           two_on_time, two_off_time,
                           days, datalog_sec
                    FROM device_schedule
                    WHERE device = '{device_uid}'
                      AND do_no = {valve_no}
                      AND client_id = {params.client_id}
                    ORDER BY schedule_id DESC
                    LIMIT 1
                """
                try:
                    sched_rows = custom_select_sql_query(schedule_sql, None)
                    if sched_rows and len(sched_rows) > 0:
                        sched = sched_rows[0]
                        valve_active = sched.get('do_type', 0) is not None
                        if valve_active:
                            active_valve_count += 1
                        valves[f"valve_{valve_no}"] = {
                            "do_type": sched.get('do_type'),
                            "one_on_time": str(sched.get('one_on_time', '00:00:00')),
                            "one_off_time": str(sched.get('one_off_time', '00:00:00')),
                            "two_on_time": str(sched.get('two_on_time', '00:00:00')),
                            "two_off_time": str(sched.get('two_off_time', '00:00:00')),
                            "days": sched.get('days', ''),
                            "has_schedule": True
                        }
                    else:
                        valves[f"valve_{valve_no}"] = {"has_schedule": False, "do_type": None}
                except Exception:
                    valves[f"valve_{valve_no}"] = {"has_schedule": False, "do_type": None}

            device_list.append({
                "branch_device_id": dev.get('branch_device_id'),
                "device_id": device_id,
                "device": device_uid,
                "device_name": dev.get('device_name', device_uid),
                "model": dev.get('model', ''),
                "status": 'online' if is_online else 'offline',
                "valves": valves
            })

        return {
            "branch": branch_info,
            "summary": {
                "total_devices": len(device_list),
                "active_devices": active_device_count,
                "active_valves": active_valve_count,
                "total_valves": len(device_list) * 6
            },
            "devices": device_list
        }
    except Exception as e:
        raise e


# ─── Helper: get all devices in a branch ───
def _get_branch_devices(branch_id, client_id):
    """Returns list of {device_id, device, gateway_id} for all devices in a branch"""
    from db_model.MASTER_MODEL import select_one_data
    dev_select = "bd.device_id, bd.device"
    dev_table = "manage_branch_device as bd"
    dev_condition = f"bd.branch_id = {branch_id} AND bd.client_id = {client_id}"
    devices = select_data(dev_table, dev_select, dev_condition)
    if devices is None:
        devices = []

    result = []
    for dev in devices:
        # Get gateway_id for each device
        try:
            gw = select_one_data("md_device", "gateway_id", f"device_id = {dev['device_id']} AND client_id = {client_id}", order_by="device_id DESC")
            gateway_id = gw['gateway_id'] if gw else None
        except Exception:
            gateway_id = None

        result.append({
            "device_id": dev['device_id'],
            "device": dev['device'],
            "gateway_id": gateway_id
        })
    return result


# ─── Branch-Level Switch (all devices) ───
def switch_branch_all(params):
    """Send valve ON/OFF to ALL devices in the branch"""
    try:
        devices = _get_branch_devices(params.branch_id, params.client_id)
        if not devices:
            raise ValueError("No devices found in this branch")

        return {
            "devices": devices,
            "do_no": params.do_no,
            "value": params.value,
            "device_count": len(devices)
        }
    except Exception as e:
        raise e


# ─── Branch-Level Schedule Save (all devices) ───
def schedule_branch_all(params):
    """Save schedule to ALL devices in the branch"""
    try:
        from db_model.MASTER_MODEL import select_one_data
        devices = _get_branch_devices(params.branch_id, params.client_id)
        if not devices:
            raise ValueError("No devices found in this branch")

        return {
            "devices": devices,
            "do_type": params.do_type,
            "do_no": params.do_no,
            "one_on_time": params.one_on_time,
            "one_off_time": params.one_off_time,
            "two_on_time": params.two_on_time,
            "two_off_time": params.two_off_time,
            "datalog_sec": params.datalog_sec,
            "days": params.days,
            "device_count": len(devices)
        }
    except Exception as e:
        raise e


# ─── Branch-Level Schedule Reset (all devices) ───
def reset_branch_schedule_all(params):
    """Reset schedule for a valve on ALL devices in the branch"""
    try:
        devices = _get_branch_devices(params.branch_id, params.client_id)
        if not devices:
            raise ValueError("No devices found in this branch")

        return {
            "devices": devices,
            "do_no": params.do_no,
            "device_count": len(devices)
        }
    except Exception as e:
        raise e

