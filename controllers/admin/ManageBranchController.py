from db_model.MASTER_MODEL import select_data, insert_data, update_data, delete_data, select_one_data, custom_select_sql_query
from utils.date_time_format import get_current_datetime

def get_available_branch_numbers(params):
    """Get unique branch_number values from md_device that are not yet assigned to a branch"""
    try:
        sql = f"""
            SELECT DISTINCT d.branch_number 
            FROM md_device d 
            WHERE d.client_id = {params.client_id} 
              AND d.branch_number IS NOT NULL 
              AND d.branch_number != '' 
              AND d.branch_number NOT IN (
                  SELECT b.branch_number FROM manage_branch b 
                  WHERE b.client_id = {params.client_id} 
                    AND b.branch_number IS NOT NULL 
                    AND b.branch_number != ''
              )
            ORDER BY d.branch_number
        """
        data = custom_select_sql_query(sql, 1)
        return data if data else []
    except Exception as e:
        raise e

def add_branch(branch):
    
    try:
        column = "organization_id, project_id, branch_name, branch_number"
        row_data = f"{branch.organization_id}, {branch.project_id}, '{branch.branch_name}', '{branch.branch_number}'"
        data = insert_data("manage_branch", column ,row_data)
        return {"success": True, "branch_name": branch.branch_name, "branch_number": branch.branch_number}
    except Exception as e:
        raise e

def list_branch(params):
    """List branches with device count and device details from md_device via branch_number"""
    try:
        select = """b.branch_id, b.client_id, b.organization_id, b.project_id, 
                    b.branch_name, b.branch_number,
                    DATE_FORMAT(b.created_at, '%%Y-%%m-%%d %%H:%%i:%%s') AS created_at, 
                    o.organization_name, p.project_name,
                    (SELECT COUNT(*) FROM md_device d WHERE d.branch_number = b.branch_number AND d.client_id = b.client_id) AS device_count"""
        table = "manage_branch as b LEFT JOIN md_organization as o ON b.organization_id = o.organization_id LEFT JOIN md_project as p ON b.project_id = p.project_id"
        
        condition = f"b.client_id = {params.client_id}"
        if hasattr(params, 'organization_id') and params.organization_id:
            condition += f" AND b.organization_id = {params.organization_id}"
        if hasattr(params, 'project_id') and params.project_id:
            condition += f" AND b.project_id = {params.project_id}"
            
        data = select_data(table, select, condition)
        
        if data:
            for branch in data:
                branch_number = branch.get('branch_number')
                if branch_number:
                    dev_sql = f"""SELECT device_id, device, device_name, device_type, device_status, model, 
                                        branch_number, gateway_id, lat, lon, imei_no
                                 FROM md_device 
                                 WHERE branch_number = '{branch_number}' 
                                   AND client_id = {params.client_id}"""
                    try:
                        devices = custom_select_sql_query(dev_sql, 1)
                        branch['devices'] = devices if devices else []
                    except Exception:
                        branch['devices'] = []
                else:
                    branch['devices'] = []
        return data
    except Exception as e:
        raise e

def edit_branch(branch):
    """Edit branch - uses branch_number in WHERE clause, branch_number itself is NOT editable"""
    try:
        condition = f"branch_number = '{branch.branch_number}'"
        columns = {
            "organization_id": branch.organization_id,
            "project_id": branch.project_id,
            "branch_name": branch.branch_name
        }
        data = update_data("manage_branch", columns, condition)
        print("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBranch update result:", data)
        return {"success": True}
    except Exception as e:
        raise e

def delete_branch(branch):
    try:
        condition = f"branch_id = {branch.branch_id}"
        data = delete_data("manage_branch", condition)
        return {"success": bool(data)}
    except Exception as e:
        raise e

# Branch Config - Full control panel data
def get_branch_config(params):
    """Returns branch info + all devices with their valve scheduling states"""
    try:
        # 1) Get branch info
        branch_select = "b.branch_id, b.branch_name, b.branch_number, b.organization_id, b.project_id, o.organization_name, p.project_name"
        branch_table = "manage_branch as b LEFT JOIN md_organization as o ON b.organization_id = o.organization_id LEFT JOIN md_project as p ON b.project_id = p.project_id"
        branch_condition = f"b.branch_id = {params.branch_id} AND b.client_id = {params.client_id}"
        branch_rows = select_data(branch_table, branch_select, branch_condition)
        if not branch_rows:
            raise ValueError("Branch not found")
        branch_info = branch_rows[0]

        branch_number = branch_info.get('branch_number', '')
        if branch_number:
            dev_sql = f"""SELECT device_id, device, device_name, model, device_status, device_type, gateway_id, lat, lon, imei_no
                         FROM md_device 
                         WHERE branch_number = '{branch_number}' 
                           AND client_id = {params.client_id}"""
            devices = custom_select_sql_query(dev_sql, 1)
            if devices is None:
                devices = []
        else:
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
                # 1) Get schedule info
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
                # 2) Get current valve ON/OFF status from td_water_data
                status_sql = f"""
                    SELECT do_status 
                    FROM td_water_data 
                    WHERE device = '{device_uid}' 
                      AND do_no = {valve_no}
                      AND client_id = {params.client_id}
                    ORDER BY water_data_id DESC
                    LIMIT 1
                """

                valve_data = {"has_schedule": False, "do_type": None, "do_status": 0}

                try:
                    sched_rows = custom_select_sql_query(schedule_sql, None)
                    if sched_rows and len(sched_rows) > 0:
                        sched = sched_rows[0]
                        valve_active = sched.get('do_type', 0) is not None
                        if valve_active:
                            active_valve_count += 1
                        
                        valve_data.update({
                            "do_type": sched.get('do_type'),
                            "one_on_time": str(sched.get('one_on_time', '00:00:00')),
                            "one_off_time": str(sched.get('one_off_time', '00:00:00')),
                            "two_on_time": str(sched.get('two_on_time', '00:00:00')),
                            "two_off_time": str(sched.get('two_off_time', '00:00:00')),
                            "days": sched.get('days', ''),
                            "has_schedule": True
                        })
                except Exception:
                    pass

                try:
                    status_rows = custom_select_sql_query(status_sql, None)
                    if status_rows and len(status_rows) > 0:
                        valve_data["do_status"] = int(status_rows[0].get('do_status') or 0)
                except Exception:
                    pass

                valves[f"valve_{valve_no}"] = valve_data

            device_list.append({
                "device_id": device_id,
                "device": device_uid,
                "device_name": dev.get('device_name', device_uid),
                "model": dev.get('model', ''),
                "device_type": dev.get('device_type', 'OMS'),
                "status": 'online' if is_online else 'offline',
                "lat": dev.get('lat'),
                "lon": dev.get('lon'),
                "valves": valves
            })

        # 4) Get branch-level group schedules
        branch_schedule = {}
        for valve_no in range(1, 7):
            gs_sql = f"""
                SELECT group_schedule_id, do_type, do_no,
                       one_on_time, one_off_time,
                       two_on_time, two_off_time,
                       days, datalog_sec, slot, status
                FROM device_group_schedule
                WHERE branch_id = {params.branch_id}
                  AND do_no = {valve_no}
                  AND client_id = {params.client_id}
                ORDER BY group_schedule_id DESC
                LIMIT 1
            """
            try:
                gs = custom_select_sql_query(gs_sql, None)
                if gs and gs.get('group_schedule_id'):
                    branch_schedule[f"valve_{valve_no}"] = {
                        "do_type": gs.get('do_type'),
                        "one_on_time": str(gs.get('one_on_time', '00:00:00')),
                        "one_off_time": str(gs.get('one_off_time', '00:00:00')),
                        "two_on_time": str(gs.get('two_on_time', '00:00:00')),
                        "two_off_time": str(gs.get('two_off_time', '00:00:00')),
                        "days": gs.get('days', ''),
                        "slot": gs.get('slot', 0),
                        "status": gs.get('status', 1),
                        "has_schedule": True
                    }
                else:
                    branch_schedule[f"valve_{valve_no}"] = {"has_schedule": False, "do_type": None}
            except Exception:
                branch_schedule[f"valve_{valve_no}"] = {"has_schedule": False, "do_type": None}

        return {
            "branch": branch_info,
            "summary": {
                "total_devices": len(device_list),
                "active_devices": active_device_count,
                "active_valves": active_valve_count,
                "total_valves": len(device_list) * 6
            },
            "devices": device_list,
            "branch_schedule": branch_schedule
        }
    except Exception as e:
        raise e

# ─── Helper: get all devices in a branch ───
def _get_branch_devices(branch_id, client_id):
    """Returns list of {device_id, device, gateway_id} for all devices in a branch via branch_number"""
    try:
        branch_info = select_one_data("manage_branch", "branch_number", f"branch_id = {branch_id} AND client_id = {client_id}")
        if not branch_info or not branch_info.get('branch_number'):
            return []
        
        branch_number = branch_info['branch_number']
        dev_sql = f"""SELECT device_id, device, gateway_id 
                     FROM md_device 
                     WHERE branch_number = '{branch_number}' 
                       AND client_id = {client_id}"""
        devices = custom_select_sql_query(dev_sql, 1)
        if devices is None:
            return []
        
        result = []
        for dev in devices:
            result.append({
                "device_id": dev['device_id'],
                "device": dev['device'],
                "gateway_id": dev.get('gateway_id')
            })
        return result
    except Exception:
        return []

def _get_branch_number(branch_id, client_id):
    """Returns branch_number for the given branch_id"""
    try:
        branch_info = select_one_data("manage_branch", "branch_number", f"branch_id = {branch_id} AND client_id = {client_id}")
        if branch_info:
            return branch_info.get('branch_number', '')
        return ''
    except Exception:
        return ''

# ─── Branch-Level Switch (all devices) ───
def switch_branch_all(params):
    """Get branch info and unique gateway_ids for sending a single GC command per gateway"""
    try:
        branch_info = select_one_data("manage_branch", "branch_number", f"branch_id = {params.branch_id} AND client_id = {params.client_id}")
        if not branch_info or not branch_info.get('branch_number'):
            raise ValueError("Branch not found")
        
        branch_number = branch_info['branch_number']
        
        # Get unique gateway_ids for this branch
        gw_sql = f"""SELECT DISTINCT gateway_id 
                     FROM md_device 
                     WHERE branch_number = '{branch_number}' 
                       AND client_id = {params.client_id}
                       AND gateway_id IS NOT NULL 
                       AND gateway_id != ''"""
        gateways = custom_select_sql_query(gw_sql, 1)
        if not gateways:
            raise ValueError("No gateways found for this branch")
        
        gateway_ids = [gw['gateway_id'] for gw in gateways if gw.get('gateway_id')]
        
        return {
            "branch_number": branch_number,
            "gateway_ids": gateway_ids,
            "do_no": params.do_no,
            "value": params.value,
            "gateway_count": len(gateway_ids)
        }
    except Exception as e:
        raise e

# ─── Branch-Level Schedule Save (all devices) ───
def schedule_branch_all(params):
    """Save schedule to ALL devices in the branch"""
    try:
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

# ─── Insert/Update device_group_schedule ───
def upsert_group_schedule(params, user_id):
    """Insert or update the branch-level group schedule record"""
    try:
        current_datetime = get_current_datetime()
        slot = params.slot if hasattr(params, 'slot') and params.slot is not None else 0
        status = params.status if hasattr(params, 'status') and params.status is not None else 1
        condi = f"branch_id = {params.branch_id} AND do_no = {params.do_no} AND client_id = {params.client_id} AND slot = {slot}"
        existing = select_one_data("device_group_schedule", "group_schedule_id", condi)

        branch_number = _get_branch_number(params.branch_id, params.client_id)

        if existing:
            # UPDATE
            columns = {
                "do_type": params.do_type,
                "datalog_sec": params.datalog_sec or 120,
                "one_on_time": params.one_on_time,
                "one_off_time": params.one_off_time,
                "two_on_time": params.two_on_time or "00:00:00",
                "two_off_time": params.two_off_time or "00:00:00",
                "days": params.days or "sun,mon,tue,wed,thu,fri,sat",
                "slot": slot,
                "status": status,
                "updated_at": current_datetime,
                "created_by": user_id
            }
            update_data("device_group_schedule", columns, condi)
            return existing.get('group_schedule_id')
        else:
            # INSERT
            col_str = "client_id, branch_id, branch_number, do_type, do_no, datalog_sec, one_on_time, one_off_time, two_on_time, two_off_time, days, slot, status, created_by, created_at"
            val_str = (
                f"{params.client_id}, {params.branch_id}, '{branch_number}', "
                f"{params.do_type}, {params.do_no}, {params.datalog_sec or 120}, "
                f"'{params.one_on_time}', '{params.one_off_time}', "
                f"'{params.two_on_time or '00:00:00'}', '{params.two_off_time or '00:00:00'}', "
                f"'{params.days or 'sun,mon,tue,wed,thu,fri,sat'}', "
                f"{slot}, {status}, "
                f"{user_id}, '{current_datetime}'"
            )
            return insert_data("device_group_schedule", col_str, val_str)
    except Exception as e:
        raise e

# ─── Delete group schedule for a valve ───
def delete_group_schedule(branch_id, do_no, client_id):
    """Delete the branch-level group schedule record for a specific valve"""
    try:
        condi = f"branch_id = {branch_id} AND do_no = {do_no} AND client_id = {client_id}"
        delete_data("device_group_schedule", condi)
    except Exception as e:
        print("Error deleting group schedule:", e)
        raise e

# ─── Branch-Level Schedule Reset (all devices) ───
def reset_branch_schedule_all(params):
    """Reset schedule for a valve on ALL devices in the branch"""
    try:
        devices = _get_branch_devices(params.branch_id, params.client_id)
        if not devices:
            raise ValueError("No devices found in this branch")

        # Also delete the group schedule
        delete_group_schedule(params.branch_id, params.do_no, params.client_id)

        return {
            "devices": devices,
            "do_no": params.do_no,
            "device_count": len(devices)
        }
    except Exception as e:
        raise e
