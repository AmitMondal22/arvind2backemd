from db_model.MASTER_MODEL import select_data, insert_data,update_data,delete_data
from utils.date_time_format import get_current_datetime


def add_device(device):
    try:
        current_datetime = get_current_datetime()
        columns = "client_id,organization_id, user_id, device_id,device, created_by, created_at"
        value = f"{device.client_id},{device.organization_id}, {device.user_id}, {device.device_id},'{device.device}', '{device.created_by}', '{current_datetime}'"
        device_id = insert_data("md_manage_user_device", columns, value)
        print(device_id)
        if device_id is None:
            raise ValueError("device registration failed")
        else:
            device_data = {"device_id": device_id, "client_id": device.client_id, "organization_id": device.organization_id, "user_id": device.user_id, "device_id": device.device_id, "device": device.device, "created_by": device.created_by, "created_at": current_datetime}
        return device_data
    except Exception as e:
        raise e
    
def project_add_device(project_device, user_data):
    try:
        current_datetime = get_current_datetime()
        columns = "project_id, organization_id, device_id, device, create_by, created_at"
        value = f"{project_device.project_id},{project_device.organization_id}, {project_device.device_id}, '{project_device.device}', '{user_data['client_id']}', '{current_datetime}'"
        manage_project_device = insert_data("md_manage_project_device", columns, value)
        print(manage_project_device)
        
        if manage_project_device is None:
            raise ValueError("device registration failed")
        else:
            device_data = {"manage_project_device_id": manage_project_device, "project_id": project_device.project_id, "organization_id": project_device.organization_id, "device_id": project_device.device_id, "device": project_device.device, "created_by": user_data["client_id"], "created_at": current_datetime}
        return device_data
    except Exception as e:
        raise e
    
    
def project_list_device(user_data):
    try:
        if user_data["user_type"]=='U' or user_data["user_type"]=='O':
            condition =f"c.project_id = b.project_id AND c.organization_id = a.organization_id AND d.device_id = c.device_id AND a.client_id = {user_data['client_id']} AND a.organization_id = {user_data['organization_id']} "
        elif user_data["user_type"]=='C':
            condition =f"c.project_id = b.project_id AND c.organization_id = a.organization_id AND d.device_id = c.device_id AND a.client_id = {user_data['client_id']}"
        select="b.project_name, b.project_id, b.organization_id, a.organization_name, a.organization_id, d.device_id, d.device, c.manage_project_device_id, d.imei_no, d.model,d.device_name"
        data = select_data("md_organization a, md_project b, md_manage_project_device c, md_device d", select,condition)
        return data
    except Exception as e:
        raise e
    
    
    

def project_delete_device(organization):
    try:
        condition = f"manage_project_device_id = {organization.manage_project_device_id}"
        data = delete_data("md_manage_project_device", condition)
        return data
    except Exception as e:
        raise e
    
    
    

def list_user_device(params):
    try:
        select="o.organization_name,o.organization_id,u.user_id,u.user_name,u.user_email,u.user_active_status,d.device_id,d.device,d.do_channel,d.model,d.lat,d.lon,d.imei_no, mud.manage_user_device_id"
        
        table="md_manage_user_device AS mud, users AS u, md_device AS d, md_organization  AS o"
        condition=f"mud.user_id = u.user_id AND mud.device_id = d.device_id AND o.organization_id=mud.organization_id AND o.client_id={params.client_id}"
        order_by="d.device ASC, u.user_id ASC"
        
        
        data = select_data(table, select,condition,order_by)
        return data
    except Exception as e:
        raise e


def edit_device(device):
    try:
        condition = f"manage_user_device_id = {device.manage_user_device_id} AND client_id = {device.client_id}"
        columns={"organization_id":device.organization_id, "user_id":device.user_id, "device_id":device.device_id,"device":device.device, "created_by":device.created_by,"updated_at":get_current_datetime()}
        data = update_data("md_manage_user_device", columns, condition)
        print(data)
        return data
    except Exception as e:
        raise e


def delete_device(device):
    try:
        condition = f"manage_user_device_id = {device.manage_user_device_id} AND client_id = {device.client_id}"
        data = delete_data("md_manage_user_device", condition)
        return data
    except Exception as e:
        raise e