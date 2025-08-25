from db_model.MASTER_MODEL import select_data,batch_insert_data,update_data,delete_data
from utils.date_time_format import get_current_datetime

def add_alert(data):
    try:

        column="client_id, organization_id, device_id, device, unit_id, alert_type, alert_status, alert_value,alert_email,create_by,created_at"
        # row_data=f"{data.client_id},{data.organization_id},{data.device_id}, '{data.device}', {data.unit_id}, '{data.alert_type}', '{data.alert_status}', {data.alert_value},'{data.alert_email}',{data.create_by},'{get_current_datetime()}'"
        # alert_id=insert_data("td_alert", column,row_data)
        
        
        
        # rows_data = [{"name": "John", "email": "john@example.com"}, {"name": "Alice", "email": "alice@example.com"}]
        
        rows_data = []
        for entry in data:
            row_data = {
                "client_id": entry.client_id,
                "organization_id": entry.organization_id,
                "device_id": entry.device_id,
                "device": entry.device,
                "unit_id": entry.unit_id,
                "alert_type": entry.alert_type,
                "alert_status": entry.alert_status,
                "alert_value": entry.alert_value,
                "alert_email": entry.alert_email,
                "create_by": entry.create_by,
                "created_at": get_current_datetime()  # Assuming get_current_datetime() returns the current datetime
            }
            rows_data.append(row_data)
        
       
        
        print("row_data_list---------------------", rows_data)
        # row_data=f"{data.client_id},{data.organization_id},{data.device_id}, '{data.device}', {data.unit_id}, '{data.alert_type}', '{data.alert_status}', {data.alert_value},'{data.alert_email}',{data.create_by},'{get_current_datetime()}'"
        
        batch_dataid=batch_insert_data("td_alert", column, rows_data)
        print("batch_dataid---------------------", batch_dataid)
        return batch_dataid
    except Exception as e:
        raise e



async def edit_alert(params):
    try:
        condition = f"alert_id = {params.alert_id} AND client_id = {params.client_id}"
        columns={"device_id":params.device_id, "device":params.device, "unit_id":params.unit_id, "alert_type":params.alert_type, "alert_status":params.alert_status, "alert_value":params.alert_value, "alert_email":params.alert_email, "create_by":params.create_by, "updated_at":get_current_datetime()}
        data = update_data("td_alert", columns, condition)
        print(data)
        return data
    except Exception as e:
        raise e


async def list_alert(params,user_data):
    try:
        
        if user_data["user_type"]=='U' or user_data["user_type"]=='O':
            condition = f"a.unit_id=b.unit_id AND a.client_id={user_data['client_id']} AND a.organization_id={user_data['organization_id']}"
        elif user_data["user_type"]=='C':
            condition = f"a.unit_id=b.unit_id AND a.client_id={user_data['client_id']}"
            
            
        select="a.alert_id, a.client_id, a.organization_id, a.device_id, a.device, a.unit_id, a.alert_type, a.alert_status, a.alert_value, a.alert_email, a.create_by, a.created_at, b.unit,b.unit_name"
        table = "td_alert AS a, md_unit AS b"
        
        order_by="a.device_id ASC"
        data = select_data(table, select,condition,order_by)
        return data
    except Exception as e:
        raise e



async def delete_alert(params):
    try:
        condition = f"alert_id = {params.alert_id} AND client_id = {params.client_id} AND organization_id = {params.organization_id} AND device_id = {params.device_id}"
        data = delete_data("td_alert", condition)
        print("?????????????????????????????????",data)
        return data
    except Exception as e:
        raise e