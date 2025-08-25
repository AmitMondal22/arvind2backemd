from db_model.MASTER_MODEL import insert_data,custom_select_sql_query,select_one_data,select_data,update_data
from utils.date_time_format import get_current_datetime

async def client_screen_settings(user, params):
    try:
        data = select_data("st_view_organization", "*", f"client_id = {user['client_id']} AND organization_id = {params.organization_id}")
        return data
    except Exception as e:
        raise e

async def client_screen_settings_edit(user, params):
    try:
        current_datetime = get_current_datetime()
        if params.id_view_organization:
            table="st_view_organization"
            set_value = {"gv_energy_used" : params.gv_energy_used.value, "gv_voltage" : params.gv_voltage.value, "gv_current" : params.gv_current.value, "gv_power" : params.gv_power.value, "mn_add_organization" : params.mn_add_organization.value, "mn_device_management" : params.mn_device_management.value, "mn_user_management" : params.mn_user_management.value, "en_tab_device_info" : params.en_tab_device_info.value, "en_tab_create_alert" : params.en_tab_create_alert.value, "en_tab_scheduling" : params.en_tab_scheduling.value, "en_tab_report_analysis" : params.en_tab_report_analysi.value, "updated_at":current_datetime}
            
            condition=f"id_view_organization = {params.id_view_organization} AND client_id = {user['client_id']} AND organization_id = {params.organization_id}"
            data=update_data(table, set_value, condition)
        else:
            table="st_view_organization"
            columns="user_type, client_id, organization_id, gv_energy_used, gv_voltage, gv_current, gv_power, mn_add_organization, mn_device_management, mn_user_management, en_tab_device_info, en_tab_create_alert, en_tab_scheduling, en_tab_report_analysis, created_by, created_at, updated_at"
            
            values=f"'{params.user_type}' ,{user['client_id']}, {params.organization_id}, '{params.gv_energy_used.value}', '{params.gv_voltage.value}', '{params.gv_current.value}', '{params.gv_power.value}', '{params.mn_add_organization.value}', '{params.mn_device_management.value}', '{params.mn_user_management.value}', '{params.en_tab_device_info.value}', '{params.en_tab_create_alert.value}', '{params.en_tab_scheduling.value}', '{params.en_tab_report_analysi.value}', {user['user_id']},'{current_datetime}','{current_datetime}'"
            print("heoolo")
            data=insert_data(table, columns, values)
            
            
            
            
        data = select_one_data("st_view_organization", "*", f"client_id = {user['client_id']} AND organization_id = {params.organization_id}")
        return data
    except Exception as e:
        raise e