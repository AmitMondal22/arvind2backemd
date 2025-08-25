from db_model.MASTER_MODEL import select_data, insert_data,update_data,delete_insert_restore,select_one_data
from utils.date_time_format import get_current_datetime
from utils.has_password import get_password_hash
from utils.otp import generate_otp




def add_user(user):
    try:
        password = get_password_hash(user.password)
        new_otp = generate_otp(6)
        current_datetime = get_current_datetime()
        
        columns = "user_name, user_email, user_info_id, user_active_status, user_type, otp_number, otp_active_status, password, created_by, created_at"
        value = f"'{user.name}', '{user.email}', {user.organization_id}, 'Y', 'U', {new_otp}, 'N', '{password}', 0, '{current_datetime}'"
        user_id = insert_data("users", columns, value)
        if user_id is None:
            raise ValueError("User registration failed")
        else:
            user_data = {"user_id": user_id, "name": user.name, "email": user.email}
        return user_data
    except Exception as e:
        raise e



def list_user(params):
    try:
        select="u.user_id, u.user_name, u.user_email, u.user_info_id, u.user_active_status, u.user_type, u.otp_number, u.otp_active_status, u.created_by, DATE_FORMAT(u.created_at, '%Y-%m-%d %H:%i:%s') AS created_at, o.organization_name, o.organization_id"
        table="users as u, md_organization as o"
        condition = f"u.user_info_id = o.organization_id AND o.client_id={params.client_id} AND u.user_type = 'U'"
        data = select_data(table, select,condition)
        return data
    except Exception as e:
        raise e



def user_info(params):
    try:
        select="u.user_id, u.user_name, u.user_email, u.user_info_id, u.user_active_status, u.user_type, u.otp_number, u.otp_active_status, u.created_by, DATE_FORMAT(u.created_at, '%Y-%m-%d %H:%i:%s') AS created_at, o.organization_name, o.organization_id"
        table="users as u, md_organization as o"
        condition = f"u.user_info_id = o.organization_id AND u.user_id={params.user_id} AND o.client_id={params.client_id} AND u.user_type = 'U'"
        data = select_one_data(table, select,condition)
        return data
    except Exception as e:
        raise e



def edit_user(user):
    try:
        # password = get_password_hash(user.password)
        condition = f"user_id = {user.user_id}"
        columns={"user_name":user.name,"user_email":user.email,"user_info_id":user.organization_id,"updated_at":get_current_datetime()}
        data = update_data("users", columns, condition)
        return data
    except Exception as e:
        raise e


def delete_user(user):
    try:
        condition = f"user_id = {user.user_id}"
        data = delete_insert_restore("users","del_users", condition)
        return data
    except Exception as e:
        raise e
    
