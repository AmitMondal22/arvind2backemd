from db_model.MASTER_MODEL import select_data, insert_data,update_data,delete_data,select_one_data
from utils.has_password import get_password_hash, verify_password


async def upload_update_client_logo(user_data,client_name,client_address,client_mobile,client_email,filename=None):
    try:
        update_condition=f"client_id = {user_data['client_id']}"
        if filename is None:
            set_values={"client_name":client_name,"client_address":client_address,"client_mobile":client_mobile,"client_email":client_email,"create_by":user_data['user_id']}
        else:
            set_values={"logo":filename,"client_name":client_name,"client_address":client_address,"client_mobile":client_mobile,"client_email":client_email,"create_by":user_data['user_id']}
        
    
        edit_organization=update_data("md_client",set_values,update_condition)
        return edit_organization
    except Exception as e:
          raise e
      
async def change_password(user_data,params):
    try:
        base_select = ("password")
        base_condition = f"user_email = '{user_data['user_email']}' AND user_id={user_data['user_id']}"
        # Fetch initial user info
        user_info = select_one_data("users", base_select, base_condition, None)
        if not verify_password(params.old_password, user_info["password"]):
            raise ValueError("Old Password mismatch")
        password = get_password_hash(params.confirm_password)
        update_condition=f"user_id = {user_data['user_id']} AND user_email = '{user_data['user_email']}"
        set_values={"password":password}
        update_data("users",set_values,update_condition)
        return True
    except Exception as e:
          raise e