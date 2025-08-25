from db_model.MASTER_MODEL import insert_data,select_one_data
from utils.has_password import get_password_hash, verify_password
from utils.otp import generate_otp
from utils.date_time_format import get_current_datetime
from utils.jwt_access import create_access_token
from fastapi import HTTPException




async def register(user) -> dict:
    try:
        password = get_password_hash(user.password)
        new_otp = generate_otp(6)
        current_datetime = get_current_datetime()
        columns = "user_name, user_email, user_info_id, user_active_status, user_type, otp_number, otp_active_status, password, created_by, created_at"
        value = f"'{user.name}', '{user.email}', 1, 'Y', '{user.user_type}', {new_otp}, 'N', '{password}', 0, '{current_datetime}'"
        user_id = insert_data("users", columns, value)
        if user_id is None:
            raise ValueError("User registration failed")
        else:
            user_data = {"user_id": user_id, "name": user.name, "email": user.email}
        return user_data
    except Exception as e:
        raise e


# 
# async def login(user) -> dict:
#     try:
#         condition = f"user_email = '{user.email}'"
#         select = f"user_id, user_name, user_email, user_info_id, user_active_status, user_type, otp_number, otp_active_status, password, created_by, DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s') AS created_at"
#         user_info=select_one_data("users",select,condition,None)

#         if user_info is None:
#             raise ValueError("User login failed")
#         else:
            
#             if user_info['user_type'] == 'S':
#                 table="users"
#                 select="user_id, user_name, user_email, user_info_id, user_active_status, user_type, otp_number, otp_active_status, password, created_by, DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s') AS created_at"
#                 condition=f"user_email = '{user.email}'"
#                 order_by=None
#                 login_data=select_one_data(table,select,condition,order_by)    
#                 print("no super admin",)
#             elif user_info['user_type'] == 'A':
#                 print("no clientadmin",)
#             elif user_info['user_type'] == 'C':
#                 table="users AS u,  md_client AS c"
#                 select="u.user_id, u.user_name, u.user_email, u.user_info_id, u.user_active_status, u.user_type, u.otp_number, u.otp_active_status, u.password, u.created_by, c.client_id, c.client_name, c.client_address, c.client_mobile, c.client_email"
#                 condition=f"u.user_email = '{user.email}' AND u.user_info_id = c.client_id AND u.user_active_status = 'Y' AND u.user_type = 'C'"
#                 order_by=None
#                 login_data=select_one_data(table,select,condition,order_by) 
#                 print(login_data["password"])
#             elif user_info['user_type'] == 'O':
#                 print("no org admin",)
#             elif user_info['user_type'] == 'U':
#                 table="users AS u,  md_client AS c, md_organization AS org LEFT JOIN st_view_organization AS svo ON org.organization_id = svo.organization_id AND svo.user_type='U'"
                
#                 select="u.user_id, u.user_name, u.user_email, u.user_info_id, u.user_active_status, u.user_type, u.otp_number, u.otp_active_status, u.password, u.created_by, c.client_id, c.client_name, c.client_address, c.client_mobile, c.client_email, org.organization_name,org.organization_id,svo.gv_energy_used, svo.gv_voltage, svo.gv_current, svo.gv_power, svo.mn_add_organization, svo.mn_device_management, svo.mn_user_management, svo.en_tab_device_info, svo.en_tab_create_alert, svo.en_tab_scheduling, svo.en_tab_report_analysis"
                
#                 condition=f"u.user_email = '{user.email}' AND u.user_info_id = org.organization_id AND org.client_id=c.client_id AND u.user_active_status = 'Y' AND u.user_type = 'U'"
#                 order_by=None
#                 login_data=select_one_data(table,select,condition,order_by) 
#                 print(login_data)
#                 print(login_data["password"])
                
#             else:
#                 raise ValueError("Invalid user type")
            
            
                
            
            
            
            
#             if verify_password(user.password, login_data["password"]) is False:
#                 raise ValueError("Password mismatch")
#             else:
#                 # Create and return JWT token
#                 access_token = create_access_token(data={"sub": login_data})
#                 print("Access token created",access_token)
                
                
#             return {"user_data":login_data,"token":access_token}
    
    
#     except Exception as e:
#         raise e





async def login(user) -> dict:
    try:
        # Base select and condition for initial user info fetch
        base_select = ("user_id, user_name, user_email, user_info_id, user_active_status, "
                       "user_type, otp_number, otp_active_status, password, created_by, "
                       "DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s') AS created_at")
        base_condition = f"user_email = '{user.email}'"
        
        # Fetch initial user info
        user_info = select_one_data("users", base_select, base_condition, None)
        
      
        if user_info is None or not user_info:
            raise ValueError("User not found")
        
        # Prepare select fields and joins based on user type
        select_fields = [
            "u.user_id", "u.user_name", "u.user_email", "u.user_info_id", 
            "u.user_active_status", "u.user_type", "u.otp_number", 
            "u.otp_active_status", "u.password", "u.created_by", 
            "DATE_FORMAT(u.created_at, '%Y-%m-%d %H:%i:%s') AS created_at"
        ]
        additional_joins = ""
        condition = f"u.user_email = '{user.email}'"

        if user_info['user_type'] == 'C':
            select_fields += [
                "c.client_id", "c.client_name", "c.client_address", "c.logo",
                "c.client_mobile", "c.client_email"
            ]
            additional_joins = "JOIN md_client AS c ON u.user_info_id = c.client_id"
            condition += " AND u.user_active_status = 'Y' AND u.user_type = 'C'"
        elif user_info['user_type'] == 'U':
            select_fields += [
                "c.client_id", "c.client_name", "c.client_address","c.logo", 
                "c.client_mobile", "c.client_email", "org.organization_name", 
                "org.organization_id", "svo.gv_energy_used", "svo.gv_voltage", 
                "svo.gv_current", "svo.gv_power", "svo.mn_add_organization", 
                "svo.mn_device_management", "svo.mn_user_management", 
                "svo.en_tab_device_info", "svo.en_tab_create_alert", 
                "svo.en_tab_scheduling", "svo.en_tab_report_analysis"
            ]
            additional_joins = (
                "JOIN md_organization AS org ON u.user_info_id = org.organization_id "
                "JOIN md_client AS c ON org.client_id = c.client_id "
                "LEFT JOIN st_view_organization AS svo ON org.organization_id = svo.organization_id AND svo.user_type='U'"
            )
            condition += " AND u.user_active_status = 'Y' AND u.user_type = 'U'"

        # Formulate the final query
        table = f"users AS u {additional_joins}"
        select = ", ".join(select_fields)

        # Fetch user data based on user type
        login_data = select_one_data(table, select, condition, None)
        if login_data is None:
            raise ValueError("User login failed")

        # Verify password
        if not verify_password(user.password, login_data["password"]):
            raise ValueError("Password mismatch")

        # Create and return JWT token
        access_token = create_access_token(data={"sub": login_data})
        return {"user_data": login_data, "token": access_token}

    except Exception as e:
        raise e