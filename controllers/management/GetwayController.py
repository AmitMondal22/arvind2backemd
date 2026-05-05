from db_model.MASTER_MODEL import insert_data, update_data, select_data, delete_data, custom_select_sql_query
from utils.date_time_format import get_current_datetime

class GatewayController:
    @staticmethod
    def add_gateway(params, user_data):
        try:
            current_datetime = get_current_datetime()
            # default project_id is assumed to be 0 or dynamic based on requirement. 
            project_id = 0
            created_by = user_data.get("user_id", 0)
            
            columns = "gateway_id, start_id, max_id, retry, project_id, created_by, created_at, updated_at"
            value = f"'{params.gateway_id}', {params.start_id}, {params.max_id}, {params.retry}, {project_id}, {created_by}, '{current_datetime}', '{current_datetime}'"
            
            insert_id = insert_data("md_gateway", columns, value)
            if not insert_id:
                raise ValueError("Gateway could not be saved")
            return {"id": insert_id, "gateway_id": params.gateway_id}
        except Exception as e:
            raise e

    @staticmethod
    def list_gateway():
        try:
            sql = """
                SELECT 
                    g.id, 
                    g.gateway_id, 
                    g.status,
                    g.start_id, 
                    g.max_id, 
                    g.retry, 
                    g.connected_device,
                    g.project_id, 
                    g.created_by, 
                    DATE_FORMAT(g.created_at, '%Y-%m-%d %H:%i:%s') AS created_at, 
                    DATE_FORMAT(g.updated_at, '%Y-%m-%d %H:%i:%s') AS updated_at
                FROM md_gateway g
                ORDER BY g.id DESC
            """
            gateways = custom_select_sql_query(sql, 1)
            return gateways if gateways else []
        except Exception as e:
            raise e

    @staticmethod
    def edit_gateway(params, user_data):
        try:
            current_datetime = get_current_datetime()
            columns_values = {
                "gateway_id": params.gateway_id,
                "start_id": params.start_id,
                "max_id": params.max_id,
                "retry": params.retry,
                "updated_at": current_datetime
            }
            condition = f"id = {params.id}"
            update_res = update_data("md_gateway", columns_values, condition)
            
            # Since MASTER_MODEL.update_data triggers fetchall() on UPDATEs, it may return False. 
            # We assume success if it reaches here without an SQL exception.
            return {"id": params.id, "gateway_id": params.gateway_id}
        except Exception as e:
            raise e

    @staticmethod
    def delete_gateway(params):
        try:
            condition = f"id = {params.id}"
            del_result = delete_data("md_gateway", condition)
            if not del_result:
                raise ValueError("Gateway could not be deleted")
            return "Gateway deleted successfully"
        except Exception as e:
            raise e
