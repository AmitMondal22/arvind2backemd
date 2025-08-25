from config.db import connect
from typing import Tuple, List, Any, Optional, Dict
from utils.response import createDbResponse


async def mqtt_topic_name():
    try:
         # ems/{message_data.ib_id}/{message_data.device}
        # query=f"select CONCAT('ems/', client_id, '/', device) AS concatenated_string from md_device"
        query=f"SELECT GROUP_CONCAT(DISTINCT gateway_id) AS concatenated_string FROM md_device"
        conne = connect()
        cursor = conne.cursor()
        cursor.execute(query)
        records = cursor.fetchall()
        result=createDbResponse(records, cursor.column_names, 1)
        return result
    except Exception as e:
        print(e)
        if conne:
            conne.rollback()  # Rollback the transaction if an error occurs
        raise e
    finally:
        if cursor:
            cursor.close()
        if conne:
            conne.close() 