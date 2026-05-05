from db_model.MASTER_MODEL import select_one_data, insert_data, custom_select_sql_query, select_data
from config.db import connect
from Library.EmailLibrary import send_email
import json
import asyncio


def send_alert(client_id, device, data):
    try:

        # Check thresholds
        threshold_records = custom_select_sql_query(f"SELECT * FROM oms_device_thresholds WHERE device = '{device}'")
        if not threshold_records:
            return

        threshold = threshold_records[0]
        high_threshold = threshold.get('high_threshold')
        low_threshold = threshold.get('low_threshold')
        min_val = threshold.get('min_val', 0)
        max_val = threshold.get('max_val', 100)

        if high_threshold is None or low_threshold is None:
            return
            
        val = getattr(data, 'A1', None)
        if val is None:
            if isinstance(data, dict):
                val = data.get('A1')
            elif hasattr(data, 'dict'):
                data_dict = data.dict()
                val = data_dict.get('A1')

        if val is None:
            return
            
        try:
            raw_val = float(val)
            min_v = float(min_val) if min_val is not None else 0.0
            max_v = float(max_val) if max_val is not None else 100.0
            high_t = float(high_threshold)
            low_t = float(low_threshold)
        except (ValueError, TypeError):
            return
            
        # Calibrate value: min(current + input, max)
        calibrated_val = min(min_v + raw_val, max_v)

        alert_type = None
        if calibrated_val >= high_t:
            alert_type = "High Value"
        elif calibrated_val <= low_t:
            alert_type = "Low Value"

        if alert_type:
            # Save original raw value to oms_alert_log
            columns = "client_id, device, alert_type, alert_value"
            values = f"'{client_id}', '{device}', '{alert_type}', {raw_val}"
            insert_data("oms_alert_log", columns, values)
            
            # Send alert via WebSocket to all users assigned to this device
            alert_message = json.dumps({
                "type": "DEVICE_ALERT",
                "device": device,
                "alert_type": alert_type,
                "value": calibrated_val,
                "raw_value": raw_val,
                "message": f"Device {device} Alert: {alert_type}",
                "client_id": client_id
            })
            
            try:
                # Get all user mobiles assigned to this device
                user_mobiles = custom_select_sql_query(
                    f"SELECT DISTINCT u.user_mobile FROM users u "
                    f"INNER JOIN md_manage_user_device mud ON u.user_id = mud.user_id "
                    f"WHERE mud.device_id = (SELECT device_id FROM md_device WHERE device = '{device}' AND client_id = {client_id} LIMIT 1) "
                    f"AND u.user_mobile IS NOT NULL AND u.user_mobile != ''"
                )
                
                if user_mobiles:
                    from routes.ws_routes import send_ws_alert_to_mobile
                    loop = asyncio.get_event_loop()
                    for user_row in user_mobiles:
                        mobile = user_row.get('user_mobile')
                        if mobile:
                            asyncio.ensure_future(send_ws_alert_to_mobile(mobile, alert_message))
                            print(f"Alert WS sent to mobile: {mobile} -> {alert_message}")
            except Exception as ws_err:
                print(f"Error sending WebSocket alert: {ws_err}")

            # Additional email alert
            # html_file_path = 'template/email/template_send_alert1.html'
            # try:
            #     send_email("amit.offici@gmail.com", f"Device {device} Alert: {alert_type}", html_file_path, dynamic_data=None)
            # except Exception as e:
            #     print("Failed to send email snippet:", e)

    except Exception as e:
        print("Error in send_alert:", e)
