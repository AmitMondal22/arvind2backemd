from db_model.MASTER_MODEL import select_one_data, insert_data, custom_select_sql_query
from config.db import connect
from Library.EmailLibrary import send_email
import json

def send_alert(client_id, device, data):
    try:

        # Check thresholds
        threshold_records = custom_select_sql_query(f"SELECT * FROM oms_device_thresholds WHERE device = '{device}'")
        if not threshold_records:
            return

        threshold = threshold_records[0]
        high_threshold = threshold.get('high_threshold')
        low_threshold = threshold.get('low_threshold')

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

        alert_type = None
        if val > high_threshold:
            alert_type = "High Value"
        elif val < low_threshold:
            alert_type = "Low Value"

        if alert_type:
            # Save to oms_alert_log
            columns = "client_id, device, alert_type, alert_value"
            values = f"'{client_id}', '{device}', '{alert_type}', {val}"
            insert_data("oms_alert_log", columns, values)
            
            # Additional email alert
            # html_file_path = 'template/email/template_send_alert1.html'
            # try:
            #     send_email("amit.offici@gmail.com", f"Device {device} Alert: {alert_type}", html_file_path, dynamic_data=None)
            # except Exception as e:
            #     print("Failed to send email snippet:", e)

    except Exception as e:
        print("Error in send_alert:", e)

