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
            
            # Additional email alert
            # html_file_path = 'template/email/template_send_alert1.html'
            # try:
            #     send_email("amit.offici@gmail.com", f"Device {device} Alert: {alert_type}", html_file_path, dynamic_data=None)
            # except Exception as e:
            #     print("Failed to send email snippet:", e)

    except Exception as e:
        print("Error in send_alert:", e)

