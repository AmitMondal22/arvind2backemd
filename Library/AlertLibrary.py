from db_model.MASTER_MODEL import select_data
from Library.EmailLibrary import send_email
import json
def send_alert(client_id, device_id, device, data):
    print("TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT")
    try:
        print("TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT")
        select="a.alert_id, a.client_id, a.organization_id, a.device_id, a.device, a.unit_id, a.alert_type, a.alert_value, a.alert_status, DATE_FORMAT(a.created_at, '%Y-%m-%d %H:%i:%s') AS created_at, b.unit,b.unit_name"
        table = "td_alert AS a, md_unit AS b"
        condition=f"a.unit_id=b.unit_id AND a.client_id={client_id} AND a.device_id='{device_id}' AND a.device='{device}' AND a.alert_status='Y'"
        order_by="a.alert_type ASC"
        alertdata=select_data(table,select,condition,order_by)        
        low_value = float('inf')  # Set to positive infinity
        high_value = float('-inf')  # Set to negative infinity
        critical_low_value = float('inf')  # Set to positive infinity
        data = json.loads(data)
       
        alert_status=False
        unit_value_pairs = []
        for item in alertdata:
            for key2, value2 in data.items():
                if key2 == item['unit']:
                    if item["alert_type"] == "1CL":
                        critical_low_value=item["alert_value"]
                        if item["alert_value"] > value2:
                            unit_value_pairs.append((item['unit'], "Critical Low Value",value2))
                            alert_status=True
                    elif item["alert_type"] == "2L":
                        low_value=item["alert_value"]
                        if item["alert_value"] > value2 and value2 >= critical_low_value:
                            unit_value_pairs.append((item['unit'], "Low Value", value2))
                            alert_status=True
                    elif item["alert_type"] == "3H":
                        high_value=item["alert_value"]
                        if item["alert_value"] > value2 and value2 >= low_value:
                            unit_value_pairs.append((item['unit'],"High Value", value2))
                            alert_status=True
                    elif item["alert_type"] == "4CH":
                        if item["alert_value"] < value2 or value2 > high_value:
                            unit_value_pairs.append((item['unit'], "Critical High Value", value2))
                            alert_status=True
        low_value=0.0
        high_value=0.0
        critical_low_value=0.0
        print("TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT",alert_status)
        if alert_status:
            print("QQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQunit_value_pairs",unit_value_pairs)
            
            
            
            html_file_path='template/email/template_send_alert1.html'
            
            abc=send_email("amit.offici@gmail.com", "Alert",html_file_path ,dynamic_data=None)
            print("abc0000000000000000000000000",abc)
            alert_status=False
    except Exception as e:
        print(e)
        print("TEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE")