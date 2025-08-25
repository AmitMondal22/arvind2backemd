from decimal import Decimal
import json
from datetime import date, datetime, timedelta

class DecimalEncoder(json.JSONEncoder):
     def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        elif isinstance(o, datetime):
            return o.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(o, date):
            return o.strftime('%Y-%m-%d')
        elif isinstance(o, timedelta):
            return str(o)
        elif isinstance(o, set):
            return list(o)
        elif isinstance(o, dict):
            return {key: self.default(value) for key, value in o.items()}
        elif isinstance(o, list):
            return [self.default(element) for element in o]
        elif isinstance(o, tuple):
            return tuple(self.default(element) for element in o)
        return super().default(o)
    
    




# class DecimalEncoder(json.JSONEncoder):
#     def default(self, o):
#         if isinstance(o, Decimal):
#             return str(o)
#         return super(DecimalEncoder, self).default(o)
    