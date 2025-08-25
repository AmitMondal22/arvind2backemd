from datetime import datetime, date

def get_current_datetime()->str:
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    return formatted_datetime

def get_current_date()->str:
    current_date = date.today()
    formatted_date = current_date.strftime("%Y-%m-%d")
    return formatted_date

def get_current_time()->str:
    current_datetime = datetime.now()
    formatted_time = current_datetime.strftime("%H:%M:%S")
    return formatted_time

def get_current_year()->int:
    current_date = date.today()
    current_year = current_date.year
    return current_year

def get_current_month()->int:
    current_date = date.today()
    current_month = current_date.month
    return current_month

def get_current_day()->int:
    current_date = date.today()
    current_day = current_date.day
    return current_day

def get_current_weekday()->int:
    current_date = date.today()
    current_weekday = current_date.weekday()
    return current_weekday

def get_current_hour()->int:
    current_datetime = datetime.now()
    current_hour = current_datetime.hour
    return current_hour

def get_current_minute()->int:
    current_datetime = datetime.now()
    current_minute = current_datetime.minute
    return current_minute

def get_current_second()->int:
    current_datetime = datetime.now()
    current_second = current_datetime.second
    return current_second

def get_current_microsecond()->int:
    current_datetime = datetime.now()
    current_microsecond = current_datetime.microsecond
    return current_microsecond

def get_current_timestamp()->int:
    current_datetime = datetime.now()
    current_timestamp = int(round(current_datetime.timestamp()))
    return current_timestamp

def get_current_time_isoformat()->str:
    current_datetime = datetime.now()
    current_time_isoformat = current_datetime.isoformat()
    return current_time_isoformat

def get_current_date_isoformat()->str:
    current_date = date.today()
    current_date_isoformat = current_date.isoformat()
    return current_date_isoformat

def get_current_date_time_isoformat()->str:
    current_datetime = datetime.now()
    current_date_time_isoformat = current_datetime.isoformat()
    return current_date_time_isoformat

def get_current_date_time_utc()->str:
    current_datetime = datetime.utcnow()
    current_date_time_utc = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    return current_date_time_utc

def get_current_date_utc()->str:
    current_date = date.today()
    current_date_utc = current_date.strftime("%Y-%m-%d")
    return current_date_utc

def get_current_time_utc()->str:
    current_datetime = datetime.utcnow()
    current_time_utc = current_datetime.strftime("%H:%M:%S")
    return current_time_utc
def get_current_timedelta()->str:
    current_datetime = datetime.now()
    current_timedelta = current_datetime.strftime("%H:%M:%S")
    return current_timedelta


def get_current_datetime_string():
    # Get the current date and time
    now = datetime.now()
    
    # Format the datetime as "DD,MM,YYYY,HR,MM,SS"
    formatted_datetime = now.strftime("%d,%m,%Y,%H,%M,%S")
    
    return formatted_datetime