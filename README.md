# weather_monitoring_system

<!-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++  -->

import mysql.connector
from dotenv import load_dotenv

load_dotenv()
MYSQL_HOST = "localhost"
MYSQL_USER = "amit"
MYSQL_PASSWORD = "Amit@7025"
MYSQL_DB = "weather_monitoring_system"

def connect():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB
    )
    
<!-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++  -->

SECRET_KEY = "352a3d4ce12e41af1c098553c8ca87c854c1be987a89cf9331ea8b4c7dc076a4d3e282aeb90f89e9e98e764a5fa40f35fc7605c04f5cd53a14793145ebee4a26"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 240 # 4 hours





# JWT Configuration