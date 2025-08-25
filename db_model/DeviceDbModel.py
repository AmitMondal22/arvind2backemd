from config.db import connect
def insert_deviceData():
    try:
       conne=connect()
       cursor=conne.cursor()
       query="select * from users"
       cursor.execute(query)
       records=cursor.fetchall()
       conne.close()
       cursor.close()
       return records
    except Exception as e:
        print(e)
        return None