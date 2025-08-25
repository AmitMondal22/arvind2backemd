from config.db import connect
def select_data():
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