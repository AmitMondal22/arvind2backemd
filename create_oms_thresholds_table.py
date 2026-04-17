from config.db import connect

def create_table():
    try:
        conn = connect()
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS oms_device_thresholds (
            id INT AUTO_INCREMENT PRIMARY KEY,
            device VARCHAR(155) NOT NULL UNIQUE,
            min_val FLOAT DEFAULT 0,
            max_val FLOAT DEFAULT 10,
            high_threshold FLOAT DEFAULT 8,
            low_threshold FLOAT DEFAULT 2,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
        ''')
        conn.commit()
        print("Table created successfully")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == '__main__':
    create_table()
