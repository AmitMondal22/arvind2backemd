from config.db import connect

def create_tables():
    conn = connect()
    cur = conn.cursor()
    
    # Create branches table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS manage_branch (
        branch_id INT AUTO_INCREMENT PRIMARY KEY,
        client_id INT NOT NULL,
        organization_id INT NOT NULL,
        project_id INT NOT NULL,
        branch_name VARCHAR(255) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    
    # Create branch devices table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS manage_branch_device (
        branch_device_id INT AUTO_INCREMENT PRIMARY KEY,
        client_id INT NOT NULL,
        branch_id INT NOT NULL,
        device_id INT NOT NULL,
        device VARCHAR(255) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("Tables created successfully.")

if __name__ == "__main__":
    create_tables()
