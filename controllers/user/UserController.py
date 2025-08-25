from db_model.MASTER_MODEL import select_data
def get_user(user_id: int):
    if user_id <= 0:
        raise ValueError("User ID must be positive")
    
    # Logic to fetch user from database
    user = {"user_id": user_id, "name": "John Doe"}  # Example user data
    return user

def create_user(user_data: dict):
    if "name" not in user_data:
        raise ValueError("Name is required to create a user")
    
    # Logic to create user in the database
    # For demonstration, just return the received data
    return user_data
def get_users():
    data=select_data()
    if data is None:
        raise ValueError("No data found")
    return data
