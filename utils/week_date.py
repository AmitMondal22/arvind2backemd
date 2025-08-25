from datetime import datetime, timedelta
def weekdays_date():
    today = datetime.now().date()

    # Create an array to store dates
    date_array = []

    # Loop through the last 7 days and append to the array
    for i in range(7):
        date = today - timedelta(days=i)
        date_array.append(date.strftime("%Y-%m-%d"))
    return date_array