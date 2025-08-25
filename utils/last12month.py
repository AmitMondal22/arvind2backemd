from datetime import datetime, timedelta
import calendar
def last_12_month():

    # Get current year and month
    current_year = datetime.now().year
    current_month = datetime.now().month

    # Initialize an empty list to store the last dates
    last_dates = []

    # Loop through each month starting from the current month to 12 months ago
    for i in range(12):
        # Calculate the month and year for the current iteration
        year = current_year - (i // 12)
        month = current_month - (i % 12)
        if month <= 0:
            month += 12
            year -= 1
        
        # Get the last day of the current month
        last_day_of_month = calendar.monthrange(year, month)[1]
        # Format the date as YYYY-MM-DD and append to the list
        last_date = datetime(year, month, last_day_of_month).strftime('%Y-%m-%d')
        last_dates.append(last_date)

    # Print the array of last dates
    print(last_dates)
    return last_dates