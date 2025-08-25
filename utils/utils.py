import re

def increment_string(string):
    # Split string into non-numeric and numeric parts
    non_numeric_part, numeric_part = re.match(r'(\D+)(\d*)$', string).groups()

    # Convert numeric part to integer and increment
    new_numeric_part = int(numeric_part) + 1 if numeric_part else 1

    # Ensure new numeric part is not negative
    new_numeric_part = max(new_numeric_part, 0)

    # Pad the numeric part with leading zeros to maintain the same length
    padded_numeric_part = str(new_numeric_part).zfill(len(numeric_part))

    return non_numeric_part + padded_numeric_part
