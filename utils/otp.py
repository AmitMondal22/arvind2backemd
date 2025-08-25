import random
import string

def generate_otp(length: int = 6) -> int:
    digits = string.digits
    otp_str = ''.join(random.choice(digits) for _ in range(length))
    otp_int = int(otp_str)
    return otp_int