import base64


def decode_base64(encoded_str):
    """
    Decode a base64 string and return its hexadecimal representation.
    """
    # Decode from Base64 to bytes
    byte_data = base64.b64decode(encoded_str)
    # Convert bytes to hex string
    hex_str = byte_data.hex()
    return hex_str