import struct

def parse_lora_packet(packet_str):
    try:
        # Strip framing if present
        if packet_str.startswith("^LDATA,") and packet_str.endswith("$"):
            packet_str = packet_str[len("^LDATA,"):-1]

        # Convert hex string to bytes
        raw_bytes = bytes.fromhex(packet_str)
        if len(raw_bytes) < 10:
            raise ValueError(f"Unexpected payload length: {len(raw_bytes)} (must be ≥ 10)")

        # Parse fields
        do_byte = raw_bytes[0]                     # DO status
        ai1 = struct.unpack(">H", raw_bytes[1:3])[0] / 10.0
        ai2 = struct.unpack(">H", raw_bytes[3:5])[0] / 10.0
        battery = raw_bytes[5] / 10.0
        device_id_numeric = struct.unpack(">H", raw_bytes[6:8])[0]  # 2-byte device ID
        device_id = f"TECH{str(device_id_numeric).zfill(4)}"
        # device_id = f"TS3000{device_id_numeric}"                     # concatenate TS3000 prefix

        return {
            "DO_status": format(do_byte, "08b"),  # binary string like "10101100"
            "AI1": ai1,
            "AI2": ai2,
            "BatteryVoltage": battery,
            "DeviceID": device_id
        }

    except Exception as e:
        return {"error": str(e)}
