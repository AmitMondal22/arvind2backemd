def createResponse(result, fields, flag):
    if result:  # If result is not empty
        return [dict(zip(fields, dt)) for dt in result] if flag > 0 else dict(zip(fields, result))
    else:  # If result is empty
        return [] if flag > 0 else {}

def createDbResponse(result, fields, flag):
    return [{fields[i]: value for i, value in enumerate(record)} if flag > 0 else {fields[i]: result[i] for i in range(len(fields))} for record in result]


def successResponse(data, message="Success")-> dict:
    response = {
        "status": "success",
        "message": message,
        "data": data
    }
    return response

def errorResponse(message="Error")-> dict:
    response = {
        "status": "error",
        "message": message
    }
    return response