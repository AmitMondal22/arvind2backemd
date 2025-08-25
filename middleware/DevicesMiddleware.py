from fastapi import Request, HTTPException




class DevicesMiddleware:
    async def __call__(self, request: Request, call_next):
        # Check Authorization header
        token = request.headers.get("Authorization")
        if token != "123456":
            raise HTTPException(status_code=401, detail="Unauthorized")
        
        # Check query parameters
        query_params = request.query_params
        # You can add logic to handle query parameters here
        
        # Check request body
        body = await request.body()
        # You can add logic to handle request body here
        
        # If everything is okay, proceed to the next handler
        response = call_next(request)
        return response