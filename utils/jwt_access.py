
from jose import jwt,ExpiredSignatureError
from fastapi import Request, status, HTTPException,Depends
from datetime import datetime, timedelta
from config.JWT_config import SECRET_KEY,ACCESS_TOKEN_EXPIRE_MINUTES,ALGORITHM
from fastapi.security import HTTPBearer



security = HTTPBearer()


def create_access_token(data: dict):
    try:
        to_encode = data.copy()
        access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        
        # Ensure subject is converted to a string if necessary
        to_encode["sub"] = str(to_encode["sub"])

        to_encode.update({"exp": datetime.utcnow() + access_token_expires})
        encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
        return encoded_jwt
    except jwt.JWTError as e:
        # Handle JWT encoding errors
        print("JWT encoding error:", e)
        return None




async def verify_token(token):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return eval(payload.get("sub"))
    except ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired",
        )
    except jwt.JWTError as e:
        # Handle other JWT decoding errors
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
        )
    
    
    
    
    
    
    
# # Custom middleware for token verification
async def verify_token1(request: Request):
    try:
        authorization: str = request.headers.get("Authorization")
        if not authorization:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authorization header not provided",
            )
        
        scheme, token = authorization.split()
        if scheme.lower() != "bearer":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication scheme",
            )
        print("token",token)
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        print("??????????????????????????????????????????????????",payload)
        # Example: Extract user_id from payload and set it in the request state
        
        
        
        request.state.user_data = eval(payload.get("sub"))
    except ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired",
        )
    except jwt.JWTError as e:
        # Handle other JWT decoding errors
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
        )
