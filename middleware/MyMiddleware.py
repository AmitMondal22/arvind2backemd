from fastapi import Request, status, HTTPException
from utils.jwt_access import verify_token


async def mw_auth(request: Request):
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
        userdata_list=await verify_token(token)
        request.state.user_data = userdata_list
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error MW")

async def mw_client(request: Request):
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
        userdata_list=await verify_token(token)
        print(">>>>>>>>>>>>>>>>>>>>>>??????????????????????",userdata_list)
        if userdata_list['user_type'] != "C" or userdata_list is None :
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Access denied for this user type",
            )
        request.state.user_data = userdata_list
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error MW")
    
async def mw_user(request: Request):
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
        userdata_list=await verify_token(token)
        print(">>>>>>>>>>>>>>>>>>>>>>??????????????????????",userdata_list)
        if userdata_list['user_type'] != "U" or userdata_list is None :
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Access denied for this user type",
            )
        request.state.user_data = userdata_list
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error MW")
    
    
    
    
# =========================================================
async def mw_user_client(request: Request):
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
        userdata_list=await verify_token(token)
        print(">>>>>>>>>>>>>>>>>>>>>>??????????????????????",userdata_list)
        if userdata_list['user_type'] == "U" or userdata_list['user_type'] == "C" or userdata_list is not None :
            request.state.user_data = userdata_list
        else:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Access denied for this user type",
            )
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error MW")