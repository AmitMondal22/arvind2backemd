from fastapi import APIRouter, HTTPException, Response,Depends,Request, Query
from middleware.MyMiddleware import mw_auth
from controllers.common import LocationController
from utils.response import errorResponse, successResponse
from Library.DecimalEncoder import DecimalEncoder
from models import common_model
import json
from typing import Optional

api_common_routes = APIRouter()


@api_common_routes.get("/location/regions", dependencies=[Depends(mw_auth)])
async def get_regions():
    try :
        data = await LocationController.regions_list()
        resdata = successResponse(data, message="List of regions")
        return Response(content=json.dumps(resdata, cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")
    
@api_common_routes.get("/location/sub_regions", dependencies=[Depends(mw_auth)])
async def get_sub_regions(region_id: Optional[int] = Query(None, description="The ID of the item to get")):
    try :
        data = await LocationController.sub_regions_list(region_id)
        resdata = successResponse(data, message="List of sub regions")
        return Response(content=json.dumps(resdata, cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")
    
# @api_common_routes.get("/location/countries")
@api_common_routes.get("/location/countries", dependencies=[Depends(mw_auth)])
async def get_countries(sub_region_id: Optional[int] = Query(None, description="The sub region id to get"),region_id: Optional[int] = Query(None, description="The region id to get"),):
    try :
        data = await LocationController.countries_list(sub_region_id,region_id)
        resdata = successResponse(data, message="List of countries")
        return Response(content=json.dumps(resdata, cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")

@api_common_routes.get("/location/states", dependencies=[Depends(mw_auth)])
async def get_states(country_id: int = Query(..., description="ID of the country to fetch states for")) -> common_model.CountryRequest:
    try :
        data = await LocationController.states_list(country_id)
        resdata = successResponse(data, message="List of states")
        return Response(content=json.dumps(resdata, cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")

@api_common_routes.get("/location/cities", dependencies=[Depends(mw_auth)])
async def get_cities(state_id: int = Query(..., description="ID of the state to fetch cities for")) -> common_model.StateRequest:
    try :
        data = await LocationController.cities_list(state_id)
        resdata = successResponse(data, message="List of cities")
        return Response(content=json.dumps(resdata, cls=DecimalEncoder), media_type="application/json", status_code=200)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")
