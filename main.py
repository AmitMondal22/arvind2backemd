from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from json import JSONEncoder
from datetime import date, datetime, timedelta
from routes import api_client_routes, devices_routes, user_routes,auth_routes,mqtt_routes,ws_routes,api_user_routes,api_common_routes,webhooks_routes

from decimal import Decimal
import uvicorn



app = FastAPI()


# Set up CORS
origins = [
    "http://127.0.0.1:8051",
    "http://localhost:8051",
    "*"
]

# @app.middleware("http")
# Apply TrustedHost middleware to the main FastAPI app
app.add_middleware(TrustedHostMiddleware, allowed_hosts=["*"])

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Custom JSON encoder
class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        elif isinstance(obj, date):
            return obj.isoformat()
        elif isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, timedelta):
            return str(obj)
        else:
            return super().default(obj)

# Set the custom JSON encoder for FastAPI
app.json_encoder = CustomJSONEncoder



# Include user routes
app.include_router(auth_routes.auth_routes, prefix="/api/auth", tags=["auth"])

# Include user routes
app.include_router(user_routes.user_rutes, prefix="/api/user", tags=["user"])

# Include user routes
app.include_router(devices_routes.devices_routes, prefix="/api/device", tags=["device"])


# Include user routes
app.include_router(api_client_routes.api_client_routes, prefix="/api/client", tags=["client"])


app.include_router(api_user_routes.user_routes, prefix="/api/client_user", tags=["api_user"])



# Include user routes
app.include_router(mqtt_routes.mqtt_routes, prefix="/api/mqtt", tags=["mqtt"])


app.include_router(ws_routes.ws_routes, prefix="/api/water_ms_routes", tags=["WS"])
app.include_router(api_common_routes.api_common_routes, prefix="/api/common", tags=["common"])
app.include_router(webhooks_routes.webhooks_routes, prefix="/api/webhooks", tags=["webhooks"])





# Index route
@app.get('/')
def index():
    return "hello world"  # Corrected typo






if __name__ == "__main__":
    
    # Run the FastAPI application
    uvicorn.run("main:app", host="0.0.0.0", port=8051, reload=True)
    

    
    
    
