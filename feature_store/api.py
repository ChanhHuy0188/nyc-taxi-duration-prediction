from datetime import datetime
from typing import List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from feast import FeatureStore
from pydantic import BaseModel, Field
import pandas as pd

store = FeatureStore(repo_path=".")

app = FastAPI(
    title="NYC Taxi Feature Store API",
    description="API for serving real-time and historical taxi trip features",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class LocationFeatureRequest(BaseModel):
    """Request model for location-based features"""
    latitude: float = Field(..., ge=-90, le=90, description="Location latitude")
    longitude: float = Field(..., ge=-180, le=180, description="Location longitude")
    timestamp: Optional[datetime] = Field(default_factory=datetime.now, description="Timestamp for feature calculation")


class TripFeatureRequest(BaseModel):
    """Request model for trip-based features"""
    pickup_location_id: int = Field(..., description="Pickup location ID")
    dropoff_location_id: int = Field(..., description="Dropoff location ID")
    vendor_id: int = Field(..., description="Vendor ID")
    passenger_count: int = Field(..., gt=0, description="Number of passengers")
    timestamp: Optional[datetime] = Field(default_factory=datetime.now, description="Trip timestamp")


class BatchTripFeatureRequest(BaseModel):
    """Request model for batch trip feature calculation"""
    trip_ids: List[int] = Field(..., description="List of trip IDs to get features for")


@app.post("/location/features")
async def get_location_features(request: LocationFeatureRequest):
    """Get real-time features for a specific location"""
    try:
        result = await store.get_online_features_async(
            features=[
                "location_features:avg_fare",
                "location_features:trip_count",
                "location_features:avg_tip_percentage",
                "location_features:popular_hours",
                "location_features:surge_multiplier",
                "location_features:demand_level"
            ],
            entity_rows=[{
                "latitude": request.latitude,
                "longitude": request.longitude,
                "timestamp": request.timestamp.isoformat()
            }],
        ).to_dict()

        return {
            "location_features": result,
            "timestamp": request.timestamp,
            "metadata": {
                "feature_freshness": "real-time",
                "source": "online_store"
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching location features: {str(e)}")


@app.post("/trip/features")
async def get_trip_features(request: TripFeatureRequest):
    """Get real-time features for a taxi trip"""
    try:
        result = await store.get_online_features_async(
            features=[
                "trip_features:expected_duration",
                "trip_features:expected_fare",
                "trip_features:surge_multiplier",
                "trip_features:traffic_level",
                "trip_features:weather_conditions",
                "trip_features:historical_demand"
            ],
            entity_rows=[{
                "pickup_location_id": request.pickup_location_id,
                "dropoff_location_id": request.dropoff_location_id,
                "vendor_id": request.vendor_id,
                "passenger_count": request.passenger_count,
                "timestamp": request.timestamp.isoformat()
            }],
        ).to_dict()

        return {
            "trip_features": result,
            "timestamp": request.timestamp,
            "metadata": {
                "feature_freshness": "real-time",
                "source": "online_store"
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching trip features: {str(e)}")


@app.post("/batch/trip/features")
async def get_batch_trip_features(request: BatchTripFeatureRequest):
    """Get historical features for a batch of trips"""
    try:
        result = store.get_historical_features(
            entity_df=pd.DataFrame({
                "trip_id": request.trip_ids,
                "event_timestamp": [datetime.now()] * len(request.trip_ids)
            }),
            features=[
                "historical_trip_features:avg_duration",
                "historical_trip_features:avg_fare",
                "historical_trip_features:avg_tip_percentage",
                "historical_trip_features:common_payment_type",
                "historical_trip_features:peak_hour_stats",
                "historical_trip_features:route_popularity"
            ],
        ).to_dict()

        return {
            "historical_features": result,
            "metadata": {
                "feature_freshness": "historical",
                "source": "offline_store"
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching batch features: {str(e)}")


@app.get("/feature-list")
async def get_feature_list():
    """Get list of available features"""
    return {
        "location_features": [
            "avg_fare",
            "trip_count",
            "avg_tip_percentage",
            "popular_hours",
            "surge_multiplier",
            "demand_level"
        ],
        "trip_features": [
            "expected_duration",
            "expected_fare",
            "surge_multiplier",
            "traffic_level",
            "weather_conditions",
            "historical_demand"
        ],
        "historical_features": [
            "avg_duration",
            "avg_fare",
            "avg_tip_percentage",
            "common_payment_type",
            "peak_hour_stats",
            "route_popularity"
        ]
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
