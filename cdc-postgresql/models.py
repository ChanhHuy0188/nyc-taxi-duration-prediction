from postgresql_client import Base
from sqlalchemy import BigInteger, Column, DateTime, Float, String, Integer, func


class TaxiTrip(Base):
    __tablename__ = "taxi_trips"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    VendorID = Column(Integer)
    tpep_pickup_datetime = Column(
        DateTime(timezone=True),
        server_default=func.timezone("UTC", func.current_timestamp()),
    )
    tpep_dropoff_datetime = Column(DateTime(timezone=True))
    passenger_count = Column(Integer)
    trip_distance = Column(Float)
    pickup_longitude = Column(Float)
    pickup_latitude = Column(Float)
    RateCodeID = Column(Integer)
    store_and_fwd_flag = Column(String(1))  # Usually 'Y' or 'N'
    dropoff_longitude = Column(Float)
    dropoff_latitude = Column(Float)
    payment_type = Column(Integer)
    fare_amount = Column(Float)
    extra = Column(Float)
    mta_tax = Column(Float)
    tip_amount = Column(Float)
    tolls_amount = Column(Float)
    improvement_surcharge = Column(Float)
    total_amount = Column(Float)
