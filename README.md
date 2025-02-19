# NYC Taxi Trips Duration Prediction

In the era of data-driven decision-making, machine learning (ML) is crucial in optimizing transportation systems. This project focuses on building an end-to-end ML pipeline to predict the trip duration of NYC taxi rides. Accurately estimating trip durations is essential for improving ride-sharing efficiency, reducing passenger wait times, and optimizing traffic flow in urban areas.

![image](https://github.com/user-attachments/assets/7dc853c3-5f80-4ae4-83b8-43d65e41baf0)

## 📑 Table of Contents

- [📊 Dataset](#-dataset)
  - [File Structure](#file-structure)
  - [Feature Engineering](#feature-engineering)
- [🌐 Architecture Overview](#-architecture-overview)
  - [1. Data Pipeline](#1-data-pipeline)
    - [📤 Data Sources](#-data-sources)
    - [✅ Schema Validation](#-schema-validation)
    - [☁️ Storage Layer](#-storage-layer)
    - [🛒 Spark Streaming](#-spark-streaming)
- [Contributing](#contributing)
- [📃 License](#-license)

## 📊 Dataset
The dataset used for this project can be found here. It contains trip record data collected from New York City’s Taxi and Limousine Commission (TLC), including details on trip duration, pickup and drop-off locations, fare amount, and other trip-related attributes.

The dataset spans multiple years and includes millions of taxi trip records, making it ideal for analyzing urban transportation patterns and developing machine learning models for trip duration prediction.

This dataset provides valuable insights into NYC taxi demand, traffic patterns, and passenger behavior, supporting applications in ride-hailing optimization, urban mobility planning, and congestion analysis.

### File Structure
|Field             |	Description|
| ---------------- | ---------------------------------------- |
|pickup_datetime   |	Timestamp when the trip started (UTC)   |
|dropoff_datetime  |	Timestamp when the trip ended (UTC)     |
|trip_duration     |	Duration of the trip in seconds         |
|pickup_longitude  |	Longitude of the pickup location        |
|pickup_latitude   |	Latitude of the pickup location         |
|dropoff_longitude |	Longitude of the drop-off location      |
|dropoff_latitude  |	Latitude of the drop-off location       |
|passenger_count   |	Number of passengers in the taxi        |
|fare_amount       |	Fare charged for the trip (USD)         |
|vendor_id         |	Identifier for the taxi service provider|

### Feature Engineering

To enhance the predictive power of our machine learning model, we extract meaningful features from the raw trip data. Key engineered features include:

### Feature	Description

| Feature				| Description												|
| --------------------- | --------------------------------------------------------- |
| hour_of_day 			| Hour of the day when the trip started						|
| day_of_week 			| Day of the week the trip occurred							|			
| trip_distance 		| Calculated distance between pickup and drop-off locations	|						
| speed_estimate 		| Approximate average speed of the trip						|	
| is_weekend 			| Indicator for whether the trip occurred on a weekend		|					
| weather_data 			| Weather conditions at the time of pickup (if available)	|						

The dataset enables the development of machine learning models to predict NYC taxi trip durations, helping to optimize ride-sharing services, estimate arrival times, and improve urban traffic management.

You can download the dataset and place it in the data folder.

## 🌐 Architecture Overview

The system comprises four main components—**Data**, **Training**, **Serving**, and **Observability**—alongside a **Dev Environment** and a **Model Registry**.

### 1. Data Pipeline

#### 📤 Data Sources

- **Kafka Producer**: Continuously emits user behavior events to `tracking.raw_data` topic
- **CDC Service**: Uses Debezium to capture PostgreSQL changes, streaming to `tracking_postgres_cdc.public.taxi_trips`

#### ✅ Schema Validation

- Validates incoming events from both sources
- Routes events to:
  - `tracking.data.validated` for valid events
  - `tracking.data.invalid` for schema violations
- Handles ~10k events/second
- Alerts invalid events to Elasticsearch
  
#### 🛒 Spark Streaming

- Transforms validated events into ML features
- Focuses on session-based metrics and purchase behavior
- Dual-writes to online/offline stores
