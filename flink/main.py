import argparse
from typing import Dict, Type
import logging

from dotenv import find_dotenv, load_dotenv
from pyflink.datastream import StreamExecutionEnvironment

from .base import FlinkJob

load_dotenv(find_dotenv())

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def get_available_jobs() -> Dict[str, Type[FlinkJob]]:
    """Return a dictionary of available jobs with lazy loading"""
    return {
        "schema_validation": lambda: __import__(
            "flink.schema_validation_job", fromlist=["SchemaValidationJob"]
        ).SchemaValidationJob,
        "alert_invalid_events": lambda: __import__(
            "flink.alert_invalid_events_job",
            fromlist=["AlertInvalidEventsJob"],
        ).AlertInvalidEventsJob,
    }


def get_job_class(job_name: str) -> Type[FlinkJob]:
    """Get the job class, loading it only when requested"""
    jobs = get_available_jobs()
    if job_name not in jobs:
        raise ValueError(f"Unknown job: {job_name}")
    return jobs[job_name]()


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run a Flink job")
    parser.add_argument(
        "job_name", choices=get_available_jobs().keys(), help="Name of the job to run"
    )
    args = parser.parse_args()

    try:
        # Get the job class and create an instance
        job_class = get_job_class(args.job_name)
        job = job_class()

        # Create and execute the pipeline
        env = StreamExecutionEnvironment.get_execution_environment()
        job.create_pipeline(env)
        env.execute(f"{job.job_name} Pipeline")
        logger.info(f"Job {job.job_name} has been started successfully!")
    except Exception as e:
        logger.error(f"Error running job {args.job_name}: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
