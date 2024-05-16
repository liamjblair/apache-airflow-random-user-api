import pandas as pd
import requests
import json
import logger
import boto3
from botocore.exceptions import ClientError
import constants
from datetime import datetime

def main():

    docker_container_path = "/usr/local/airflow/output/"
    date_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_name = f"output_{date_time}.csv"
    file = docker_container_path + file_name

    with open(file, "w", encoding="utf-8") as f:

        f.write("Gender,FirstName,LastName,Location,Email,Username,DoB,Age,Phone,Nationality\n")

        for _ in range(100):
            try:
                person = requests.get("https://randomuser.me/api").text
                data = json.loads(person)
                results = data["results"]
            except requests.exceptions.RequestException as e:
                logger.logger.error(f"Error accessing API {e}.")

            try:
                for result in results:
                    f.write(f"{result['gender']},"
                            f"{result['name']['first']},"
                            f"{result['name']['last']},"
                            f"{result['location']['city']},"
                            f"{result['email']},"
                            f"{result['login']['username']},"
                            f"{result['dob']['date']},"
                            f"{result['dob']['age']},"
                            f"{result['phone']},"
                            f"{result['nat']}\n"
                        )

            except Exception as e:
                logger.logger.error(f"Error pasring API results - {e} - {result}")

        logger.logger.info("User data successfuly loaded.")

    load_to_s3(file, file_name)        

def load_to_s3(file, file_name):
    try:
        s3 = boto3.client("s3", 
                          aws_access_key_id=constants.AWS_S3_KEY, 
                          aws_secret_access_key=constants.AWS_S3_SECRET
                          )
        s3_bucket = constants.AWS_S3_BUCKET

        s3.upload_file(file, s3_bucket, file_name)
    except FileNotFoundError as e:
        logger.logger.error(f"File {file} not found: {e}")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'AccessDenied':
            logger.logger.error("Access denied. Check your AWS credentials and permissions.")
        elif error_code == 'NoSuchBucket':
            logger.logger.error("The specified bucket does not exist.")
        else:
            logger.logger.error(f"An error occurred: {e}")
    except Exception as e:
        logger.logger.error(f"An unexpected error occurred: {e}")
    