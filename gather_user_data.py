import requests
import phonenumbers
import pandas as pd
import json
from logger import logger
import boto3
from botocore.exceptions import ClientError
import constants
from datetime import datetime


class RandomUserAPI:

    def __init__(self, aws_access_key_id, aws_secret_access_key, num_users) -> None:
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.num_users = num_users

    def create_output_file(self):
        # docker_container_path = "/usr/local/airflow/output/"
        docker_container_path = "./output/"
        date_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.file_name = f"output_{date_time}.csv"
        self.file = docker_container_path + self.file_name

    def gather_random_user_date(self):
        with open(self.file, "w", encoding="utf-8") as f:

            f.write("Gender,FirstName,LastName,Location,Email,Username,DoB,DoB_Date,DoB_Time,Age,Phone,StandarisedPhonenumber,Nationality\n")

            for _ in range(self.num_users):
                try:
                    person = requests.get("https://randomuser.me/api").text
                    data = json.loads(person)
                    results = data["results"]
                except requests.exceptions.RequestException as e:
                    logger.error(f"Error accessing API {e}.")

                for result in results:
                    self.write_user_data_results(f, result)

    def write_user_data_results(self, file, result):
        try:
            file.write(f"{result['gender']},"
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
            logger.error(f"Error writing results to output file - {e}")

    def data_transformations(self):
        df = pd.read_csv(self.file)

        # format date of birth
        df['dob_date'] = df['DoB'].str.extract(r'^(\d{4}-\d{2}-\d{2})')
        df['dob_time'] = df['DoB'].str.extract(r'(\d{4}:\d{2}:\f{2})')

        # standarise the phones numbers by removing spaces, parentheise and adding country codes
        def standardise_phone_numbers(number, country_code):
            try:
                parsed_number = phonenumbers.parse(number, country_code)
                if phonenumbers.is_valid_number(parsed_number):
                    return phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.E164)
                else:
                    return number
            except phonenumbers.phonenumberutil.NumberParseException as e:
                logger.error(f"Invalid phone number, unable to parse to a standarised format - {e}")

        df['StandardisedPhoneNumber'] = df.apply(lambda row: standardise_phone_numbers(row['Phone'], row['Nationality']), axis=1)

        # assign new column order
        column_order = [
            'FirstName', 'LastName', 'Gender', 'DoB', 'DoB_Date',
            'DoD_Time', 'Age','Phone', 'StandardisedPhoneNumber',
             'Email', 'Username', 'Location', 'Nationality'
        ]

        df = df[column_order]
        df.to_csv(self.file, index=False)

    def load_to_s3(self):
        try:
            s3 = boto3.client("s3", 
                            aws_access_key_id=self.aws_access_key_id, 
                            aws_secret_access_key=self.aws_secret_access_key
                            )
            s3_bucket = constants.AWS_S3_BUCKET

            s3.upload_file(self.file, s3_bucket, self.file_name)
        except FileNotFoundError as e:
            logger.error(f"File {self.file} not found: {e}")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'AccessDenied':
                logger.error("Access denied. Check your AWS credentials and permissions.")
            elif error_code == 'NoSuchBucket':
                logger.error("The specified bucket does not exist.")
            else:
                logger.logger.error(f"An error occurred: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
        

