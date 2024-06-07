
from gather_user_data import RandomUserAPI
import constants

def run():
    aws_access_key_id=constants.AWS_S3_KEY
    aws_secret_access_key=constants.AWS_S3_SECRET
    num_users = 100

    user_data = RandomUserAPI(aws_access_key_id, aws_secret_access_key, num_users)
    user_data.gather_random_user_data()
    user_data.data_transformations()
    user_data.load_to_s3()

