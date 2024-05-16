import dotenv
import os

dotenv.load_dotenv()

AWS_S3_KEY=os.getenv("AWS_S3_KEY")
AWS_S3_SECRET=os.getenv("AWS_S3_SECRET")
AWS_S3_BUCKET=os.getenv("AWS_S3_BUCKET")