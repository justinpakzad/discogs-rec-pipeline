import boto3
import logging
from io import BytesIO, StringIO
import polars as pl


class S3Helper:
    def __init__(self, aws_access_key_id, aws_secret_access_key, region="us-east-1"):
        self.session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region,
        )
        self.client = self.session.client("s3")

    def list_objects(self, bucket, folder=None):
        try:
            response = (
                self.client.list_objects_v2(Bucket=bucket, Prefix=folder)
                if folder
                else self.client.list_objects_v2(Bucket=bucket)
            )
            return response.get("Contents", [])
        except Exception as e:
            logging.error(f"Error listing objects: {e}")
            return None

    def get_object(self, bucket, file_name, is_text=True):
        try:
            response = self.client.get_object(Bucket=bucket, Key=file_name)
            body = response.get("Body").read()
            if is_text:
                return StringIO(body.decode("utf-8"))
            return BytesIO(body)
        except Exception as e:
            logging.error(f"Error getting object: {e}")
            return None

    def write_object(self, bucket, file_name, data, is_object=False):
        try:
            if is_object:
                response = self.client.upload_fileobj(
                    Bucket=bucket, Key=file_name, Fileobj=data
                )
            else:
                response = self.client.upload_file(
                    Bucket=bucket, Key=file_name, Body=data
                )
            return response
        except Exception as e:
            logging.error(f"Error writing object: {e}")
            return None

    def delete_object(self, bucket, file_name):
        try:
            response = self.client.delete_object(Bucket=bucket, Key=file_name)
            return response
        except Exception as e:
            logging.error(f"Error writing object: {e}")
            return None

    def tag_object(self, bucket, file_name, tags_dict):
        try:
            response = self.client.put_object_tagging(
                Bucket=bucket,
                Key=file_name,
                Tagging={"TagSet": [tags_dict]},
            )
            return response
        except Exception as e:
            logging.error(f"Error writing object: {e}")
            return None

    def get_object_tagging(self, bucket, file_name):
        try:
            response = self.client.get_object_tagging(
                Bucket=bucket,
                Key=file_name,
            )
            return response
        except Exception as e:
            logging.error(f"Error writing object: {e}")
            return None


class S3SchemaReader:
    def __init__(self, s3_helper=None, **kwargs):
        if s3_helper:
            self.s3 = s3_helper
        else:
            self.s3 = S3Helper(kwargs)

    def read_schema_parquet(self, bucket, file_name):
        response_buffer = self.s3.get_object(
            bucket=bucket, file_name=file_name, is_text=False
        )
        df = pl.read_parquet(response_buffer).sample(100)
        return dict(df.schema)

    def read_schema_csv(self, bucket, file_name):
        response_buffer = self.s3.get_object(
            bucket=bucket, file_name=file_name, is_text=True
        )
        df = pl.read_csv(response_buffer)
        return dict(df.schema).sample(100)

    def read_schema_json(self, bucket, file_name):
        response_buffer = self.s3.get_object(
            bucket=bucket, file_name=file_name, is_text=True
        )
        df = pl.read_json(response_buffer)
        return dict(df.schema).sample(100)
