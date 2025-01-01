import os
import json
from contextlib import contextmanager
from datetime import datetime
from typing import Union
import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from minio import Minio


@contextmanager
def connect_minio(config):
    client = Minio(
        endpoint=config.get("endpoint_url"),
        access_key=config.get("aws_access_key_id"),
        secret_key=config.get("aws_secret_access_key"),
        secure=False,
    )
    try:
        yield client
    except Exception:
        raise


class MinIOIOManager(IOManager):
    def __init__(self, config):
        self._config = config
    def _get_path(self, context: Union[InputContext, OutputContext]):
        layer, schema, table = context.asset_key.path
        key = "/".join([layer, schema, table.replace(f"{layer}_", "")])
        tmp_file_path = "/tmp/file-{}-{}.parquet".format(
            datetime.today().strftime("%Y%m%d%H%M%S"), "-".join(context.asset_key.path))
        return f"{key}.parquet", tmp_file_path


    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        key_name, tmp_file_path = self._get_path(context)
        obj.to_parquet(tmp_file_path, engine = "pyarrow")
        # obj.to_csv(tmp_file_path, index = False)
        try:
            with connect_minio(self._config) as client:
                client.fput_object(self._config["bucket"], key_name, tmp_file_path)
                os.remove(tmp_file_path)
        except Exception:
            raise


    def load_input(self, context: InputContext) -> pd.DataFrame:
        key_name, tmp_file_path = self._get_path(context)
        try:
            with connect_minio(self._config) as client:
                client.fget_object(self._config["bucket"], key_name, tmp_file_path)
                df = pd.read_parquet(tmp_file_path, engine="pyarrow")
                # df = pd.read_csv(tmp_file_path)

                os.remove(tmp_file_path)
            return df
        except Exception:
            raise 

class MinIOHandler:

    def __init__(self, minio_config, tmp_dir = "./tmp", root_dir = "bronze/tiki"):
        self.minio_config = minio_config
        self.tmp_dir = tmp_dir
        self.root_dir = root_dir
        
    def put_file_to_minio(self, file, path, file_type="json"):
        try:
            tmp_file_path = os.path.join(self.tmp_dir, path)
            os.makedirs(os.path.dirname(tmp_file_path), exist_ok=True)
            if file_type == "json":  
                with open(tmp_file_path, "w", encoding="utf-8") as f:
                    json.dump(file, f, ensure_ascii=False, indent=4)
            elif file_type == "csv":  
                file.to_csv(tmp_file_path, index=False)

            with connect_minio(self.minio_config) as client:
                minio_path = os.path.join(self.root_dir, path)
                client.fput_object(self.minio_config["bucket"], minio_path, tmp_file_path)
                print(f"Successfully uploaded data to Minio at {path}")

            os.remove(tmp_file_path)

        except Exception as e:
            print(f"Failed to upload data to Minio at {path}: {e}")
            raise

    def get_file_from_minio(self, path, file_type="json"):
        try:
            tmp_file_path = os.path.join(self.tmp_dir, path)
            os.makedirs(os.path.dirname(tmp_file_path), exist_ok=True)

            with connect_minio(self.minio_config) as client:
                if not path.startswith(self.root_dir):
                    minio_path = os.path.join(self.root_dir, path)
                else:
                    minio_path = path

                client.fget_object(self.minio_config["bucket"], minio_path, tmp_file_path)
                
                if file_type == "json":
                    with open(tmp_file_path, "r", encoding="utf-8") as f:
                        data = json.load(f)  
                elif file_type == "csv":
                    data = pd.read_csv(tmp_file_path)  

                else:
                    raise ValueError(f"Unsupported file type: {file_type}")

                os.remove(tmp_file_path)
                return data  
            
        except Exception as e:
            print(f"Failed to get file from Minio at {path}: {e}")
            raise

    def get_parquet_from_minio(self, path, file_type):
        try:
            tmp_file_path = os.path.join(self.tmp_dir, path)
            os.makedirs(os.path.dirname(tmp_file_path), exist_ok=True)

            with connect_minio(self.minio_config) as client:
                if not path.startswith(self.root_dir):
                    minio_path = os.path.join(self.root_dir, path)
                else:
                    minio_path = path

                client.fget_object(self.minio_config["bucket"], minio_path, tmp_file_path)
                
                if file_type == "json":
                    with open(tmp_file_path, "r", encoding="utf-8") as f:
                        data = json.load(f)  
                elif file_type == "csv":
                    data = pd.read_parquet(tmp_file_path, engine= "pyarrow")  

                else:
                    raise ValueError(f"Unsupported file type: {file_type}")

                os.remove(tmp_file_path)
                return data  
            
        except Exception as e:
            print(f"Failed to get file from Minio at {path}: {e}")
            raise
