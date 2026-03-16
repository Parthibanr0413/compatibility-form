import argparse
import configparser
import hashlib
import os
import pathlib
import shutil
import subprocess

import boto3
import psycopg2
from aws_secretmanager_utils.aws_secretmanager import SecretsManager
from logger import MIALogger

_UPLOAD_COMPLETE = "Upload Complete"
_UPLOAD_FAILED = "Upload Failed"
_NO_HASH_VALIDATION = "NA"


def _compute_md5(filepath):
    """Return the MD5 hex digest of a file using chunked reads."""
    md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            md5.update(chunk)
    return md5.hexdigest()


def _remove_temp_dir(path, logger):
    """Remove a temporary directory tree if it exists."""
    temp_dir = path.rsplit("/", 2)[0]
    if os.path.exists(temp_dir):
        logger.info(f"Removing temp path {temp_dir}")
        shutil.rmtree(temp_dir)


def upload_image(s3_endpoint, local_filename, aws_filename, aws_access_key,
                 aws_secret_key, aws_bucket):
    # verify=False is intentional for private/self-signed S3 endpoint certificates.
    s3 = boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        verify=False,
        region_name="us-east-1",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
    )
    s3.upload_file(local_filename, aws_bucket, aws_filename)
    return True


def init_s3(s3_endpoint, region, aws_access_key, aws_secret_key, aws_session_token,
            logger):
    try:
        if aws_access_key and aws_secret_key and aws_session_token:
            # verify=False is intentional for private/self-signed S3 endpoint certificates.
            s3_client = boto3.client(
                "s3",
                endpoint_url=s3_endpoint,
                verify=False,
                region_name=region,
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                aws_session_token=aws_session_token,
            )
            logger.info("Connected S3 via Access and Session Token")
        elif aws_access_key and aws_secret_key:
            s3_client = boto3.client(
                "s3",
                region_name=region,
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
            )
            logger.info("Connected S3 via Access Keys")
        else:
            s3_client = boto3.client("s3")
            logger.info("Connected S3 directly")
        return s3_client
    except Exception as e:
        logger.error(f"Error on Connecting S3 Client: {e}")
        raise


def mrxs_download(s3_client, aws_bucket, image_name, expected_hash, source_s3_path,
                  concentriq_landing_path, concentriq_base_path, concentriq_temp_path,
                  concentriq_s3_endpoint, concentriq_aws_access_key,
                  concentriq_aws_secret_key, concentriq_aws_bucket, local_destination,
                  logger):
    if concentriq_landing_path is None:
        logger.info(
            "concentriq landing path is missing for the image in the master image "
            "tracking table"
        )
        return _UPLOAD_FAILED

    landing_dir = concentriq_landing_path.rsplit("/", 1)[0]
    destination = os.path.join(concentriq_base_path, landing_dir) + "/"
    local_filename = os.path.join(local_destination, os.path.basename(image_name).replace(".mrxs", ".zip"))
    aws_filename = landing_dir + "/" + os.path.basename(image_name).replace(".mrxs", ".zip")
    download_path = os.path.join(concentriq_temp_path, image_name.rsplit(".", 1)[0]) + "/"

    logger.info(f"concentriq base path: {destination}")

    def _upload_and_remove():
        upload_image(
            concentriq_s3_endpoint, local_filename, aws_filename,
            concentriq_aws_access_key, concentriq_aws_secret_key, concentriq_aws_bucket,
        )
        logger.info(
            f"Concentriq File: '{local_filename}' upload finished into aws location: "
            f"{aws_filename}"
        )
        os.remove(local_filename)

    if pathlib.Path(local_filename).exists():
        logger.info(
            f"{image_name} related files already present in this directory: {destination}"
        )
        _remove_temp_dir(download_path, logger)
        try:
            logger.info("Upload starting")
            _upload_and_remove()
        except Exception as e:
            logger.error(f"Issue while uploading image to Concentriq s3: {e}")
            return _UPLOAD_FAILED
        return _UPLOAD_COMPLETE

    os.makedirs(download_path, exist_ok=True)

    s3_objects = s3_client.list_objects(
        Bucket=aws_bucket, Prefix=source_s3_path.rsplit(".", 1)[0] + "/"
    )
    image_objects = [
        obj["Key"]
        for obj in s3_objects.get("Contents", [])
        if not obj["Key"].endswith("/")
    ]
    logger.info(f"image_objects: {image_objects}")

    try:
        if not image_objects:
            logger.info(f"No Related files for the image {image_name}")
            return _UPLOAD_FAILED

        logger.info(f"{image_name} related files have started to download in {download_path}")
        for object_path in image_objects:
            dest_file = os.path.join(download_path, object_path.rsplit("/", 1)[1])
            logger.info(f"Object_path: {object_path}")
            logger.info(f"Filename: {dest_file}")
            s3_client.download_file(Bucket=aws_bucket, Key=object_path, Filename=dest_file)
        logger.info(f"{image_name} related files download is completed")

        logger.info(f"{image_name} has started to download in {concentriq_temp_path}")
        s3_client.download_file(
            Bucket=aws_bucket,
            Key=source_s3_path,
            Filename=os.path.join(concentriq_temp_path, image_name),
        )
        logger.info(f"{image_name} downloaded successfully.")
    except Exception as e:
        logger.info(f"Download failed with Exception: {e}")
        return _UPLOAD_FAILED

    os.makedirs(destination, exist_ok=True)

    try:
        mrxs_file = os.path.join(concentriq_temp_path, image_name)
        local_hash = _compute_md5(mrxs_file)

        if expected_hash != _NO_HASH_VALIDATION and local_hash != expected_hash:
            logger.error(f"{image_name} download failed: hash mismatch")
            _remove_temp_dir(download_path, logger)
            return _UPLOAD_FAILED

        zip_output = os.path.join(local_destination, os.path.basename(image_name).replace(".mrxs", ".zip"))
        zipcmd = ["/bin/7z", "a", "-mm=Copy", "-r", zip_output, mrxs_file, download_path]
        logger.info(
            f"Creating zip file {os.path.basename(zip_output)} in {local_destination}"
        )
        result = subprocess.run(zipcmd)
        if result.returncode != 0:
            logger.error(f"7z command failed with return code {result.returncode}")
            _remove_temp_dir(download_path, logger)
            return _UPLOAD_FAILED

        try:
            _upload_and_remove()
        except Exception as e:
            logger.error(f"Issue while uploading image to Concentriq s3: {e}")
            return _UPLOAD_FAILED

        _remove_temp_dir(download_path, logger)
        return _UPLOAD_COMPLETE
    except Exception as e:
        logger.error(f"Exception: {e}")
        _remove_temp_dir(download_path, logger)
        return _UPLOAD_FAILED


def s3_download(s3_client, aws_bucket, image_name, expected_hash, source_s3_path,
                concentriq_landing_path, concentriq_base_path, logger):
    if concentriq_landing_path is None:
        destination = os.path.join(concentriq_base_path, "concentriq_temp") + "/"
    else:
        destination = (
            os.path.join(concentriq_base_path, concentriq_landing_path.rsplit("/", 1)[0])
            + "/"
        )

    logger.info(f"concentriq base path: {destination}")
    os.makedirs(destination, exist_ok=True)

    dest_file = os.path.join(destination, image_name)
    if pathlib.Path(dest_file).exists():
        logger.info(f"{image_name} already present in this directory: {destination}")
        return _UPLOAD_FAILED

    try:
        logger.info(f"{image_name} has started to download in {destination}")
        s3_client.download_file(Bucket=aws_bucket, Key=source_s3_path, Filename=dest_file)
        local_hash = _compute_md5(dest_file)
        if local_hash == expected_hash:
            logger.info(f"{image_name} downloaded successfully.")
            return _UPLOAD_COMPLETE
        else:
            logger.error(f"{image_name} download failed: hash mismatch")
            return _UPLOAD_FAILED
    except Exception as e:
        logger.error(f"Exception while downloading: {e}")
        return _UPLOAD_FAILED


def _update_image_status(cur, conn, status, manifest_file, image_name, logger):
    query = """
        UPDATE rdap.master_image_tracking_table
        SET image_status = %s
        WHERE manifest_file = %s
          AND image_name = %s
          AND image_status IN ('Uploading')
    """
    cur.execute(query, (status, manifest_file, image_name))
    conn.commit()
    logger.info(f"master image table upload status: {status}")


def main():
    parser = argparse.ArgumentParser(description="Pathology Image file DQ Checker")
    parser.add_argument(
        "--config_file", dest="config_file", required=True,
        help="Path to the configuration file",
    )
    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read(args.config_file)

    region = config["s3"]["region"]
    region_value = config["s3"]["region_name"]
    aws_bucket = config["s3"]["aws_bucket"]
    s3_endpoint = config["s3"]["endpoint"]
    aws_session_token = config["s3"]["aws_session_token"]
    log_path = config["general"]["log_file_path"]

    logger = MIALogger(
        logfile=os.path.join(log_path, "pathoUploadtoconcentriq.log"), loglevel="INFO"
    )

    concentriq_base_path = config["general"]["concentriq_base_path"]
    concentriq_temp_path = config["general"]["concentriq_temp_path"]
    local_destination = config["general"]["local_destination"]
    concentriq_s3_endpoint = config["s3"]["concentriq_s3_endpoint"]
    concentriq_aws_bucket = config["s3"]["concentriq_aws_bucket"]

    s3_secret = SecretsManager.get_secret("rdap_bronze_aws_s3", region_name=region_value)
    aws_access_key_id = s3_secret["AWS_ACCESS_KEY_ID"]
    aws_secret_access_key = s3_secret["AWS_SECRET_ACCESS_KEY"]

    concentriq_s3_secret = SecretsManager.get_secret(
        "concentriq_aws_access_s3", region_name=region_value
    )
    concentriq_aws_access_key = concentriq_s3_secret["concentriq_aws_access_key"]
    concentriq_aws_secret_key = concentriq_s3_secret["concentriq_aws_secret_key"]

    secret_data = SecretsManager.get_secret("iics_db", region_name=region_value)
    conn = psycopg2.connect(
        host=secret_data["host"],
        database=secret_data["dbname"],
        user=secret_data["username"],
        password=secret_data["password"],
        port=secret_data.get("port", 5432),
    )

    if not os.path.exists(concentriq_base_path):
        logger.info("Concentriq base path is not available")
        return

    select_query = """
        SELECT DISTINCT image_name, source_s3_path, hash, manifest_file,
               concentriq_landing_path
        FROM rdap.master_image_tracking_table
        WHERE image_status IN ('Ready to Pickup', 'Upload Failed', 'Uploading')
    """

    with conn:
        with conn.cursor() as cur:
            logger.info(f"Postgresql Connection Successful: server - {secret_data['host']}")
            logger.info(f"Querying master image table: {select_query}")
            cur.execute(select_query)
            rds_data = cur.fetchall()

            if not rds_data:
                logger.info("There is no Image to download")
                return

            s3_client = init_s3(
                s3_endpoint, region, aws_access_key_id, aws_secret_access_key,
                aws_session_token, logger,
            )

            mark_uploading = """
                UPDATE rdap.master_image_tracking_table
                SET image_status = 'Uploading'
                WHERE manifest_file = %s
                  AND image_name = %s
                  AND image_status IN ('Ready to Pickup', 'Upload Failed')
            """

            for n, (image_name, source_s3_path, expected_hash, manifest_file,
                    concentriq_landing_path) in enumerate(rds_data):
                if n != 0:
                    logger.info("")
                logger.info(f"Image number {n + 1}: {image_name}")

                cur.execute(mark_uploading, (manifest_file, image_name))
                conn.commit()

                if image_name.endswith(".mrxs") or image_name.endswith(".zip"):
                    status = mrxs_download(
                        s3_client, aws_bucket, image_name, expected_hash,
                        source_s3_path, concentriq_landing_path, concentriq_base_path,
                        concentriq_temp_path, concentriq_s3_endpoint,
                        concentriq_aws_access_key, concentriq_aws_secret_key,
                        concentriq_aws_bucket, local_destination, logger,
                    )
                else:
                    status = s3_download(
                        s3_client, aws_bucket, image_name, expected_hash,
                        source_s3_path, concentriq_landing_path, concentriq_base_path,
                        logger,
                    )

                _update_image_status(cur, conn, status, manifest_file, image_name, logger)


if __name__ == "__main__":
    main()
