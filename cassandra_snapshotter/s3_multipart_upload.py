import os
import boto3
import logging
from botocore.exceptions import ClientError
from cassandra_snapshotter.utils import compressed_pipe
from cassandra_snapshotter import logging_helper

class S3MultipartUpload(object):
  # AWS throws EntityTooSmall error for parts smaller than 5 MB
  PART_MINIMUM = int(5e6)

  def __init__(self,
               bucket,
               key,
               local_path,
               logger,
               aws_access_key_id, 
               aws_secret_access_key,
               storage_class,
               part_size=int(15e6),
               region_name="us-east-1",
               metadata={},
               verbose=False):
    self.bucket = bucket
    self.key = key
    self.path = local_path
    self.logger=logger
    self.total_bytes = os.stat(local_path).st_size
    self.part_bytes = part_size
    self.region = region_name
    self.metadata = metadata
    self.storage_class = storage_class
    #assert part_size > self.PART_MINIMUM
    #assert (self.total_bytes % part_size == 0
    #        or self.total_bytes % part_size > self.PART_MINIMUM)
    self.s3 = boto3.session.Session(
        region_name=region_name,aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key).client("s3")
    if verbose:
      boto3.set_stream_logger(name="boto")
    else:
      boto3.set_stream_logger('boto', logging.WARNING)

  def abort_all(self):
    mpus = self.s3.list_multipart_uploads(Bucket=self.bucket)
    aborted = []
    self.logger.error("Aborting %d uploads"  % (len(mpus)))
    if "Uploads" in mpus:
      for u in mpus["Uploads"]:
        upload_id = u["UploadId"]
        aborted.append(
            self.s3.abort_multipart_upload(
                Bucket=self.bucket, Key=self.key, UploadId=upload_id))
    return aborted

  def create(self):
    mpu = self.s3.create_multipart_upload(Bucket=self.bucket, Key=self.key, Metadata=self.metadata, StorageClass=self.storage_class)
    mpu_id = mpu["UploadId"]
    return mpu_id

  def upload(self, mpu_id, bufsize, rate_limit, quiet):
    parts = []
    uploaded_bytes = 0
    for i, chunk in enumerate(compressed_pipe(self.path, bufsize, rate_limit, quiet)):
        i+=1
        part = self.s3.upload_part(
            Body=chunk, Bucket=self.bucket, Key=self.key, UploadId=mpu_id, PartNumber=i)
        parts.append({"PartNumber": i, "ETag": part["ETag"]})
        #uploaded_bytes += bufsize
        #self.logger.info("{0} of {1} uploaded ({2:.3f}%)".format(
        #    uploaded_bytes, self.total_bytes,
        #    as_percent(uploaded_bytes, self.total_bytes)))
    return parts

  def complete(self, mpu_id, parts):
    result = self.s3.complete_multipart_upload(
        Bucket=self.bucket,
        Key=self.key,
        UploadId=mpu_id,
        MultipartUpload={"Parts": parts})
    return result


# Helper
def as_percent(num, denom):
  return float(num) / float(denom) * 100.0
