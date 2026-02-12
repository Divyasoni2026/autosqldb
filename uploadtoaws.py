#uploading the csv file to the aws bucket

import boto3

s3=boto3.client('s3')

local_file=r"D:\internship\awsauto\data.csv"
bucket_name="csvautodata"

file_name="uploadeddata/data.csv"

try:
    s3.upload_file(local_file,bucket_name,file_name)
    print("data uploaded to s3 bucket!!")
except Exception as e:
    print("Error",e)
    