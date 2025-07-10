# How to Access NLDAS-3 Forcing Dataset Via AWS CLI

## 1. Install AWS CLI  
Ensure you have the AWS CLI installed. You can check by running the following command in the terminal:  
aws --version

If the AWS CLI is not installed, follow the AWS CLI installation guide.

## 2. Access to the S3 Bucket

The nasa-waterinsight bucket allows public read access.

## 3. Downloading Files from the S3 Bucket

##### List Files in the S3 Folder
First, check the available files using the following command:

    aws s3 ls s3://nasa-waterinsight/NLDAS3/forcing/hourly/201501/ --no-sign-request

You'll see an output like:

    2025-01-01 12:00:00    1234567  NLDAS_FOR0010_H.A20150131.030.beta.nc
This confirms that .nc files exist in the folder.

##### Download a Specific File
Use the following command to download a specific file:

    aws s3 cp s3://nasa-waterinsight/NLDAS3/forcing/hourly/201501/NLDAS_FOR0010_H.A20150131.030.beta.nc . --no-sign-request

    This will save NLDAS_FOR0010_H.A20150131.030.beta.nc in your current directory.

If you want to download the file to a specific location (e.g., /Volumes/Personal/NLDAS3):

    aws s3 cp s3://nasa-waterinsight/NLDAS3/forcing/hourly/201501/NLDAS_FOR0010_H.A20150131.030.beta.nc /Volumes/Personal/NLDAS3 --no-sign-request

##### Download All Files in the Folder
If you want to download all files in the folder, use the --recursive flag, which ensures that all files and subdirectories inside 201501/ are copied:

    aws s3 cp s3://nasa-waterinsight/NLDAS3/forcing/hourly/201501/ ./ --recursive --no-sign-request

    This will download all files from 201501/ to your current directory.

To download all files to a specific directory (e.g., /Volumes/Personal/NLDAS3):

    aws s3 cp s3://nasa-waterinsight/NLDAS3/forcing/hourly/201501/ /Volumes/Personal/NLDAS3 --recursive --no-sign-request

# NLDAS-3 Forcing Data Feedback Form
This is a testing and feedback form for the NASA NLDAS-3 forcing data that will be conducted from Spring to early Fall 2025. This evaluation should take about 5-10 minutes.

[Feedback Form](https://docs.google.com/forms/d/e/1FAIpQLScL_LNT-YtKjQWiXqNOLak4JFxd5ETWvA3P7KzxK_WSE1swGg/viewform)

Thanks for your time and sharing your ideas!    

##### Now, you're all set to access and download files from the NLDAS-3 Forcing Dataset using AWS CLI!
