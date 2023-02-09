import streamlit as st
import pandas as pd
import numpy as np
import json
import streamlit.components.v1 as components
import os
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.errorfactory import ClientError # checking if file exists already
import string
import webbrowser
from decouple import config
# Import class from dbUtil
import dbUtil
# Import AWS Logging
from aws_logging import write_logs
from dbUtil import *

util = DbUtil("metadata.db")
########################################################################################################################
# AWS Destination Credentials:
aws_access_key_id = config('aws_access_key_id')
aws_secret_access_key = config('aws_secret_access_key')
# AWS CloudWatch Credentials:
# log_aws_access_key_id = config('log_aws_access_key_id')
# log_aws_secret_access_key = config('log_aws_secret_access_key')
# Destination S3 Directory:
dest_bucket = 'damg7245'
dest_folder = 'assignment1'
########################################################################################################################

st.title('SEVIR Data Fetcher')

# CREATE TABLE:
# util = DbUtil('metadata1.db')
# column_names = ['id INTEGER PRIMARY KEY', 'product TEXT', 'year TEXT', 'day_of_year TEXT', 'hour TEXT']
# util.create_table('geos18', *column_names)

# SELECT DATASOURCE:
data_source = st.selectbox('Data Source: ', ['GOES-18 geostationary satellite', 'NEXRAD weather radars'])

# Search Method:
search_method = st.selectbox('Search Method: ', ['File Name', 'Field Selection'])

if data_source == 'GOES-18 geostationary satellite':
    BUCKET_NAME = 'noaa-goes18'
    core_url = 'https://noaa-goes18.s3.amazonaws.com/{product_fn}/{year}/{day_of_year}/{hour}/{file_name}'
    metadata = ['Product', 'Year', 'Day of Year', 'Hour', 'File Name']
    if search_method == 'Field Selection':
        st.markdown(f"""
    URL Format: {core_url}\n
    Required Fields:
    - Product
    - Year
    - Day of Year
    - Hour
    - File Name
    """)
elif data_source == 'NEXRAD weather radars':
    BUCKET_NAME = 'noaa-nexrad-level2'
    core_url = 'https://noaa-nexrad-level2.s3.amazonaws.com/{year}/{month}/{day}/{nexrad_station}/{file_name}'
    metadata = ['Year', 'Month', 'Day', 'NEXRAD Station', 'File Name']
    if search_method == 'Field Selection':
        st.markdown(f"""
URL Format: {core_url}\n
Required Fields:
- Year
- Month
- Day
- NEXRAD Station
- File Name
""")



first_level = []
year = []
months = []
days = []
files = []
files_selected = ""

first_level.append("")
year.append("")
months.append("")
days.append("")
files.append("")


# def find_url_from_filename(filename: str) -> str:
#   file = filename.split("_")

#   prefix = "https://noaa-goes16.s3.amazonaws.com/"
#   delim = "/"
#   prod = file[1][:file[1].rindex("-")-1]
#   prod = prod.rstrip(string.digits)
#   year =  file[3][1:5]
#   month =  file[3][5:8]
#   date =  file[3][8:10]

#   return prefix + prod + delim + year + delim + month + delim + date + delim + filename


def filename_url_producer(bucket_name, file_name):
    pieces = file_name.split("_")
    if 'nexrad' in bucket_name: 
        print(pieces)
        nexrad_station = pieces[0][0:4]
        year = pieces[0][4:8]
        month = pieces[0][8:10]
        day = pieces[0][10:12]
        time = pieces[1]
        core_url = f'https://noaa-nexrad-level2.s3.amazonaws.com/{year}/{month}/{day}/{nexrad_station}/{file_name}'
    if 'noaa-goes' in bucket_name:
        print(pieces)
        product = pieces[1]
        product_fn = product[:product.rindex('-')]
        year = pieces[3][1:5]
        day_of_year = pieces[3][5:8]
        hour = pieces[3][8:10]
        core_url = f'https://noaa-goes18.s3.amazonaws.com/{product_fn}/{year}/{day_of_year}/{hour}/{file_name}'
    
    print(core_url)
    return core_url 


# SEARCH BY FILENAME:
if search_method == 'File Name':
    st.write('Search by Filename:')
    file_name_input = st.text_input('File Name:')
    # TODO: Need rules on what is a valid file_name input
    if file_name_input:
        try:
            url = filename_url_producer(BUCKET_NAME, file_name_input)
        except:
            error = '<p style="font-family:sans-serif; color:Red; font-size: 20px;">File Name Error!</p>'
            st.markdown(error, unsafe_allow_html=True)


# BUCKET_NAME = 'noaa-goes16'
# BUCKET_FILE_NAME = 'ABI-L1b-RadC/2023/003/02/OR_ABI-L1b-RadC-M6C01_G16_s20230030206174_e20230030208551_c20230030208598.nc'
# LOCAL_FILE_NAME = 'OR_ABI-L1b-RadC-M6C01_G16_s20230030206174_e20230030208551_c20230030208598.nc'

s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
# s3.download_file(BUCKET_NAME, BUCKET_FILE_NAME, LOCAL_FILE_NAME)

# Download Entire Folder
def download_s3_folder(bucket_name, s3_folder, local_dir=None):
    bucket = s3.Bucket(bucket_name)
    # TODO: Only first 5 files for now
    counter = 0
    for obj in bucket.objects.filter(Prefix=s3_folder):
        if counter == 5:
            break
        counter += 1
        target = obj.key if local_dir is None \
            else os.path.join(local_dir, os.path.relpath(obj.key, s3_folder))
        if not os.path.exists(os.path.dirname(target)):
            os.makedirs(os.path.dirname(target))
        if obj.key[-1] == '/':
            continue
        bucket.download_file(obj.key, target)


# COPY S3 FILE TO DESTINATION:
def copy_file_to_dest_s3(src_bucket, dest_bucket, dest_folder, prefix, files_selected):
    # Get S3 File:
    s3_src = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    
    src_response = s3_src.get_object(Bucket=src_bucket, Key=prefix+files_selected)


    # Upload S3 to Destination:
    s3_dest = boto3.client('s3',
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)
    
    dest_file_name = f'{dest_folder}/{src_bucket}/{files_selected}'

    # Check if file has already beeen transferred
    try:
        s3_dest.head_object(Bucket='damg7245', Key=dest_file_name)
        # st.write("""File Transfer Error: File already exists!""")
        error = '<p style="font-family:sans-serif; color:Red; font-size: 20px;">File Transfer Error: File already exists!</p>'
        st.markdown(error, unsafe_allow_html=True)
        # dest_url = """File Transfer Error: File already exists!"""
    except ClientError:
        # Not found
        print("""File doesn't exist""")

        test = s3_dest.upload_fileobj(src_response['Body'], dest_bucket, dest_file_name)
        
    dest_url = f'https://{dest_bucket}.s3.amazonaws.com/{dest_file_name}'
    # print(f'Destination s3 URL: {dest_url}')
    
    return dest_url


if search_method == 'Field Selection' and data_source == 'GOES-18 geostationary satellite':
    result = s3.list_objects_v2(Bucket=BUCKET_NAME, Delimiter="/")
    folders = [fld["Prefix"] for fld in result["CommonPrefixes"]]

    for folder in folders:
        first_level.append(folder)

    st.write("Selected Product is ABI-L1b-RadC")

    year_list =util.filter("geos18", 'year', product='ABI-L1b-RadC')
    year_list.insert(0, "")
    year_selected = st.selectbox(
    f'Please select {metadata[1]}', year_list)

    if year_selected:      

        day_list =util.filter("geos18", 'day_of_year', product='ABI-L1b-RadC', year=year_selected)
        day_list.insert(0, "")
        day_selected = st.selectbox(
        f'Please select {metadata[2]}', day_list)

        if day_selected:      
                                                                                   #months selected
            hour_list =util.filter("geos18", 'hour', product='ABI-L1b-RadC', year=year_selected, day_of_year=day_selected)
            hour_list.insert(0, "")
            hour_selected = st.selectbox(
            f'Please select {metadata[3]}', hour_list)

            if hour_selected:      
                                                                                    #months selected
                prefix = "ABI-L1b-RadC/"+year_selected+"/"+day_selected+"/"+hour_selected+"/"
                result = s3.list_objects(Bucket=BUCKET_NAME, Prefix=prefix, Delimiter='/')
                for item in result['Contents']:
                    file = item['Key']
                    files.append(file)

                for i in range(len(files)):
                    files[i] = files[i].replace(prefix, '')

                # Download all files in folder
                files.insert(1, 'Download All Files')

                files_selected = st.selectbox(
                f'Please select {metadata[4]}', files)

                if files_selected == 'Download All Files':
                    result = s3.list_objects(Bucket=BUCKET_NAME, Prefix=prefix, Delimiter='/')
                for item in result['Contents']:
                    file = item['Key']
                    files.append(file)

                st.write(f'Total Files Available: {len(files)}')

                if files_selected == 'Download All Files':
                    s3 = boto3.resource('s3', config=Config(signature_version=UNSIGNED))
                    # Total Folder Size
                    s3_folder = prefix
                    bytes = sum([object.size for object in s3.Bucket(BUCKET_NAME).objects.filter(Prefix=s3_folder)])
                    st.write(f'Total Folder Size: {round(bytes//1000/1024/1024, 3)} GB')

                    if st.button('Download All Files'):
                        # TODO
                        download_s3_folder(BUCKET_NAME, s3_folder, local_dir=None)

                elif files_selected:
                    url = filename_url_producer(BUCKET_NAME, files_selected)
                    st.write(url)
                    user_inputs = ["ABI-L1b-RadC", year_selected, day_selected, hour_selected, files_selected]
                    write_logs(f'User Input: {user_inputs}')
                    write_logs(f'Generated URL: {url}')

                    # TESTING
                    st.write('TEST:')
                    st.write(f'Prefix = {prefix}  \n Files_selected = {files_selected}')
                    st.text("")
                    st.text("")
                    st.text("")

                    if (search_method == 'File Name' and file_name_input) or (search_method == 'Field Selection' and files_selected):
                        col1, col2 = st.columns(2)
                        with col1:
                            if st.button('Download File'):
                                write_logs('User Action: Downloaded File Locally')
                                webbrowser.open_new_tab(url)
                                st.write('File Downloaded Locally')

                        with col2:
                            if st.button('Transfer File to S3 Bucket'):
                                dest_url = copy_file_to_dest_s3(BUCKET_NAME, dest_bucket, dest_folder, prefix, files_selected)
                                write_logs(f'User Action: Transfered file to S3 Bucket - {dest_url}')
                                if 'Error' in dest_url:
                                    st.write(dest_url)
                                st.write(f'Destination s3 URL: {dest_url}')

if search_method == 'Field Selection' and data_source == 'NEXRAD weather radars':
    BUCKET_NAME = 'noaa-nexrad-level2'
    result = s3.list_objects_v2(Bucket=BUCKET_NAME, Delimiter="/")
    folders = [fld["Prefix"] for fld in result["CommonPrefixes"]]

    for folder in folders:
        first_level.append(folder)

    st.write("Selected Year is 2023")

    month_list =util.filter("nexrad", 'month', year='2023')
    month_list.insert(0, "")
    month_selected = st.selectbox(
    f'Please select {metadata[1]}', month_list)

    if month_selected:      

        day_list =util.filter("nexrad", 'day', year='2023', month=month_selected)
        day_list.insert(0, "")
        day_selected = st.selectbox(
        f'Please select {metadata[2]}', day_list)

        if day_selected:      
                                                                                   #months selected
            station_list =util.filter("nexrad", 'station', year='2023', month=month_selected, day=day_selected)
            station_list.insert(0, "")
            station_selected = st.selectbox(
            f'Please select {metadata[3]}', station_list)

            if station_selected:      
                                                                                #months selected
                prefix = "2023/"+month_selected+"/"+day_selected+"/"+station_selected+"/"
                result = s3.list_objects(Bucket=BUCKET_NAME, Prefix=prefix, Delimiter='/')
                for item in result['Contents']:
                    file = item['Key']
                    files.append(file)

                st.write(f'Total Files Available: {len(files)}')

                for i in range(len(files)):
                    files[i] = files[i].replace(prefix, '')

                # Download all files in folder
                files.insert(1, 'Download All Files')

                files_selected = st.selectbox(
                f'Please select {metadata[4]}', files)

                if files_selected == 'Download All Files':
                    s3 = boto3.resource('s3', config=Config(signature_version=UNSIGNED))
                    # Total Folder Size
                    s3_folder = prefix
                    bytes = sum([object.size for object in s3.Bucket(BUCKET_NAME).objects.filter(Prefix=s3_folder)])
                    st.write(f'Total Folder Size: {round(bytes//1000/1024/1024, 3)} GB')

                if st.button('Download All Files'):
                    # TODO
                    download_s3_folder(BUCKET_NAME, s3_folder, local_dir=None)
                elif files_selected:
                    st.write(files_selected)
                    url = filename_url_producer(BUCKET_NAME, files_selected)
                    st.write(url)
                    user_inputs = ["2023", month_selected, day_selected, station_selected, files_selected]
                    write_logs(f'User Input: {user_inputs}')
                    write_logs(f'Generated URL: {url}')

                    # TESTING
                    st.write('TEST:')
                    st.write(f'Prefix = {prefix}  \n Files_selected = {files_selected}')
                    st.text("")
                    st.text("")
                    st.text("")

                    if (search_method == 'File Name' and file_name_input) or (search_method == 'Field Selection' and files_selected):
                        col1, col2 = st.columns(2)
                        with col1:
                            if st.button('Download File'):
                                write_logs('User Action: Downloaded File Locally')
                                webbrowser.open_new_tab(url)
                                st.write('File Downloaded Locally')

                        with col2:
                            if st.button('Transfer File to S3 Bucket'):
                                dest_url = copy_file_to_dest_s3(BUCKET_NAME, dest_bucket, dest_folder, prefix, files_selected)
                                write_logs(f'User Action: Transfered file to S3 Bucket - {dest_url}')
                                if 'Error' in dest_url:
                                    st.write(dest_url)
                                st.write(f'Destination s3 URL: {dest_url}')

    


# {
#       "expectation_type": "expect_table_row_count_to_be_between",
#       "kwargs": {
#         "max_value": 100000,
#         "min_value": 0
#       },
#       "meta": {
#         "profiler_details": {
#           "metric_configuration": {
#             "domain_kwargs": {},
#             "metric_name": "table.row_count",
#             "metric_value_kwargs": null
#           },
#           "num_batches": 1
#         }
#       }
#     },
#     {
#       "expectation_type": "expect_compound_columns_to_be_unique",
#       "kwargs": {
#         "column_list": [
#           "product",
#           "year",
#           "day_of_year",
#           "hour"
#         ]
#       },
#       "meta": {}
#     },
#     {
#       "expectation_type": "expect_column_values_to_not_be_null",
#       "kwargs": {
#         "column": "product"
#       },
#       "meta": {}
#     },
#     {
#       "expectation_type": "expect_column_values_to_not_be_null",
#       "kwargs": {
#         "column": "year"
#       },
#       "meta": {}
#     },
#     {
#       "expectation_type": "expect_column_values_to_not_be_null",
#       "kwargs": {
#         "column": "day_of_year"
#       },
#       "meta": {}
#     },
#     {
#       "expectation_type": "expect_column_values_to_not_be_null",
#       "kwargs": {
#         "column": "hour"
#       },
#       "meta": {}
#     },
#     {
#       "expectation_type": "expect_column_values_to_be_in_set",
#       "kwargs": {
#         "column": "product",
#         "value_set": [
#           "ABI-L1b-RadC"
#         ]
#       },
#       "meta": {}
#     },
#     {
#       "expectation_type": "expect_table_columns_to_match_set",
#       "kwargs": {
#         "column_set": [
#           "day_of_year",
#           "product",
#           "year",
#           "hour",
#           "id"
#         ]
#       },
#       "meta": {
#         "profiler_details": {
#           "success_ratio": 1.0
#         }
#       }
#     }