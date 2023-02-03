import streamlit as st
import pandas as pd
import numpy as np
import json
import streamlit.components.v1 as components
import os
import boto3
from botocore import UNSIGNED
from botocore.config import Config
import string
import webbrowser
from decouple import config
# Import class from dbUtil
import dbUtil

########################################################################################################################
# AWS Destination Credentials:
aws_access_key_id = config('aws_access_key_id')
aws_secret_access_key = config('aws_secret_access_key')
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
data_source = st.selectbox('Data Source: ', ['GOES-16 geostationary satellite', 'NEXRAD weather radars'])

if data_source == 'GOES-16 geostationary satellite':
    BUCKET_NAME = 'noaa-goes16'
    core_url = 'https://noaa-goes18.s3.amazonaws.com/{product_fn}/{year}/{day_of_year}/{hour}/{file_name}'
    metadata = ['Product', 'Year', 'Day of Year', 'Hour', 'File Name']
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
    test = s3_dest.upload_fileobj(src_response['Body'], dest_bucket, dest_file_name)
    
    dest_url = f'https://{dest_bucket}.s3.amazonaws.com/{dest_file_name}'
    # print(f'Destination s3 URL: {dest_url}')
    
    return dest_url



result = s3.list_objects_v2(Bucket=BUCKET_NAME, Delimiter="/")
folders = [fld["Prefix"] for fld in result["CommonPrefixes"]]

for folder in folders:
    # print("Folder:", folder)
    first_level.append(folder)

prod_selected = st.selectbox(
    f'Please select {metadata[0]}', first_level)

if prod_selected:                                                                                   #prod selected
     
    result = s3.list_objects(Bucket=BUCKET_NAME, Prefix=prod_selected, Delimiter='/')
    for o in result.get('CommonPrefixes'):
        year.append(o.get('Prefix'))

    for i in range(len(year)):
        year[i] = year[i].replace(prod_selected, '')
    
    year_selected = st.selectbox(
    f'Please select {metadata[1]}', year)


    if year_selected:      
                                                                                     #year selected
        prefix = prod_selected+year_selected
        result = s3.list_objects(Bucket=BUCKET_NAME, Prefix=prefix, Delimiter='/')
        for o in result.get('CommonPrefixes'):
            months.append(o.get('Prefix'))

        for i in range(len(months)):
            months[i] = months[i].replace(prefix, '')
        
        months_selected = st.selectbox(
        f'Please select {metadata[2]}', months)

        if months_selected:      
                                                                                     #months selected
            prefix = prod_selected+year_selected+months_selected
            result = s3.list_objects(Bucket=BUCKET_NAME, Prefix=prefix, Delimiter='/')
            for o in result.get('CommonPrefixes'):
                days.append(o.get('Prefix'))

            for i in range(len(days)):
                days[i] = days[i].replace(prefix, '')
            
            days_selected = st.selectbox(
            f'Please select {metadata[3]}', days)

            if days_selected:      
                                                                                     #months selected
                prefix = prod_selected+year_selected+months_selected+days_selected
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
                    url = filename_url_producer(BUCKET_NAME, files_selected)
                    st.write(url)

                    # TESTING
                    st.write('TEST:')
                    st.write(f'Prefix = {prefix}  \n Files_selected = {files_selected}')
                    st.text("")
                    st.text("")
                    st.text("")

                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button('Download File'):
                            webbrowser.open_new_tab(url)

                    with col2:
                        if st.button('Transfer File to S3 Bucket'):
                            dest_url = copy_file_to_dest_s3(BUCKET_NAME, dest_bucket, dest_folder, prefix, files_selected)
                            st.write(f'Destination s3 URL: {dest_url}')


        


