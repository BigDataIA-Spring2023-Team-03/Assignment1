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

def find_url_from_filename(filename: str) -> str:
  file = filename.split("_")

  prefix = "https://noaa-goes16.s3.amazonaws.com/"
  delim = "/"
  prod = file[1][:file[1].rindex("-")-1]
  prod = prod.rstrip(string.digits)
  year =  file[3][1:5]
  month =  file[3][5:8]
  date =  file[3][8:10]

  return prefix + prod + delim + year + delim + month + delim + date +delim + filename

BUCKET_NAME = 'noaa-goes16'
BUCKET_FILE_NAME = 'ABI-L1b-RadC/2023/003/02/OR_ABI-L1b-RadC-M6C01_G16_s20230030206174_e20230030208551_c20230030208598.nc'
LOCAL_FILE_NAME = 'OR_ABI-L1b-RadC-M6C01_G16_s20230030206174_e20230030208551_c20230030208598.nc'

s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
s3.download_file(BUCKET_NAME, BUCKET_FILE_NAME, LOCAL_FILE_NAME)


result = s3.list_objects_v2(Bucket=BUCKET_NAME, Delimiter="/")
folders = [fld["Prefix"] for fld in result["CommonPrefixes"]]

for folder in folders:
    print("Folder:", folder)
    first_level.append(folder)

prod_selected = st.selectbox(
    'Please select Product', first_level)

if prod_selected:                                                                                   #prod selected
     
    result = s3.list_objects(Bucket=BUCKET_NAME, Prefix=prod_selected, Delimiter='/')
    for o in result.get('CommonPrefixes'):
        year.append(o.get('Prefix'))

    for i in range(len(year)):
        year[i] = year[i].replace(prod_selected, '')
    
    year_selected = st.selectbox(
    'Please select year', year)


    if year_selected:      
                                                                                     #year selected
        prefix = prod_selected+year_selected
        result = s3.list_objects(Bucket=BUCKET_NAME, Prefix=prefix, Delimiter='/')
        for o in result.get('CommonPrefixes'):
            months.append(o.get('Prefix'))

        for i in range(len(months)):
            months[i] = months[i].replace(prefix, '')
        
        months_selected = st.selectbox(
        'Please select month', months)

        if months_selected:      
                                                                                     #months selected
            prefix = prod_selected+year_selected+months_selected
            result = s3.list_objects(Bucket=BUCKET_NAME, Prefix=prefix, Delimiter='/')
            for o in result.get('CommonPrefixes'):
                days.append(o.get('Prefix'))

            for i in range(len(days)):
                days[i] = days[i].replace(prefix, '')
            
            days_selected = st.selectbox(
            'Please select Day', days)

            if days_selected:      
                                                                                     #months selected
                prefix = prod_selected+year_selected+months_selected+days_selected
                result = s3.list_objects(Bucket=BUCKET_NAME, Prefix=prefix, Delimiter='/')
                for item in result['Contents']:
                    file = item['Key']
                    files.append(file)

                for i in range(len(files)):
                    files[i] = files[i].replace(prefix, '')

                files_selected = st.selectbox(
                'Please select file', files)

                if files_selected:
                    link = find_url_from_filename(files_selected)
                    st.write(link)

                    st.text("")
                    st.text("")
                    st.text("")

                    if st.button('Download File'):
                        webbrowser.open_new_tab(link)


        


