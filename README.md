## Big Data Systems and Int Analytics

## Assignment1

#### Team Information

| NAME                      |     NUID        |
|---------------------------|-----------------|
|   Raj Mehta               |   002743076     |
|   Mani Deepak Reddy Aila  |   002728148     |
|   Jared Videlefsky        |   001966442     |
|   Rumi Jha                |   002172213     |
 
 Submission Date: 10th February'23

#### Data Pipeline in AWS

#### CLAAT Link



## About

**Implementing a data pipeline of SEVIR and Storm Events using Amazon Web Services S3 bucket for storage,
Amazon Glue for ETL and Amazon Quicksight for Visualization and Dashboard creation.**

![AWS_Architecture](add image)
S3:
![image](https://user-images.githubusercontent.com/47637485/217303369-17c5a261-d876-4090-8afd-61b0aae98825.png)

## Requirements

1. GEOS
    1. Explore and download selected datasets for the GOES satellite dataset
    2. Given a filename, construct the hyperlink of data location.
    3. Write Unit tests for all the use cases
    4. Test using the links from [Google-Docs-file](https://docs.google.com/spreadsheets/d/1o1CLsm5OR0gH5GHbTsPWAEOGpdqqS49-P5e14ugK37Q/edit#gid=0)
2. NexRad
    1. Explore and download selected datasets for the NexRad dataset
    2. Given a filename, construct the hyperlink of data location.
    3. Write Unit tests for all the use cases
    4. Test using the links from [Google-Docs-File](https://docs.google.com/spreadsheets/d/1o1CLsm5OR0gH5GHbTsPWAEOGpdqqS49-P5e14ugK37Q/edit#gid=0)
    5. Use a python package of your choice and plot the NexRad locations from [Nexrad-Wikipedia-Page](https://en.wikipedia.org/wiki/NEXRAD)

## Test Results

#### Creating script to fetch metadata from S3 and store it in a database along with fetching file names dynamically from the S3 bucket and then saving file to S3 user's bucket "Boto3"

## AWS Config

#### Add here

## SQLite Database

1. Imported the sqlite3 library ( can be installed using the command `pip install sqlite3`)
2. Sqlite Studio to work with the .db file (GUI)
3. Created 3 tables `geos18`, `nexrad` and `nexrad_lat_long`.
4. Utility class 'dbUtil' for functions like creating table, insertion of data into table and filter required data from table.

## Streamlit

We have implemented a Streamlit app to plot NexRad Radar Station in a frontend application.

Steps:
1. Import libraries (Streamlit, folium, Streamlit_foliumn) needed to plot the locations on map
2. Connect to the SQlite database and fetch data from nexrad_lat_long table having latitude and longitude information on the location
3. make use of folium function to plot the location on map
4. Launch the web application by running `streamlit run NexRadRadarStations.py` script

The data in the image below contains locations of current and archived radar stations. The map denotes these specified stations by a blue pin.
Additional information can be retrieved like the station name and city in which station is located by hovering over the points.
![image](https://user-images.githubusercontent.com/91744801/217998698-1e8d89ce-ed71-4a3e-8d77-dfc45a842986.jpg)


## Dashboards

### AWS Dashboard

#### change image according to our console

![Details-Fatality-2018](https://user-images.githubusercontent.com/59594174/110068829-88b8e980-7d44-11eb-8763-35b26129fb3d.png)

![Sevir_Details_2018](https://user-images.githubusercontent.com/59594174/110068840-8fdff780-7d44-11eb-9f4e-1ec0e1f3d5be.png)

### SQLite Tables

### Logging
Logging was created with AWS CloudWatch. Logs contain a timestamp and a message. The messages we record are below:
- User Input
- Generated URL based off of the User Input
- User Action: Download Locally or Transfer to S3 Bucket

Example Logs:
![image](https://user-images.githubusercontent.com/47637485/217996246-a39d46e0-ad0d-445a-b9ea-296f1be21abf.png)

#### change image according to our console

### Expected Results
![NEXRAD Locations by cities](https://user-images.githubusercontent.com/59594174/110068964-c9b0fe00-7d44-11eb-9c03-1f8660eca010.PNG)

## Attestation and Contribution Declaration:
Required attestation and contribution declaration on the GitHub page:
WE ATTEST THAT WE HAVEN’T USED ANY OTHER STUDENTS’ WORK IN OUR ASSIGNMENT
AND ABIDE BY THE POLICIES LISTED IN THE STUDENT HANDBOOK
- Raj Mehta
- Mani Deepak Reddy Aila
- Jared Videlefsky
- Rumi Jha 

## References & Citation

#### Add References
