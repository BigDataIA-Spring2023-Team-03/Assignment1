import folium 
import streamlit as st
import pandas as pd
from streamlit_folium import st_folium
from Util.dbUtil import DbUtil


# data = pd.read_csv('nexrad-stations.csv')


util = DbUtil('metadata.db')
conn = util.conn

data = pd.read_sql_query(f'select * from nexrad_lat_long', conn)

st.title("NexRad Radar Stations")
st.subheader("The data here contains locations of current and archived radar stations. The map denotes these specified stations by a blue pin.")
st.caption("Note: You can find information like the station name and city in which station is located by hovering over the points.")

# Select only Columns that are needed. First 2 column will be used for plotting others can be used for labelling.
map_loc = data[["LAT", "LONG", "station", "city"]]

# Passing the latitude and longitutde value to plot it on map 
map = folium.Map(location=[map_loc.LAT.mean(), map_loc.LONG.mean()], zoom_start=14, control_scale=True)

# Adding the points to the map by itterating through the dataframe
for index, location_info in map_loc.iterrows():
    folium.Marker([location_info["LAT"], location_info["LONG"]], 
    popup=[location_info["LAT"], location_info["LONG"]], 
    tooltip=[location_info["LAT"], location_info["LONG"], "Station: " + location_info["station"], "City: " + location_info["city"]]).add_to(map)
    
st_data = st_folium(map, width=725)

util.conn.close()
