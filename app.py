import streamlit as st
from streamlit_mongodb import MongoDBConnection

conn = st.experimental_connection('mongo_sample', type=MongoDBConnection)
query = {"minimum_nights": "3"}

no_of_results = st.slider('Query limit', 1, 50, 1)

df = conn.query(query, db="sample_airbnb", col="listingsAndReviews", limit=no_of_results)
st.dataframe(df, use_container_width=True)