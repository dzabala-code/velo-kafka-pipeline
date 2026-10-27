import streamlit as st
import pandas as pd
from kafka import KafkaConsumer
import json
import time
import pydeck as pdk

st.title("Bikes in Lyon in real time")

topic_name = "velib-stations"
consumer=KafkaConsumer(topic_name,bootstrap_servers='localhost:9092',group_id=None)

station_record={}
zone_carte = st.empty()
dernier_dessin = 0

for message in consumer:
    texte=message.value.decode('utf-8')
    station=json.loads(texte)
    id_station=station['number']
    if station['contract_name']=='lyon':
        station_record[id_station]={'lat':station['position']['lat'],'lon':station['position']['lng'],"bikes":station['available_bikes']}

    if station_record and time.time() - dernier_dessin > 2:
        df = pd.DataFrame(list(station_record.values()))          

        df["texte"] = df["bikes"].astype(str)
        df["couleur"] = df["bikes"].apply(lambda b: [214, 39, 40] if b == 0 else [44, 160, 44])
        

        cercles = pdk.Layer(
            "ScatterplotLayer",
            data=df,
            get_position="[lon, lat]",
            get_fill_color="couleur",
            get_radius=40,
            radius_min_pixels=8,
            radius_max_pixels=14,
            radius_units="pixels",
        )

        textes = pdk.Layer(
            "TextLayer",
            data=df,
            get_position="[lon, lat]",
            get_text="texte",
            get_size=12,
            get_color=[255, 255, 255],
            get_text_anchor='"middle"',
            get_alignment_baseline='"center"',
        )

        vue = pdk.ViewState(latitude=45.76, longitude=4.84, zoom=12)

        zone_carte.pydeck_chart(pdk.Deck(layers=[cercles, textes],
                                         initial_view_state=vue,
                                          map_style="light"))
        dernier_dessin = time.time()
