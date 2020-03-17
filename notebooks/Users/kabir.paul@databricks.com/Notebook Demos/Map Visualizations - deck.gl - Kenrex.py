# Databricks notebook source
# MAGIC %md # Map Visualizations
# MAGIC 
# MAGIC ##Pydeck
# MAGIC 
# MAGIC https://pypi.org/project/pydeck/
# MAGIC 
# MAGIC Prerequisites:
# MAGIC - Sign up for a [MapBox](https://www.mapbox.com/) account. Pydeck and [Deck.gl]("https://deck.gl/#/") uses mapbox for base maps.
# MAGIC - Add your `MAPBOX_API_KEY` as an environment variable through the Cluster UI
# MAGIC - Install `pydeck` library on cluster

# COMMAND ----------

 ## MAPBOX_API_KEY=<enter key>

# COMMAND ----------

import pydeck

# 2014 locations of car accidents in the UK
UK_ACCIDENTS_DATA = ('https://raw.githubusercontent.com/uber-common/'
                     'deck.gl-data/master/examples/3d-heatmap/heatmap-data.csv')

# Define a layer to display on a map
layer = pydeck.Layer(
    'HexagonLayer',
    UK_ACCIDENTS_DATA,
    get_position='[lng, lat]',
    auto_highlight=True,
    elevation_scale=50,
    pickable=True,
    elevation_range=[0, 3000],
    extruded=True,                 
    coverage=1)

# Set the viewport location
view_state = pydeck.ViewState(
    longitude=-1.415,
    latitude=52.2323,
    zoom=6,
    min_zoom=5,
    max_zoom=15,
    pitch=40.5,
    bearing=-27.36)

# COMMAND ----------

# Render
r = pydeck.Deck(layers=[layer], initial_view_state=view_state)
r.to_html('demo.html')

# Please see the note about using a Mapbox API token here:
# https://github.com/uber/deck.gl/tree/master/bindings/python/pydeck#mapbox-api-token

# COMMAND ----------

# MAGIC %sh cp /databricks/driver/demo.html /dbfs/FileStore/../demo.html

# COMMAND ----------

displayHTML("""<a href="https://.../demo.html">Visualization here, click me to download</a> """)