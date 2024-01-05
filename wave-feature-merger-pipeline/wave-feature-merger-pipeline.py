import hopsworks
import pandas as pd
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import accuracy_score
from sklearn.metrics import confusion_matrix
from sklearn.metrics import classification_report
import seaborn as sns
from matplotlib import pyplot
from hsml.schema import Schema
from hsml.model_schema import ModelSchema
import joblib
import os

# You have to set the environment variable 'HOPSWORKS_API_KEY' for login to succeed
project = hopsworks.login()
fs = project.get_feature_store()

################# GET THE BUOY DATA #################
# The feature view is the input set of features for your model. The features can come from different feature groups.    
# You can select features from different feature groups and join them together to create a feature view
buoy_swells_huntington_fg = fs.get_feature_group(name="buoy_swells_huntington", version=1)
query = buoy_swells_huntington_fg.select_all()
df = query.read()
df['hits_at'] = df['hits_at'].dt.strftime('%Y-%m-%d %H')
buoy_swells_sorted = df.sort_values(by='hits_at', ascending=False)

################# GET THE BEACH DATA #################
beach_swells_huntington_fg = fs.get_feature_group(name="beach_swells_huntington", version=1)
query2 = beach_swells_huntington_fg.select_all()
df2 = query2.read()
df2['datetime'] = df2['datetime'].dt.strftime('%Y-%m-%d %H')
beach_swells_sorted = df2.sort_values(by='datetime', ascending=False)

################# MERGE DATAFRAMES #################
merged_df = pd.merge(buoy_swells_sorted, beach_swells_sorted, left_on='hits_at', right_on='datetime', how='inner')
merged_df = merged_df.drop(["pred_dtime",'datetime'],axis=1)

################# CREATE NEW FEATURE GROUP #################
# Save the merged DataFrame as a new Feature Group
merged_fg = fs.get_or_create_feature_group(name="merged_swells_huntington",
                version=2,
                primary_key=['year','month','day','hour','minute'],
                description="Merged swell from buoy and huntington website",
                online_enabled=True,
                statistics_config={"enabled": True, "histograms": True, "correlations": True},
                )
merged_fg.insert(merged_df)

################# CREATE FEATURE VIEW #################
# I do not think we should change the feature view, if we do not want to train a new model
# merged_fg = fs.get_feature_group(name="merged_swells_huntington", version=2)
# query = merged_fg.select_all()
# feature_view = fs.get_or_create_feature_view(name="merged_swells_huntington",
#                                   version=2,
#                                   description="Feature view combining the buoy swells and the beach swells",
#                                   labels=["quality"],
#                                   query=query)