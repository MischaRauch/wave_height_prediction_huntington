# Create the newest prediction and logs the statistics and potential visuals
import hopsworks
import pandas as pd
import numpy as np 
import joblib
from datetime import datetime, timedelta
import dataframe_image as dfi
import matplotlib.pyplot as plt 
from sklearn.metrics import mean_absolute_error

import sys 
import os
os.chdir('/'.join(os.getcwd().split('/')[:-1]))
from wave_model.LinReg import LinReg


project = hopsworks.login()
fs = project.get_feature_store(name='smdl_a01_featurestore')

mr = project.get_model_registry()
model = mr.get_model('wave_reg',version=1)
model_dir = model.download()
model = joblib.load(model_dir + "/wave_reg.pkl")

fv = fs.get_feature_view('merged_swells_huntington', version=5)
df = fv.get_batch_data(
    start_time = (datetime.now()-timedelta(1)).strftime("%Y%m%d"),
    end_time = datetime.now().strftime("%Y%m%d"),
    read_options={"use_hive": True})

y_pred = model.predict_labels(df)

fg = fs.get_feature_group(name="merged_swells_huntington", version=4)
df = fg.read()[-df.shape[0]:]

labels = df['quality']

monitor_fg = fs.get_or_create_feature_group(name="wave_monitor_predictions",
                                            version=1,
                                            primary_key=["datetime"],
                                            description="Outcome Monitoring"
                                            )

now = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
data = {
    'prediction': y_pred,
    'label': labels,
    'prediction_at': [now for _ in range(len(y_pred))],
    'datetime':df['hits_at'].apply(str).str.slice(0,13)
    }
monitor_df = pd.DataFrame(data)
monitor_fg.insert(monitor_df, write_options={"wait_for_job" : False})

history_df = monitor_fg.read(read_options={"use_hive": True} )
# Add our prediction to the history, as the history_df won't have it - 
# the insertion was done asynchronously, so it will take ~1 min to land on App
history_df = pd.concat([history_df, monitor_df])

df_export = pd.DataFrame({'prediction':history_df['prediction'],
 'label':history_df['label'],
 'abs_err':np.abs(model.encode(history_df['label']).reshape(-1) - model.encode(history_df['prediction'])),
 'datetime':history_df['datetime'],
 'prediction_at':history_df['prediction_at']})

df_recent = df_export.tail(5)
dfi.export(df_recent, './wave_df_recent.png', table_conversion = 'matplotlib')
dataset_api = project.get_dataset_api()
dataset_api.upload("./wave_df_recent.png", "Resources/images", overwrite=True)

predictions = model.encode(history_df.tail(10)['prediction'])
labels = model.encode(history_df.tail(10)['label'])
mae = mean_absolute_error(labels,predictions)

fig,ax = plt.subplots()
ax.text(0.5,0.9,'MAE',fontsize=50,ha='center')
ax.text(0.5,0.5,mae,fontsize=100,va='center', ha='center')
ax.text(0.5,0.1,'from last 10 data points',fontsize=25,va='center', ha='center')
ax.tick_params(left=False, labelleft=False, bottom=False, labelbottom=False)
for spine in ax.spines.values():
    spine.set_visible(False)
fig.savefig('wave_mae_recent')

dataset_api.upload("./wave_mae_recent.png", "Resources/images", overwrite=True)