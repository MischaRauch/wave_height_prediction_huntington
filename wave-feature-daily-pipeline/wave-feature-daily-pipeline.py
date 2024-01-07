# Get the daily/hourly (I think its updated every 6 hours) data from the buoy (https://www.ndbc.noaa.gov/station_page.php?station=46256)
# and save it to the data store in Hopsworks

import os
import urllib.request  
import re
from itertools import chain
import pandas as pd
import numpy as np
import hopsworks
from datetime import datetime, timedelta
import sys


def get_latest_url(today):
    pred_date = today.strftime("%Y%m%d")

    # There are 4 predictions per day at hours: "00", "06", "12", "18",
    h=int(today.strftime("%H"))
    found = False
    test_url = ""
    attempted_date = today

    while not found:
        pred_hour = "00"
        if h > 5:
            pred_hour = "06"
        if h > 11:
            pred_hour = "12" 
        if h > 17:
            pred_hour = "18"
        test_url = "https://ftpprd.ncep.noaa.gov/data/nccf/com/gfs/prod/gfs." \
        + attempted_date.strftime("%Y%m%d") + \
        "/" + pred_hour + "/wave/station/bulls.t" + pred_hour + "z/gfswave." + buoy + ".bull"
        try:
            urllib.request.urlopen(test_url)
            found = True
        except urllib.error.HTTPError as e: 
            # assume 404, URL not found. Try previous time.
            h = h - 6
            if h < 0:
                attempted_date = attempted_date - timedelta(days=1)
                # if i have to look back >1 day, then just exit with error - because upstream is prob broken
                if (today.day - attempted_date.day > 1):
                    sys.exit("ERROR: Could not download url: " + test_url) 
    return test_url, pred_hour


def process_url(buoy_url):
    out = []
    for line in urllib.request.urlopen(buoy_url):
        l = line.decode('utf-8') #utf-8 or iso8859-1 or whatever the page encoding scheme is
        row=[]
        #print(l)
        if "Cycle" in l:
            regex = re.findall(r'Cycle.*:\s+([0-9]+)\s+([0-9]+)\s+UTC.*', l)
            if len(regex):
                thedate=regex[0]
        else:
            res = re.match(r'.*[|]\s+([0-9]+)\s+([0-9]+)\s+[|].*', l)
            #waves = re.findall(r'[|]\s+([0-9\.]+)\s+([0-9\.]+)\s+([0-9]+)\s+[|]', l)
            waves = re.findall(r'\s+([0-9\.]+)\s+([0-9\.]+)\s+([0-9]+)\s+', l)
            #print("waves", waves)
            if res is not None:
                row.append(thedate)
                row.append(res.groups())
            #print("Waves ",len(waves))
            if len(waves):
                if len(waves) > 3:
                    #print("found > 3 waves, reduce to 3")
                    waves = waves[:3]
                b = []
                list(b.extend(item) for item in waves)
                row.append(b)
                my = tuple(chain.from_iterable(row))
                out.append(my)
    return out, thedate


primary_columns=['pred_dtime', 'hour', 'pred_day', 'pred_hour', 'height1', 'period1', 'direction1', 'height2', 
         'period2', 'direction2', 'height3', 'period3', 'direction3'] 

# changed angels to valid swell from around 10 - 65 degrees
def is_valid_swell_direction(direction):
    if int(direction) > 66 or int(direction) < 9:
        return False
    return True

def best_height(row):
    best_secondary=2
    # Check which is best secondary swell - swell 2 or swell 3?
    if row['direction3'] != None:
        if is_valid_swell_direction(row['direction3']):
            if is_valid_swell_direction(row['direction2']) == False :
                best_secondary=3    
    best_direction = "direction" + str(best_secondary)
    best=1
    # Check which is best of swell 1 and secondary swell ?
    if row[best_direction] != None and is_valid_swell_direction(row[best_direction]) == True:
        if is_valid_swell_direction(row['direction1']) == False:
            best=best_secondary
                
    height = row['height' + str(best)]
    period = row['period' + str(best)]
    direction = row['direction' + str(best)]
        
    return pd.Series([height, period, direction])

# feature engineering - estimate the time at which the swell arrives at Lahinch from buoy
def estimate_hits_at(row):
    # baseline estimate
    hits_at = row['pred_dtime'] + row['hour_offset'] + timedelta(hours=0.4) 
    return pd.Series([hits_at])


## START 
buoy="46253" # our bouy ID

# get date
today = datetime.now()
url, pred_hour = get_latest_url(today)

hours = 6 # number of sweel predictions we save i.e. every 6 hours = 6 sweels 
secondary_columns=[]
for i in range(1,hours):
    j=i*2
    secondary_columns.append("height" + str(j))
    secondary_columns.append("period" + str(j))
    secondary_columns.append("direction" + str(j))
    secondary_columns.append("hits_at" + str(j))

# get data and process the data
res,thedate=process_url(url)
df = pd.DataFrame(res, columns=primary_columns)
df['pred_dtime'] = pd.to_datetime(df['pred_dtime'], format='%Y%m%d')

# Generate the timedelta series (20 minutes for each row)
time_offsets = pd.Series([timedelta(minutes=20) * i for i in range(len(df))])
df['hour_offset'] = time_offsets

df['hour'] = df['hour'].astype(int)  # Convert to integer
df['pred_dtime'] = df['pred_dtime'] + pd.to_timedelta(df['hour'], unit='h')

# get the waves that go to shore and are relevant
df[['height','period','direction']]=df.apply(best_height, axis=1)
# add the time needed to reach the beach
df[['hits_at']]=df.apply(estimate_hits_at, axis=1)
df['beach_id'] = 1
df.drop(['height1', 'period1', 'direction1', 'height2', 'period2', 'direction2', 'hour_offset',
          'height3', 'period3', 'direction3','hour', 'pred_day', 'pred_hour'], axis=1, inplace=True) 

# connect to hopsworks and save data
project = hopsworks.login()
fs = project.get_feature_store()

version = 1
swells_fg = fs.get_or_create_feature_group(name="buoy_swells_huntington",
                version=version,
                primary_key=["beach_id"],
                event_time="hits_at",
                description="Buoy surf height predictions",
                online_enabled=True,
                statistics_config={"enabled": True, "histograms": True, "correlations": True}
                )
swells_fg.insert(df)

import joblib

data = df.sort_values(by='hits_at',ascending=False).head(1)[['height','period','direction','pred_dtime']]

time = data['pred_dtime'] + timedelta(minutes=24)
X = data.drop(['pred_dtime'],axis=1).values
mr = project.get_model_registry()
model = mr.get_model('wave_reg',version=1)
model_dir = model.download()
model = joblib.load(model_dir + "/wave_reg.pkl")

y_pred = model.predict_labels(X)

time = time.values[0]

import matplotlib.pyplot as plt
fig,ax = plt.subplots()
ax.text(0.5,0.9,'Surfing quality is predicted to be:',fontsize=50,ha='center')
ax.text(0.5,0.5,y_pred[0],fontsize=100,va='center', ha='center')
ax.text(0.5,0.1,'updated on '+pd.to_datetime(str(time)).strftime('%H:%M %d/%m/%Y'),fontsize=25,va='center', ha='center')
ax.tick_params(left=False, labelleft=False, bottom=False, labelbottom=False)
for spine in ax.spines.values():
    spine.set_visible(False)
fig.savefig('prediction')

dataset_api = project.get_dataset_api()
dataset_api.upload("./prediction.png", "Resources/images", overwrite=True)