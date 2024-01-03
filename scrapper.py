import requests
from bs4 import BeautifulSoup
import pandas as pd
import hopsworks

def scrape_huntington_beach_conditions(url):
    # Sending a request to the website
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parsing the webpage content
        soup = BeautifulSoup(response.text, 'html.parser')

        # Finding the table in the webpage
        # Assuming the first table is the one we need
        tables = soup.find_all('table')

        time = tables[1]
        table = tables[2]

        # Extracting all rows from the table
        rows = table.find_all('tr')

        # List to hold all rows of data
        data = []

        # Iterating over rows and extracting data
        for row in rows:
            cols = row.find_all('td')
            cols = [ele.text.strip() for ele in cols]
            data.append(cols)

        # Creating a DataFrame from the scraped data
        df = pd.DataFrame(data).T
        df.columns = df.iloc[0,:]
        df = df.iloc[1,:]
        df['time'] = time.find_all('tr')[1].find_all('td')[0].text.strip()[9:]

        return pd.DataFrame(df).T
    else:
        print("Failed to retrieve data")
        return None

import datetime
from pytz import timezone    


def reformat_time(time:str,year:int):
    time_list = time.split()
    hour, minute = map(int,time_list[3].split(":"))
    day = int(time_list[1])
    month = datetime.datetime.strptime(time_list[0],'%B').month
    if time_list[4] == 'PM' and hour != 12:
        hour +=12
    if time_list[4] == 'AM' and hour == 12:
        hour = 0

    return datetime.datetime(year=year,month=month,day=day,hour=hour,minute=minute)

def reformat_time_la(time:str):
    los_angeles = timezone('America/Los_Angeles')
    la_time = datetime.datetime.now(los_angeles)
    return reformat_time(time,year=la_time.year)

def preprocess(df):
    df = df.copy()
    df['datetime'] = df['time'].apply(reformat_time_la)
    df['quality'] = df['Surf Quality']
    df = df[['quality','datetime']]
    df['year'] = df['datetime'].dt.year
    df['month'] = df['datetime'].dt.month
    df['day'] = df['datetime'].dt.day
    df['hour'] = df['datetime'].dt.hour
    df['minute'] = df['datetime'].dt.minute

    return df

# URL to be scraped
url = "https://www.huntingtonbeachca.gov/residents/beach_info/livebeachcondition.cfm"

df = scrape_huntington_beach_conditions(url)
if df is not None:
    project = hopsworks.login()
    fs = project.get_feature_store()
    df = preprocess(df)
    fg = fs.get_feature_group(name="beach_swells_huntington",version=1)
    fg.insert(df)
