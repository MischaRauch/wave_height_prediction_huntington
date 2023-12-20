import requests
from bs4 import BeautifulSoup
import pandas as pd

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

# URL to be scraped
url = "https://www.huntingtonbeachca.gov/residents/beach_info/livebeachcondition.cfm"

dfa = pd.read_csv('data/huntington.csv',index_col=0)
# Scrape the table and print it
dfb = scrape_huntington_beach_conditions(url)
if dfb is not None:
    df = pd.concat([dfa,dfb])
    print(df)
    df.to_csv('data/huntington.csv')
