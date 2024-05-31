# banks_de_project_py
## Final project assignment on IBM Python for Data Engineering Course.

### Project Task:
Create a code that can be used to compile the list of the top 10 largest banks in the world ranked by market capitalization in billion USD. Further, the data needs to be transformed and stored in GBP, EUR, and INR as well, in accordance with the exchange rate information that has been made available to you as a CSV file. The processed information table is to be saved locally in a CSV format and as a database table.

Create an automated system to generate this information so that the same can be executed in every financial quarter to prepare the report.

### Instructions:
- Ensure that the `exchange_rate.csv` file is available in the same directory as the script.
- Modify the database path if required.
- Run the script to extract, transform, and load the data into an SQLite database, and execute the queries to display the results.

### Key Points:
- The script extracts the list of the largest banks by market capitalization from a specific URL.
- It transforms the data to include market capitalization in GBP, EUR, and INR.
- The transformed data is loaded into an SQLite database.
- The script includes functions to run and display results of SQL queries on the database.

### Usage:
Run the script in a Python environment with the necessary packages installed (`requests`, `bs4`, `pandas`, `numpy`, `sqlite3`).

```python
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import sqlite3

def extract(url, table_attribs):
    # Fetch the webpage content
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')

    # Initialize an empty DataFrame with the specified columns
    df = pd.DataFrame(columns=table_attribs)

    # Locate the correct table under the heading "By market capitalization"
    heading = data.find(id="By_market_capitalization")
    if heading is None:
        print("Error: 'By market capitalization' section not found.")
        return df

    table = heading.find_next('table', {'class': 'wikitable'})
    if table is None:
        print("Error: Table not found after 'By market capitalization' heading.")
        return df

    rows = table.find_all('tr')
    for row in rows[1:]:  # Skip the header row
        cols = row.find_all('td')
        if len(cols) != 0:
            bank_name = cols[0].get_text(strip=True)
            market_cap = cols[2].get_text(strip=True).replace('\n', '')

            # Skip rows with missing or invalid data
            if bank_name and market_cap and '—' not in market_cap:
                try:
                    market_cap_value = float(market_cap)
                    data_dict = {"bank name": bank_name, "MC_USD_Billion": market_cap_value}
                    df1 = pd.DataFrame(data_dict, index=[0])
                    df = pd.concat([df, df1], ignore_index=True)
                except ValueError:
                    print(f"Error converting market cap value: {market_cap}")

    return df

def transform(df, exchange_rate_file):
    # Read exchange rate CSV file
    exchange_rate_df = pd.read_csv(exchange_rate_file)
    
    # Convert to dictionary
    exchange_rate = exchange_rate_df.set_index('Currency').to_dict()['Rate']
    
    # Add new columns scaled by the corresponding exchange rate factors
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]

    return df

def load_to_db(conn, table_name, df):
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    print(f"Data successfully written to table '{table_name}' in the database.")

def run_queries(conn, query):
    print(f"Executing query:\n{query}")
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    for row in results:
        print(row)

# Define the URL and the table attributes
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ['bank name', 'MC_USD_Billion']
exchange_rate = exchange_rate_csv

# Extract data
df = extract(url, table_attribs)

# Path to the exchange rate CSV file
exchange_rate_file = 'exchange_rate.csv'

# Transform data
df_transformed = transform(df, exchange_rate_file)

# Connect to the SQLite database
conn = sqlite3.connect('Banks.db')

# Define the table name
table_name = 'Largest_banks'

# Load the transformed data to the database
load_to_db(conn, table_name, df_transformed)

# Define the queries
queries = [
    "SELECT * FROM Largest_banks",
    "SELECT AVG(MC_USD_Billion) FROM Largest_banks",
    "SELECT `bank name` FROM Largest_banks LIMIT 5"
]

# Execute the queries
for query in queries:
    run_queries(conn, query)

# Close the connection
conn.close()
