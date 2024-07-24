import traceback
import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine
import os
import os
import matplotlib.pyplot as plt
csvs_created=[]
response=requests.get("https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page")
print(response.status_code)
soup=BeautifulSoup(response.content,'html.parser')
found_new=soup.find('div',{'class':'faq-answers','id':'faq2019'})
found_neww=found_new.find_all('a')
for found in found_neww:
    try:
        link=found['href']
        df=pd.read_parquet(link)
        df.drop_duplicates(inplace=True)
        print('45 ',df[:2])
        print((link[link.rfind('/')+1:]).replace('parquet','csv'))
        csvs_created.append((link[link.rfind('/')+1:]).replace('parquet','csv'))
        df['tpep_pickup_datetime'] = pd.to_datetime( df.iloc[:, 2])
        df['tpep_dropoff_datetime'] = pd.to_datetime( df.iloc[:, 3]) 
        #trip duration in minutes
        df['trip_duration'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60
        df['average_speed'] = df['trip_distance'] / df['trip_duration']
        
        # Aggregate data by day and calculate total trips and average fare per day
        # Extract date from datetime
        df['date'] = df['tpep_pickup_datetime'].dt.date

        # Group by date and calculate total trips and average fare
        df_grouped = df.groupby('date').agg({'VendorID': 'count', 'fare_amount': 'mean'}).reset_index()
        df_new=pd.concat([df,df_grouped])
        df_new.to_csv((link[link.rfind('/')+1:]).replace('parquet','csv'))
    except Exception as e:
        traceback.print_exc()
        pass

user = 'sumiran'
password = 'Abc@123'
host = 'localhost'
database = 'trip_data'
engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{database}', echo=False)

for csv_file_path in csvs_created:
    try:
        table_name = os.path.splitext(os.path.basename(csv_file_path))[0]
        df = pd.read_csv(csv_file_path)
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        print(f"Data from {csv_file_path} has been uploaded to the {table_name} table in the {database} database.")
        # Query the database to get date, total_trips, avg_fare
        query1 = f"""
            SELECT date, total_trips, avg_fare
            FROM {table_name}
            ORDER BY date;
        """
        cursor=engine.cursor()
        cursor.execute(query1)
        results = cursor.fetchall()
        
        # Plot the results
        plt.plot([x[0] for x in results], [x[1] for x in results])
        plt.xlabel('Date')
        plt.ylabel('Total Trips')
        plt.title('Total Trips per Day')
        plt.show()
        
        # Query the database to get passenger_count and avg_fare
        query2 = f"""
            SELECT passenger_count, AVG(fare_amount) AS avg_fare
            FROM {table_name}
            GROUP BY passenger_count;
        """
        cursor.execute(query2)
        results = cursor.fetchall()
        
        # Plot the results
        plt.bar([x[0] for x in results], [x[1] for x in results])
        plt.xlabel('Passenger Count')
        plt.ylabel('Average Fare')
        plt.title('Fare Distribution by Passenger Count')
        plt.show()
        
        # Close the database connection
        engine.close()
    except Exception as e:
        traceback.print_exc()
        pass
    

    