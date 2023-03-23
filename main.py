import requests
import boto3
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import json
from datetime import date, timedelta
import os

# set today's date
today = date.today()

# Set up API variables
api_key = os.environ['weather_api_key']
location = "New York"
start_date = today - timedelta(days=7)
end_date = today

# Make API request to WeatherAPI
url_history = f'https://api.weatherapi.com/v1/history.json?key={api_key}&q={location}&dt={start_date}&end_dt={end_date}'
response = requests.get(url_history, verify=False)

# Extract data from response JSON
data = response.json()

# Upload response JSON to S3 bucket
AWS_ACCESS_KEY_ID = os.environ['aws_access_key']
AWS_SECRET_ACCESS_KEY = os.environ['aws_secret_access_key']

s3 = boto3.client("s3", aws_access_key_id = AWS_ACCESS_KEY_ID, aws_secret_access_key = AWS_SECRET_ACCESS_KEY)
s3.put_object(Bucket="mystorsdata", Key="response.json", Body=str(data))

# AWS RDS PostgreSQL credentials
DB_ENDPOINT = os.environ['db_endpoint']
DB_PORT = os.environ['db_port']
DB_NAME = os.environ['db_name']
DB_USER = os.environ['db_user']
DB_PASSWORD = os.environ['db_password']
try:
    connection = psycopg2.connect(
        host=DB_ENDPOINT,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    print("Database connection successful!")
except psycopg2.Error as e:
    print(f"Error connecting to database: {e}")
finally:
    connection.close()
# Create a connection to the database
engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_ENDPOINT}:{DB_PORT}/{DB_NAME}')

# Transform the data

# Load data to PostgreSQL database
for i in range(len(data["forecast"]["forecastday"])):
    df = pd.DataFrame(data["forecast"]["forecastday"][i]["hour"])
    df['condition'] = df['condition'].apply(json.dumps)
    columns = ['time', 'temp_c' , 'is_day', 'condition' ,'wind_kph','wind_degree',"wind_dir","pressure_mb","precip_mm",
             "humidity","cloud",'feelslike_c', "windchill_c", "heatindex_c", "dewpoint_c", "will_it_rain", "chance_of_rain",
             "will_it_snow", "chance_of_snow", "vis_km", "gust_kph", "uv"]
    df[columns].to_sql('history_weather', engine, if_exists='append', index=True)
    print(df[columns], 'aici e ora----------------------')
print('gata for-u')

print('Data successfully loaded to PostgreSQL database!')
