import requests
import boto3
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import json
from datetime import date, timedelta

# set today's date
today = date.today()

# Set up API variables
api_key = "1cd0b5e76d8a40f296b101351231603"
location = "New York"
start_date = today - timedelta(days=7)
end_date = today

# Make API request to WeatherAPI
url_history = f'https://api.weatherapi.com/v1/history.json?key={api_key}&q={location}&dt={start_date}&end_dt={end_date}'
print(url_history)
response = requests.get(url_history, verify=False)
#'url_forecast = f''

# Extract data from response JSON
data = response.json()

# Upload response JSON to S3 bucket
#AWS_ACCESS_KEY_ID = 'AKIAW4PHUVHLPFR5JIBJ'
#AWS_SECRET_ACCESS_KEY = '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'
########################################################################
#DE STERSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS inainte de a publica pe net
AWS_ACCESS_KEY_ID = 'AKIAW4PHUVHLPFR5JIBJ'
AWS_SECRET_ACCESS_KEY = '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'
########################################################################


s3 = boto3.client("s3", aws_access_key_id = AWS_ACCESS_KEY_ID, aws_secret_access_key = AWS_SECRET_ACCESS_KEY)
s3.put_object(Bucket="mystorsdata", Key="response.json", Body=str(data))

# AWS RDS PostgreSQL credentials
DB_ENDPOINT = 'my-db-instance.c1kgjgffsqlh.eu-central-1.rds.amazonaws.com'
DB_PORT = '5432'
DB_NAME = 'initial_db'
DB_USER = 'postgres'
DB_PASSWORD = 'postgres'
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
#df = pd.DataFrame(data["forecast"]["forecastday"][0]["hour"])
#df['condition'] = df['condition'].apply(json.dumps)
#columns = ["time", "temp_c" , 'is_day', "condition" , "wind_kph","wind_degree","wind_dir","pressure_mb","precip_mm",
#         "humidity","cloud",'feelslike_c', "windchill_c", "heatindex_c", "dewpoint_c", "will_it_rain", "chance_of_rain",
#         "will_it_snow", "chance_of_snow", "vis_km", "gust_kph", "uv"]
#print(df)


# Load data to PostgreSQL database
#df[columns].to_sql('history_weather', engine, if_exists='replace', index=True)
#print ('History weather data added successfully')


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
