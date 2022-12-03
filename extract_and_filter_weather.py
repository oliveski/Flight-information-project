import json
import boto3
import requests
import configparser
import pandas as pd
import awswrangler as wr
from datetime import date

def getAPIresponse(time: str):
    api_user = "poatek_giongo"
    api_pass = "mg973XWfQU"
    api_required = "t_2m:C,visibility:m,is_fog_30min:idx,is_rain_30min:idx"
    api_coordinates = "-30.0324999,-51.2303767"
    api_base_url = "https://{}:{}@api.meteomatics.com/{}/{}/{}/csv?model=mix"
    api_url = api_base_url.format(
        api_user,
        api_pass,
        time,
        api_required,
        api_coordinates
    )
    api_url = "https://poatek_giongo:mg973XWfQU@api.meteomatics.com/2022-12-03T16:34:00.000-03:00/t_2m:C,visibility:m,is_fog_30min:idx,is_rain_30min:idx/-30.0324999,-51.2303767/csv?model=mix"
    response = requests.get(api_url)
    return response

def getResponseData(response_str: str) -> list:
    response_lines = response_str.split('\n')
    response_data = response_lines[1].split(';')
    date = response_data[0]
    t_2m = response_data[1]
    visibility = response_data[2]
    is_fog = response_data[3]
    is_rain = response_data[4]
    return_list = [date, t_2m, visibility, is_fog, is_rain]
    return return_list

parser = configparser.ConfigParser()
parser.read("mentorship.conf")
aws_key = parser.get("AWSCREDENTIALS", "access_key")
aws_secret_key = parser.get("AWSCREDENTIALS", "secret_access_key")

# Boto3 session for the AWS Wrangler
session = boto3.Session(
    aws_access_key_id=aws_key,
    aws_secret_access_key=aws_secret_key,
    region_name='us-east-1'
)

# S3 resource object
s3 = boto3.resource(
    's3',
    region_name='us-east-1',
    aws_access_key_id=aws_key,
    aws_secret_access_key=aws_secret_key
)

today = date.today()
airport_data_object = s3.Object(
    'projeto-de-mentoria',
    'data/transformed/airport/' + f'{today}.csv'
)

df = pd.read_csv(airport_data_object.get()["Body"])
# df = pd.read_csv("../s3_object_get_test.csv")
df = df.sort_values(by="scheduled_departure_time")

# print(df.head())
# df.to_csv("s3_object_get_test.csv", index=False)


# last_checked_time = 0
# weather_conditions = []
# for index,row in df.iterrows():
#     print(index, row['scheduled_departure_time'])

response = getAPIresponse("")
print(response.text)
# print()
# print(getResponseData(response.text))