import json
import boto3
import requests
import configparser
import pandas as pd
import awswrangler as wr
from datetime import date

def getAPIResponse() -> dict:
    api_mode = "departures"
    api_airport_iata = "POA"
    api_day_to_get = -2
    api_base_url = "https://api.flightapi.io/schedule/637ba984e277bdbf7ea82200?mode={}&day={}&iata={}"
    api_url = api_base_url.format(api_mode, api_day_to_get, api_airport_iata)
    api_url = "https://api.flightapi.io/schedule/637ba984e277bdbf7ea82200?mode=departures&day=-2&iata=POA"
    response = requests.get(api_url)
    data = response.json()
    return data

def getIdentificationData(flights_data: list) -> pd.DataFrame:
    number_default_data = [
        i['flight']['identification']['number']['default']
        for i in flights_data_response
        ] 

    number_alternative_data = [
        i['flight']['identification']['number']['alternative']
        for i in flights_data_response
        ]

    model_data = [
        i['flight']['aircraft']['model']['text']
        for i in flights_data_response
    ]

    registration_data = [
        i['flight']['aircraft']['registration']
        for i in flights_data_response
    ]

    airline_name_data = [
        i['flight']['airline']['name']
        for i in flights_data_response
    ]

    airline_code_data_iata = [
        i['flight']['airline']['code']['iata']
        for i in flights_data_response
    ]

    airline_code_data_icao = [
        i['flight']['airline']['code']['icao']
        for i in flights_data_response
    ]

    id_data = {
        'flight_number_default': number_default_data,
        'flight_number_alternative': number_alternative_data,
        'aircraft_model': model_data,
        'aircraft_registration': registration_data,
        'airline_name': airline_name_data,
        'airline_code': airline_code_data_iata,
        'airline_code_icao': airline_code_data_icao
    }

    return pd.DataFrame(id_data)


def getTimeData(flights_data: list) -> pd.DataFrame:
    scheduled_departure_data = [
        i['flight']['time']['scheduled']['departure']
        for i in flights_data
    ]

    real_departure_data = [
        i['flight']['time']['scheduled']['departure']
        for i in flights_data
    ]

    scheduled_arrival_data = [
        i['flight']['time']['scheduled']['arrival']
        for i in flights_data
    ]

    real_arrival_data = [
        i['flight']['time']['real']['arrival']
        for i in flights_data
    ]

    data = {
        'scheduled_departure_time': scheduled_departure_data,
        'real_departure_time': real_departure_data,
        'scheduled_arrival_time': scheduled_arrival_data,
        'real_arrival_time': real_arrival_data
    }

    return pd.DataFrame(data)

def getDestinationData(flights_data: list) -> pd.DataFrame:
    destination_iata_data = [
        i['flight']['airport']['destination']['code']['iata']
        for i in flights_data
    ]

    destination_icao_data = [
        i['flight']['airport']['destination']['code']['icao']
        for i in flights_data
    ]

    destination_name_data = [
        i['flight']['airport']['destination']['name']
        for i in flights_data
    ]

    data = {
        'destination_iata': destination_iata_data,
        'destination_icao': destination_icao_data,
        'destination_name': destination_name_data
    }

    return pd.DataFrame(data)

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

data = getAPIResponse()

s3_prefix_raw = 's3://projeto-de-mentoria/data/raw/'
filename = '{}.json'.format(date.today())

# S3 resource object
s3 = boto3.resource(
    's3',
    region_name='us-east-1',
    aws_access_key_id=aws_key,
    aws_secret_access_key=aws_secret_key
)

# Boto3 object for the raw API data
s3object = s3.Object(
    'projeto-de-mentoria',
    s3_prefix_raw + filename
)

# Upload raw data to S3
s3object.put(
    Body=(bytes(json.dumps(data).encode('UTF-8')))
)

# flights list
flights_data_response = [ i for i in data['airport']['pluginData']['schedule']['departures']['data'] ]

df1 = getIdentificationData(flights_data_response)
df2 = getTimeData(flights_data_response)
df3 = getDestinationData(flights_data_response)
df = pd.concat([df1, df2, df3], axis=1)

s3_prefix_transformed = 's3://projeto-de-mentoria/data/transformed/'
filename = '{}.csv'.format(date.today())
wr.s3.to_csv(
    df=df,
    path=s3_prefix_transformed + filename,
    index=False,
    boto3_session=session
)
