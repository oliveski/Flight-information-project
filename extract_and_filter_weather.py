import json
import boto3
import requests
import configparser
import pandas as pd
import awswrangler as wr
#from datetime import date
import datetime
import pytz

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
    response = requests.get(api_url)
    #Aqui ele testa se a API funcionou. Se não funcionar, irá retornar os valores zerados (não sei se é a melhor escolha)
    if str(response)[-5:-2] == "200":
      return response.text
    else:
      print(response)
      return "\n0;0;0;0;0\n"
 #   api_url = "https://poatek_giongo:mg973XWfQU@api.meteomatics.com/2022-12-03T16:34:00.000-03:00/t_2m:C,visibility:m,is_fog_30min:idx,is_rain_30min:idx/-30.0324999,-51.2303767/csv?model=mix"

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

#Essa função deve retornar um dataframe com os dados do clima (só funciona se o argumento tiver dados das últimas 24 horas)  
def getWeatherDF(df):
  df_weather = pd.DataFrame(columns=['date', 't_2m', 'visibility', 'is_fog', 'is_rain'])
  depTime = [0]
  depTime.extend(df['scheduled_departure_time'].to_list())
  sumTime = 0
  for i in range(len(depTime) - 1):
    timeDifference = depTime[i + 1] - depTime[i] + sumTime
    newTimestamp = datetime.datetime.fromtimestamp(depTime[i], tz=pytz.timezone('Brazil/East')).strftime('%Y-%m-%dT%H:%M:%S.000-03:00')
    if timeDifference >= 1800:
      newImput = getResponseData(getAPIresponse(newTimestamp))
      print("request:", i, "timestamp:", newTimestamp, "newImput:", newImput)
      df_weather.loc[len(df_weather)] = newImput
      sumTime = 0
    else:
      sumTime = sumTime + timeDifference
      df_weather.loc[len(df_weather)] = newImput
  return df_weather


# if lasthorario - horario[dessevoo] > 30min:
#   callweather()
#   lasthorario = horario[dessevoo]


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

today = datetime.datetime.now(pytz.timezone('Brazil/East')).strftime("%Y-%m-%d")

airport_data_object = s3.Object(
    'projeto-de-mentoria',
    'data/transformed/airport/' + f'{today}.csv'
)

df = pd.read_csv(airport_data_object.get()["Body"])
# df = pd.read_csv("../s3_object_get_test.csv")
df = df.sort_values(by="scheduled_departure_time")

#Essa função deve retornar um df com o mesmo número de linhas que o df do Schedule
dfWeather = getWeatherDF(df)
s3_prefix_transformed = 's3://projeto-de-mentoria/data/transformed/weather/'
filename = '{}.csv'.format(datetime.date.today())
wr.s3.to_csv(
    df=dfWeather,
    path=s3_prefix_transformed + filename,
    index=False,
    boto3_session=session
)

print("Test")

#Acho que o próximo passo seria juntar os dois df e fazer o upload