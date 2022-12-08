import pandas as pd
import configparser
from sqlalchemy import create_engine

def connect_to_rds(user, password):
    rds_host = "projeto-de-mentoria.c7mdccy9eqpd.us-east-1.rds.amazonaws.com/postgres"
    sql_url = "postgresql://{}:{}@{}"
    engine = create_engine(sql_url.format(user, password, rds_host))
    return engine

parser = configparser.ConfigParser()
parser.read("mentorship.conf")
rds_user = parser.get("RDSCREDENTIALS", "user")
rds_password = parser.get("RDSCREDENTIALS", "password")

tablename = "flights"
tablecols = "number_default_data"
create_query = "CREATE TABLE IF NOT EXIST {} ({});"
query = create_query.format(tablename, tablecols)

df = pd.read_csv("2022-12-08.csv")
engine = connect_to_rds(rds_user, rds_password)
with engine.connect() as connection:
    connection.execute("CREATE TABLE IF NOT EXISTS test (id int, name varchar);")
    df.to_sql("test2", connection, index=False)