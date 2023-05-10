# coding: utf-8
import argparse
import datetime
import json

import pyspark.sql.functions as sf
import requests
from pyspark import SparkContext
from sness.datalake.metadata import DatasetMetadata, Zone
from sness.datalake.sness_spark import SnessSpark

arguments = argparse.ArgumentParser()
arguments.add_argument("--api_host", type=str, required=True)
arguments.add_argument("--api_username", type=str, required=True)
arguments.add_argument("--api_password", type=str, required=True)
arguments.add_argument("--environment", type=str, required=True)
args = arguments.parse_args()
lake_environment = args.environment

sc = SparkContext.getOrCreate()
ss = SnessSpark()

DATASET = DatasetMetadata(namespace="nps_solucx", dataset="satisfaction_survey")

CHECKPOINT = DatasetMetadata(namespace=DATASET.namespace, dataset="nps_checkpoint")


def set_last_process(str_last_process, namespace, dataset):
    ckpt_dict = {"last_process": f"""{str_last_process}"""}
    body_ckpt = json.dumps(ckpt_dict)
    df_set = ss.spark.read.json(sc.parallelize([body_ckpt]))

    ss.write.csv(
        df_set.repartition(1),
        environment=lake_environment,
        zone=Zone.TRANSIENT,
        namespace=f"{namespace}",
        dataset=f"{dataset}",
        delimiter="|",
        mode="overwrite",
    )


def get_last_process(namespace, dataset):
    df_get = ss.read.csv(
        environment=lake_environment,
        zone=Zone.TRANSIENT,
        namespace=f"{namespace}",
        dataset=f"{dataset}",
        delimiter="|",
    ).withColumnRenamed("_c0", "last_process")

    time_lp = df_get.select("last_process").collect()

    return time_lp[0].last_process


# params fixos

api_key = args.api_username
token = args.api_password
URL = args.api_host

HEADERS = {
    "x-solucx-api-key": token,
    "Content-Type": "application/x-www-form-urlencoded",
}

HEADERS = {
    "x-solucx-api-key": api_key,
    "Content-Type": "application/x-www-form-urlencoded",
}


last_date = get_last_process(CHECKPOINT.namespace, CHECKPOINT.dataset)
last_date = datetime.datetime.strptime(last_date, "%Y-%m-%d").date()

# subtrair um dia pra
# range de 3 dias

# ultima dt de atualização menos um dia
data_inicio_dt = last_date - datetime.timedelta(days=0)

# range de 3 dias para conulta
data_fim_dt = data_inicio_dt + datetime.timedelta(days=5)
# now
data_hoje = (datetime.datetime.now() - datetime.timedelta(hours=3)).date()

# limitar a ultima data a data de hoje
menor_data = min([data_fim_dt, data_hoje])

# params
data_inicio_str = str(data_inicio_dt)
data_fim_str = str(menor_data)
print(f"""{data_inicio_str} , {data_fim_str}""")


# requisicao api
count = 0
result = []

for n in range(1, 25):
    PARAMS = {
        "type": "6",
        "token": token,
        "date_from": data_inicio_str,
        "date_to": data_fim_str,
        "rows_limit": "9999999",  # ou 100
        "page": n,
        "order_by": "older",
        # "with_comments": "1",
        # "bad_rating": "1"
        # "neutral_rating": "1"
        # "good_rating": "1"
        # "include_anonymous"
    }

    response = requests.request("GET", URL, headers=HEADERS, params=PARAMS)

    while True:
        if response.status_code == 200:
            data = json.loads(response.content.decode("utf-8"))
            data = data["records"]
            result = result + data

            qtd_aval = len(data)
            count = count + 1

            print(f""" Requester : {count} - Qtd. Aval.: {qtd_aval}""")
            break

        else:
            print(response)
            # time.sleep(60)

    if qtd_aval == 0:
        qtd_total_aval = len(result)
        now = datetime.datetime.now()  # .strftime("%d/%m/%Y, %H:%M:%S")
        print(
            f"""Total requests made: " {count} Qtd. Total de Tickets: {qtd_total_aval} ' - ' {now}"""
        )
        break

print(f""" {n} de 25 requisições """)

df = sc.parallelize(result).map(lambda x: json.dumps(x))
df = ss.spark.read.json(df)
df = df.withColumn("datalog", sf.lit(now))

ss.write.json(
    df.repartition(1),
    environment=lake_environment,
    zone=Zone.TRANSIENT,
    namespace=DATASET.namespace,
    dataset=DATASET.dataset,
    mode="append",
)

print(f"""Qtd. Append: {df.count()}""")

set_last_process(data_fim_str, CHECKPOINT.namespace, CHECKPOINT.dataset)
print(f"""set {data_fim_str}""")
