import requests
import pandas as pd
import pendulum
import json
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

url = "https://op.itmo.ru/auth/token/login"
username = Variable.get("username")
password = Variable.get("password")
auth_data = {"username": username, "password": password}

token_txt = requests.post(url, auth_data).text
token = json.loads(token_txt)["auth_token"]
headers = {"Content-Type": "application/json", "Authorization": "Token " + token}


def get_up_detail():
    ids = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").get_records(
        """
    select id as op_id
    from stg.work_programs wp
    """
    )
    target_fields = ["id", "ap_isu_id", "on_check", "laboriousness", "academic_plan_in_field_of_study", "update_ts"]
    data = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").get_records(
        """
    select id, ap_isu_id, on_check, laboriousness, academic_plan_in_field_of_study, update_ts
    from stg.up_detail
    """
    )

    df_data = pd.DataFrame(data, columns=target_fields)
    df_data = df_data.sort_values(by=["id", "update_ts"], ascending=[True, False])
    df_data_last = df_data.drop_duplicates(subset="id", keep="first").drop("update_ts", axis=1)

    url_down = "https://op.itmo.ru/api/academicplan/detail/"
    target_fields = [
        "id",
        "ap_isu_id",
        "on_check",
        "laboriousness",
        "academic_plan_in_field_of_study",
    ]
    for op_id in ids:
        op_id = str(op_id[0])
        dt = pendulum.now().to_iso8601_string("Asia/Yekaterinburg")
        print(op_id)
        url = url_down + op_id + "?format=json"
        page = requests.get(url, headers=headers)
        df = pd.DataFrame.from_dict(page.json(), orient="index")
        df = df.T
        df["academic_plan_in_field_of_study"] = df[~df["academic_plan_in_field_of_study"].isna()][
            "academic_plan_in_field_of_study"
        ].apply(lambda st_dict: json.dumps(st_dict))

        df.loc[:, "update_ts"] = dt
        df = df[target_fields]
        df = df.fillna(-1)

        merged = df_data_last.merge(df, how="outer", indicator=True)
        df = merged[merged["_merge"] == "right_only"].drop(columns=["_merge"])

        df = df[target_fields]
        PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").insert_rows(
            "stg.up_detail", df.values, target_fields=target_fields
        )


with DAG(
    dag_id="get_up_detail",
    start_date=pendulum.datetime(2023, 1, 10, tz="UTC"),
    schedule_interval="0 1 * * *",
    catchup=False,
) as dag:
    t1 = PythonOperator(task_id="get_up_detail", python_callable=get_up_detail)

t1
