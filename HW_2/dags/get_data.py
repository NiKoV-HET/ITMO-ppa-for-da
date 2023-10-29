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


def get_wp_descriptions():
    data = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").get_records(
        """
    select id, academic_plan_in_field_of_study, wp_in_academic_plan, update_ts
    from stg.work_programs 
    """
    )

    target_fields = ["id", "academic_plan_in_field_of_study", "wp_in_academic_plan", "update_ts"]

    df_data = pd.DataFrame(data, columns=target_fields)
    df_data = df_data.sort_values(by=["id", "update_ts"], ascending=[True, False])
    df_data_last = df_data.drop_duplicates(subset="id", keep="first").drop("update_ts", axis=1)

    url_down = "https://op.itmo.ru/api/record/academic_plan/academic_wp_description/all?format=json&page=1"
    page = requests.get(url_down, headers=headers)
    c = json.loads(page.text)["count"]
    for p in range(1, c // 10 + 2):
        url_down = "https://op.itmo.ru/api/record/academic_plan/academic_wp_description/all?format=json&page=" + str(p)
        dt = pendulum.now("Asia/Yekaterinburg").to_iso8601_string()
        page = requests.get(url_down, headers=headers)
        res = json.loads(page.text)["results"]
        for r in res:
            df = pd.DataFrame([r], columns=r.keys())
            df["academic_plan_in_field_of_study"] = df[~df["academic_plan_in_field_of_study"].isna()][
                "academic_plan_in_field_of_study"
            ].apply(lambda st_dict: json.dumps(st_dict))
            df["wp_in_academic_plan"] = df[~df["wp_in_academic_plan"].isna()]["wp_in_academic_plan"].apply(
                lambda st_dict: json.dumps(st_dict)
            )
            df.loc[:, "update_ts"] = dt

            merged = df_data_last.merge(df, how="outer", indicator=True)
            df = merged[merged["_merge"] == "right_only"].drop(columns=["_merge"])

            PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").insert_rows(
                "stg.work_programs", df.values, target_fields=target_fields
            )


def get_structural_units():
    data = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").get_records(
        """
    select fak_id, fak_title, wp_list, update_ts
    from stg.su_wp 
    """
    )

    target_fields = ["fak_id", "fak_title", "wp_list", "update_ts"]

    url_down = "https://op.itmo.ru/api/record/structural/workprogram"
    page = requests.get(url_down, headers=headers)
    res = list(json.loads(page.text))

    df_data = pd.DataFrame(data, columns=target_fields)
    df_data = df_data.sort_values(by=["fak_id", "update_ts"], ascending=[True, False])
    df_data_last = df_data.drop_duplicates(subset="fak_id", keep="first").drop("update_ts", axis=1)

    for su in res:
        df = pd.DataFrame([su])
        dt = pendulum.now("Asia/Yekaterinburg").to_iso8601_string()
        # превращаем последний столбец в json
        df["work_programs"] = df[~df["work_programs"].isna()]["work_programs"].apply(
            lambda st_dict: json.dumps(st_dict)
        )

        df.loc[:, "update_ts"] = dt
        df.columns = target_fields

        merged = df_data_last.merge(df, how="outer", indicator=True)
        df = merged[merged["_merge"] == "right_only"].drop(columns=["_merge"])

        PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").insert_rows(
            "stg.su_wp", df.values, target_fields=target_fields
        )


with DAG(
    dag_id="get_data",
    start_date=pendulum.datetime(2023, 1, 10, tz="UTC"),
    schedule_interval="0 1 * * *",
    catchup=False,
) as dag:
    t1 = PythonOperator(task_id="get_wp_descriptions", python_callable=get_wp_descriptions)
    t2 = PythonOperator(task_id="get_structural_units", python_callable=get_structural_units)

t1 >> t2
