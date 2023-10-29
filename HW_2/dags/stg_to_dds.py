import pendulum
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator


def editors():
    PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").run(
        """
    WITH discipline_data AS (
    SELECT
        jsonb_array_elements(wp_list::jsonb) AS discipline
    FROM
        stg.su_wp
    ),
    editors_data AS (
    SELECT
        (jsonb_array_elements(discipline->'editors')->>'id')::integer AS id,
        jsonb_array_elements(discipline->'editors')->>'username' AS username,
        jsonb_array_elements(discipline->'editors')->>'first_name' AS first_name,
        jsonb_array_elements(discipline->'editors')->>'last_name' AS last_name,
        jsonb_array_elements(discipline->'editors')->>'email' AS email,
        jsonb_array_elements(discipline->'editors')->>'isu_number' AS isu_number
    FROM
        discipline_data
    )
    INSERT INTO dds.editors (id, username, first_name, last_name, email, isu_number)
    SELECT DISTINCT ON (id) id, username, first_name, last_name, email, isu_number
    FROM editors_data
    WHERE id IS NOT NULL
    ON CONFLICT (id) DO UPDATE 
    SET 
        username = EXCLUDED.username, 
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name, 
        email = EXCLUDED.email, 
        isu_number = EXCLUDED.isu_number;

    """
    )


def states():
    PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").run(
        """
    truncate dds.states restart identity cascade;
    """
    )
    PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").run(
        """
    INSERT INTO dds.states (cop_state, state_name)
    with t as (select distinct (json_array_elements(wp_in_academic_plan::json)->>'status') as cop_states from stg.work_programs wp)
    select cop_states, 
        case when cop_states ='AC' then 'одобрено' 
                when cop_states ='AR' then 'архив'
                when cop_states ='EX' then 'на экспертизе'
                when cop_states ='RE' then 'на доработке'
                else 'в работе'
        end as state_name
    from t
    ON CONFLICT ON CONSTRAINT state_name_uindex DO UPDATE 
    SET 
        id = EXCLUDED.id, 
        cop_state = EXCLUDED.cop_state;
    """
    )


def up():
    PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").run(
        """
    truncate dds.up restart identity cascade;
    """
    )
    PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").run(
        """
        INSERT INTO dds.up (app_isu_id, on_check, laboriousness, year, qualification, update_ts)
        select ap_isu_id, on_check, laboriousness, (json_array_elements(academic_plan_in_field_of_study::json)->>'year')::integer as year,
        (json_array_elements(academic_plan_in_field_of_study::json)->>'qualification') as qualification,
        update_ts
        from stg.up_detail
    """
    )


def wp():
    PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").run(
        """
    truncate dds.wp restart identity cascade;
    """
    )
    PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").run(
        """
    INSERT INTO dds.wp (wp_id, discipline_code, wp_title, wp_status, unit_id, wp_description, update_ts)
    with wp_desc as (
    select 
        distinct json_array_elements(wp_in_academic_plan::json)->>'id' as wp_id,
        json_array_elements(wp_in_academic_plan::json)->>'discipline_code' as discipline_code,
        json_array_elements(wp_in_academic_plan::json)->>'description' as wp_description,
        json_array_elements(wp_in_academic_plan::json)->>'status' as wp_status,
        update_ts 
    from stg.work_programs wp),
    discipline_data AS (
    SELECT
        fak_id,
        jsonb_array_elements(wp_list::jsonb) AS discipline
    FROM
        stg.su_wp
    ),
    wp_unit as (
    select fak_id,
        discipline::json->>'id' as wp_id,
        discipline::json->>'title' as wp_title,
        (discipline::json->>'discipline_code') as discipline_code
    from discipline_data)
    select distinct
    	wp_desc.wp_id::integer,
        wp_desc.discipline_code,
        wp_unit.wp_title,
        s.id as wp_status, 
        wp_unit.fak_id as unit_id,
        wp_desc.wp_description,
        wp_desc.update_ts
    from wp_desc
    left join wp_unit
    on wp_desc.discipline_code = wp_unit.discipline_code
    left join dds.states s 
    on wp_desc.wp_status = s.cop_state
    """
    )


def wp_inter():
    PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").run(
        """
    truncate dds.wp_editor restart identity cascade;
    """
    )
    PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").run(
        """
    truncate dds.wp_up restart identity cascade;
    """
    )
    PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").run(
        """
    WITH discipline_data AS (
    SELECT
        (jsonb_array_elements(wp_list::jsonb)->>'id')::integer AS wp_id,
        jsonb_array_elements(wp_list::jsonb) AS discipline
    FROM
        stg.su_wp
    ),
    editor_relations AS (
    SELECT
        wp_id,
        (jsonb_array_elements(discipline->'editors')->>'id')::integer AS editor_id
    FROM
        discipline_data
    )
    INSERT INTO dds.wp_editor (wp_id, editor_id)
    SELECT DISTINCT wp_id, editor_id
    FROM editor_relations
    WHERE editor_id IS NOT NULL AND wp_id is NOT NULL
    """
    )

    PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").run(
        """
    INSERT INTO dds.wp_up (wp_id, up_id)
    with t as (
    select id,
        (json_array_elements(wp_in_academic_plan::json)->>'id')::integer as wp_id,
        json_array_elements(academic_plan_in_field_of_study::json)->>'ap_isu_id' as up_id
    from stg.work_programs wp)
    select t.wp_id, (json_array_elements(wp.academic_plan_in_field_of_study::json)->>'ap_isu_id')::integer as up_id from t
    join stg.work_programs wp
    on t.id = wp.id
	WHERE wp_id IS NOT NULL AND up_id is NOT NULL
    """
    )


with DAG(
    dag_id="stg_to_dds",
    start_date=pendulum.datetime(2023, 1, 10, tz="UTC"),
    schedule_interval="0 4 * * *",
    catchup=False,
) as dag:
    t1 = PythonOperator(task_id="editors", python_callable=editors)
    t2 = PythonOperator(task_id="states", python_callable=states)
    t3 = PythonOperator(task_id="up", python_callable=up)
    t4 = PythonOperator(task_id="wp", python_callable=wp)
    t5 = PythonOperator(task_id="wp_inter", python_callable=wp_inter)

[t1, t2] >> t3 >> t4 >> t5
