import boto3
from datetime import datetime, timedelta, date
import pandas as pd
import os
import time
import json
import yaml
import pyathena
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

boto3.setup_default_session(region_name='us-east-2')
client = boto3.client('athena')
s3_client = boto3.client('s3')

data = datetime.now() - timedelta(1)
data = data.strftime('%Y-%m-%d')

path_df = 's3://mmlake-presentation-zone/crm_chat'

AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')

default_args = {
        'owner':'Mario O',
        'depend_on_past': False,
        'start_date': datetime(2020,4,13),
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
}

def main_sql(data):
    return f''' SELECT
                    ma.id,
                    ccc.context,
                    ma.created_at AS datetime_first_client_message_to_aninha,
                    ma.closing_date AS datetime_chat_closed,
                    t.name AS client_service_last_team_in_conversation,
                    ma.survey as survey_id,
                    cmt.slug,
                    ma.uuid,
                    ma.cbid,
                    t.id as team_id,
                    t.company_id,
                    at.id as attendant_id
                FROM
                    lakehouse_roz.chat_manager ma
                LEFT JOIN lakehouse_roz.attendant at ON at.id = ma.attendant_id
                LEFT JOIN lakehouse_roz.team t ON	t.id = at.team_id
                left join lakehouse_roz.chat_context ccc on ma.customer_contact_id = ccc.customer_contact_id
                left join lakehouse_roz.customer_contact con on con.id = ma.customer_contact_id
                left join lakehouse_roz.customer ct on con.customer_id = ct.id
                inner join lakehouse_roz.chat_manager_type cmt on ma.type_id = cmt.id
                where ma.closing_date like '{data}';'''

def sql_transfer():
    return f''' SELECT
                        min(c.created_at) as datetime_client_call_to_transfer, c.chat_manager_id as id
                FROM
                        lakehouse_roz.chat c
                LEFT JOIN lakehouse_roz.chat_type ct ON
                        c.type_id = ct.id
                WHERE
                        ct.name in ('Transfer', 'Transfer259jujuba')
                GROUP BY c.chat_manager_id;'''

def sql_name():
    return f''' select
                        min(c.created_at) as datetime_client_receive_attedant, 
                        att.name as first_attendance, 
                        c.chat_manager_id as id,
                        att.email
                from lakehouse_roz.chat c
                INNER JOIN lakehouse_roz.attendant att ON att.id = c.attendant_id
                where
                    c.attendant_id <> '26f0241d-e673-4a40-9075-5dd6ed4a60af'
                GROUP BY att.name, c.chat_manager_id, att.email, att.id;'''

def sql_first():
    return f''' SELECT
                    min(c.created_at) as datetime_first_attendance_message_to_client,
                    c.chat_manager_id as id
                FROM
                    lakehouse_roz.chat c
                LEFT JOIN lakehouse_roz.chat_type ct ON
                    c.type_id = ct.id
                WHERE
                    ct.name = 'Message'
                    AND c.attendant_id IS NOT NULL
                    GROUP BY c.chat_manager_id;'''

def sql_last():
    return f''' select
                    att.name as last_attendance, c.chat_manager_id as id, max(c.created_at) as max_data
                from
                    lakehouse_roz.chat c
                INNER JOIN lakehouse_roz.attendant att ON
                    att.id = c.attendant_id
                where
                    c.attendant_id <> '26f0241d-e673-4a40-9075-5dd6ed4a60af' 
                GROUP BY att.name, c.chat_manager_id;'''

def sql_closed():
    return f''' select
                    ch.payload,
                    ch.chat_manager_id as id
                from
                    lakehouse_roz.chat ch
                where  ch.payload_command = 'finish';'''

def main():

    #anos = ['2019', '2020']
    #meses = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    #dias = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31']
    print('Iniciando select/insert')
    conn_athena = pyathena.connect(s3_staging_dir='s3://aws-athena-query-results-548745220594-us-east-2/', region_name='us-east-2')


    sql_transferencia = sql_transfer()
    df_transfer = pd.read_sql(sql_transferencia, conn_athena)
    print(df_transfer.head())

    sql_company = sql_name()
    df_company = pd.read_sql(sql_company, conn_athena)
    print(df_company.head())
    df_company = df_company.fillna('')
    df_company['company_name'] = df_company.apply(lambda row: 'MM' if 'madeiramadeira' in row.email else 'DBM' if 'madeiramadeiraunable to rollback' not in row.email else 'Aninha' if row.id is '' else None, axis=1)
    print(df_company.head())

    sql_pri = sql_first()
    df_first = pd.read_sql(sql_pri, conn_athena)
    print(df_first.head())

    sql_fi = sql_last()
    df_last = pd.read_sql(sql_fi, conn_athena)
    print(df_last.head())

    sql_fechado = sql_closed()
    df_closed = pd.read_sql(sql_fechado, conn_athena)

    list_origin=[]
    print('Start DF Closed')
    for i, line in df_closed.iterrows():
        
        context = str(line['payload'])
        try:
            final_dictionary = json.loads(context)
        except:
            try:
                final_dictionary = yaml.load(context)
            except:
                final_dictionary = eval(context)

        try:
            origem = final_dictionary['origin']
        except:
            try:
                origem = final_dictionary[b'origin'].decode()
            except:
                origem = None

        list_origin.append(origem)
    
    df_closed['origin'] = list_origin
    df_closed = df_closed.fillna('')
    df_closed['chat_closed_by'] = df_closed.apply(lambda row: 'Atendente' if row.origin == 'roz' else 'Aninha' if row.origin == 'torbjorn' else 'Cliente' if row.origin == 'bastion' else None, axis=1)
    print(df_closed.head())
                
    #for ano in anos:
    #    for mes in meses:
    #        for dia in dias:
    #            data = f"{ano}-{mes}-{dia}"
    print(data)
    data_used = f'{data}%'
    sql_main = main_sql(data_used)
    df_main = pd.read_sql(sql_main, conn_athena)
    print(df_main.head())

    list_pedido=[]
    list_cpf=[]
    list_id_seller=[]
    list_canal=[]

    for i, line in df_main.iterrows():

        context = str(line['context'])
        try:
            final_dictionary = json.loads(context)
        except:
            try:
                final_dictionary = yaml.load(context)
            except:
                final_dictionary = eval(context)

        try:
            pedido = final_dictionary['pedido']
        except:
            try:
                pedido = final_dictionary[b'pedido'].decode()
            except:
                pedido = None
        list_pedido.append(pedido)

        try:
            cpf_cgc = final_dictionary['cpf_cgc']
        except:
            try:
                cpf_cgc = final_dictionary[b'cpf_cgc'].decode()
            except:
                cpf_cgc = None
        list_cpf.append(cpf_cgc)

        try:
            id_seller = final_dictionary['id_seller']
        except:
            try:
                id_seller = final_dictionary[b'id_seller'].decode()
            except:
                id_seller = None
        list_id_seller.append(id_seller)

        try:
            canal = final_dictionary['canal']
        except:
            try:
                canal = final_dictionary[b'canal'].decode()
            except:
                canal = None
        list_canal.append(canal)

    df_main['pedido'] = list_pedido
    df_main['cpf_cgc'] = list_cpf
    df_main['id_seller'] = list_id_seller
    df_main['canal'] = list_canal
    df_main = df_main.sort_values(by=['id'])
    print(df_main.head())

    print('Start Final')
    final = df_main.join(df_transfer.set_index('id'), on='id')
    final = final.join(df_company.set_index('id'), on='id')
    final = final.join(df_first.set_index('id'), on='id')
    final = final.join(df_last.set_index('id'), on='id')
    final = final.join(df_closed.set_index('id'), on='id')
    print(final.head())
    
    #if final.empty:
    #    continue
    
    final = final.fillna('')
    final = final.astype(str).where(pd.notnull(final), None)
    final.to_parquet(f'{path_df}/{data}/{data}.parquet', compression='gzip')

with DAG('CRM_CHAT_LAKE',
        default_args = default_args,
        schedule_interval = '00 09 * * * ',
        catchup=False) as dag:

        start_task = DummyOperator(
            task_id='start_task')
        main_etl = PythonOperator(
            task_id='main_etl',
            python_callable=main)

        start_task >> main_etl