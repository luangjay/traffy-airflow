from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow import DAG
import os
from pathlib import Path
import mlflow
from kmodes.kmodes import KModes
from bs4 import BeautifulSoup
import json
import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
sns.set()

path = f'{Path.home()}/airflow/outputs'
api_url = 'http://localhost:6969'


def kmode_clustering():
    df = pd.read_csv(
        'https://publicapi.traffy.in.th/share/teamchadchart/download?limit=25000&problem_type_abdul=%E0%B8%88%E0%B8%A3%E0%B8%B2%E0%B8%88%E0%B8%A3')

    df['date'] = pd.to_datetime(df['timestamp']).dt.date
    df['DoW'] = pd.to_datetime(df['timestamp']).dt.day_name()
    df[['longitude', 'latitude']] = df['coords'].str.split(',', expand=True)

    duplicate_row = df.duplicated(['comment', 'coords', 'photo'])
    df = df[~duplicate_row]
    df = df[['ticket_id', 'district', 'date',
             'DoW', 'longitude', 'latitude']].dropna()

    # POPULATION SCRAPING
    YEAR = 2566
    population = requests.get(
        f'http://203.155.220.118/green-parks-admin/reports/report_parks_roof.php?park_year={YEAR}')
    soup = BeautifulSoup(population.text, "lxml")
    tds = soup.find_all('table')[1].select('td')

    dis_title = tds[3].text
    pop_title = tds[5].text
    dis = np.zeros(50, dtype=object)
    pop = np.zeros(50, dtype=object)

    for i in range(50):
        dis[i] = tds[13+9*i].text
        pop[i] = tds[15+9*i].text

    dis_series = pd.Series(dis)
    pop_series = pd.Series(pop)
    frame = {dis_title: dis_series,
             pop_title: pop_series}

    pop_df = pd.DataFrame(frame)

    pop_df_dict = dict(zip(pop_df.iloc[:, 0].apply(
        lambda x: x.lstrip('เขต')), pop_df.iloc[:, 1]))
    pop_district = set(pop_df.iloc[:, 0].apply(
        lambda x: x.lstrip('เขต')).to_list())
    traffy_district = set(df['district'].to_list())
    df = df.loc[~df['district'].isin(traffy_district-pop_district)]
    df_district = df['district'].apply(pop_df_dict.get)
    df_district = df_district.apply(lambda x: x.replace(',', ''))
    df['district_population'] = df_district

    df['DoW'] = df['DoW'].astype('category')
    df['longitude'] = df['longitude'].astype(float)
    df['latitude'] = df['latitude'].astype(float)
    df['date'] = df['date'].astype('datetime64')
    df['district'] = df['district'].astype('category')
    df['district_population'] = df['district_population'].astype(int)

    comp_df = df[['district', 'date', 'DoW', 'district_population']]
    comp_df = comp_df.set_index(df['ticket_id'])

    cost = []
    K = range(2, 10)
    for k in list(K):
        kmode = KModes(n_clusters=k, init="Huang", n_init=5,
                       verbose=1, random_state=2023)
        kmode.fit_predict(comp_df)
        cost.append(kmode.cost_)

    distortions = np.array(cost)
    differences = np.diff(distortions)
    squared_differences = differences ** 2
    cumulative_sum = np.cumsum(squared_differences)
    normalized_cumulative_sum = cumulative_sum / cumulative_sum.sum()
    second_derivative = np.diff(normalized_cumulative_sum, 2)

    elbow_point = np.argmax(second_derivative) + 2

    if not os.path.exists(path):
        os.makedirs(path)

    with mlflow.start_run():
        kmode = KModes(n_clusters=elbow_point,
                       init='Huang', n_init=5, verbose=1)
        cluster = kmode.fit_predict(comp_df)

        cluster_full_df = comp_df.copy()
        cluster_full_df['cluster'] = cluster
        cluster_full_df.to_json(
            f'{path}/cluster.json', orient='index')

        cluster_centroids_df = pd.DataFrame(kmode.cluster_centroids_)
        cluster_centroids_df.columns = [
            'district', 'date', 'DoW', 'district_population']
        cluster_centroids_df.to_json(
            f'{path}/cluster_centroids.json', orient='index')

        mlflow.log_params(kmode.get_params())
        mlflow.log_artifacts(path)

    mlflow.end_run()


def post_request():
    headers = {
        'Content-Type': 'application/json'
    }

    with open(f'{path}/cluster.json', 'r') as file:
        data = json.load(file)
    url = f'{api_url}/data'
    print('POST', url)
    response = requests.post(url, json=data, headers=headers)
    print(response.json())
    response.raise_for_status()

    with open(f'{path}/cluster_centroids.json', 'r') as file:
        data = json.load(file)
    url = f'{api_url}/data-centroids'
    print('POST', url)
    response = requests.post(url, json=data, headers=headers)
    print(response.json())
    response.raise_for_status()


with DAG(
        'update_data', start_date=days_ago(0), schedule_interval='*/60 * * * *', tags=['kmode']) as dag:
    kmode_clustering = PythonOperator(
        task_id='kmode_clustering', python_callable=kmode_clustering)
    post_request = PythonOperator(
        task_id='post_request', python_callable=post_request)

    kmode_clustering >> post_request
