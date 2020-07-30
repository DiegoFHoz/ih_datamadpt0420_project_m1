import numpy as np
import requests
import pandas as pd
import json
from bs4 import BeautifulSoup
import re

#############################
####### CLEAN SECTION #######
#############################


def age(data_table):
    if len(data_table) == 4:
        x = np.subtract(2016, int(data_table))
        return f'{str(x)} years old'
    else:
        return data_table

def clean_age (data_table):
    data_table['age']=data_table['age'].apply(age)
    return data_table


def clean_cols(data_table):
    data_table['habitat']=data_table['habitat'].str.lower()
    data_table['habitat']=data_table['habitat'].str.replace('city', 'urban').str.replace('countryside', 'non-urban').str.replace('country', 'non-urban').str.replace('non-rural','urban').str.replace('rural','non-urban')
    data_table['gender']=data_table['gender'].str.replace('Fem', 'female')
    data_table['gender']=data_table['gender'].str.lower()
    return data_table

#############################
####### API SECTION #########
#############################


def job_code_filtered(data_table):
    codelist = data_table['normalized_job_code'].unique().tolist()
    return codelist


def api_job_list (data_table):
    jobs_id_list = []
    for job_code in job_code_filtered(data_table):
        req= requests.get(f'http://api.dataatwork.org/v1/jobs/{job_code}')
        jobs_id_list.append(req.json())
    jobs_json= json.dumps(jobs_id_list)
    data_jobs = pd.read_json(jobs_json)
    data_jobs['normalized_job_code'] = data_jobs['uuid']
    dataframe_jobs = data_jobs.drop(columns=['error', 'uuid', 'parent_uuid'])
    dataframe_jobs.to_csv('data/raw/jobs_table.csv', index=False)
    print('jobs_table.csv exported...')
    return dataframe_jobs

def merge (data_table,api_job):
    return pd.merge(data_table, api_job, how='left', on=['normalized_job_code'])

#############################
### WEBSCRAPPING SECTION ####
#############################

def country_parsed():
    html = requests.get('https://ec.europa.eu/eurostat/statistics-explained/index.php/Glossary:Country_codes').content
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find_all('div', {'class':'col-lg-12 col-md-12 col-sm-12 col-xs-12 content-col content article-content'})[0]
    rows = table.find_all('td')
    rows_parsed = [row.text for row in rows]
    return rows_parsed

def country_df():
    country_list=[]
    for i in country_parsed():
        country_list.append(re.sub('^\s|[()]|\n','',i))
    country_clean= [x for x in country_list if x]
    N = 2
    country_def = [country_clean[n:n+N] for n in range(0, len(country_clean), N)]
    country_table = pd.DataFrame.from_records(country_def)
    country_table_def =country_table.rename(columns={0:'country_l', 1:'country_code'})
    country_table_def['country_code']=country_table_def['country_code'].str.replace('EL', 'GR').str.replace('UK','GB')
    country_table_def.to_csv('data/raw/country_table_def.csv', index=False)
    print('country_table_def.csv exported...')
    return country_table_def


def merge_country (data_table,country_df):
    return pd.merge(data_table, country_df, how='left', on=['country_code'])


def wrangling(data_table):
    clean_cols1 = clean_cols(data_table)
    clean_cols2 = clean_age(clean_cols1)
    result1 = merge(clean_cols2,api_job_list(data_table))
    result2 = merge_country(result1, country_df())
    result2.to_csv('data/processed/clean_data_table.csv', index=False)
    print('clean_data_table.csv procesed...')
    return result2



