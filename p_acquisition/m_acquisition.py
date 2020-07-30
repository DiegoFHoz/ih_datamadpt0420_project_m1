from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
import pandas as pd

def db_connection (path: str):
    return create_engine(f'sqlite:///{path}', poolclass=StaticPool)

def data_table(db_connection):
    data_table = pd.read_sql_query('SELECT\
                                                country_info.uuid,\
                                                country_info.country_code,\
                                                country_info.rural as habitat,\
                                                career_info.dem_education_level as education_level,\
                                                career_info.normalized_job_code as normalized_job_code,\
                                                personal_info.gender,\
                                                personal_info.age,\
                                                personal_info.age_group \
                                                from country_info\
                                                join career_info \
                                                on country_info.uuid = career_info.uuid\
                                                join personal_info \
                                                on career_info.uuid = personal_info.uuid', con=db_connection)



    data_table.to_csv('data/raw/data_table.csv', index=False)
    print('main_data_table.csv exported...')
    return data_table




def acquire (path):
    engine= db_connection(path)
    first_table=data_table(engine)
    return first_table



