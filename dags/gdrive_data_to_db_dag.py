import pandas as pd
from io import StringIO
import re 
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, Float
from sqlalchemy.exc import OperationalError
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from datetime import timedelta

default_args = {
    "owner": "andyquiroz",
    "depends_on_past": False,
    "start_date": datetime(2020, 7, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "gdrive_data_to_db",
    catchup=False,
    max_active_runs=1,
    schedule_interval="@daily",
    default_args=default_args,
)


class gdrive_data_to_db_operator(BaseOperator):
    #variables
    @apply_defaults
    def __init__(self, 
        df=pd.DataFrame().empty,
        df2=pd.DataFrame().empty,
        *args,
        **kwargs):
        #super().__init__(*args, **kwargs)
        self.df = df
        self.df2= df2
        super().__init__(*args, **kwargs)


    def read_data(self):
        #reading the data from the url
        try:
            url='https://drive.google.com/file/d/14JcOSJAWqKOUNyadVZDPm7FplA7XYhrU/view'
            file_id=url.split('/')[-2]
            download_data='https://drive.google.com/uc?id=' + file_id
            df = pd.read_csv(download_data)
            self.df = df
            return df
        except (NameError,TypeError,UnboundLocalError,ValueError) as error:
            print('Declare la variable correcta:',error) 
            raise
        
    def transform_data(self):
        #transforming the data
        try:
            df = self.df
            col = ['origin_coord','destination_coord']
            keywords = ["POINT","(",")"," "]
            float_point = ["."]
            for i in range(len(keywords)):
                df = df[['origin_coord','destination_coord']].apply(lambda x: x.str.replace(keywords[i],'',regex=True))
            for i in range(len(float_point)):
                df = df[['origin_coord','destination_coord']].apply(lambda x: x.str.replace(float_point[i],',',regex=True))
            self.df2 = df
            self.df2[['origin_coord','destination_coord']].astype(str)

            aux_indexes=[]
            aux_indexes_=[]
            for item, item_ in zip(self.df2.origin_coord,self.df2.destination_coord):
                index = item.find(",")
                index_ = item_.find(",")
                aux_indexes.append(index)
                aux_indexes_.append(index_)

            self.df2 = self.df2[['origin_coord','destination_coord']].apply(lambda x: x.str.replace(',','',regex=True))


            new_index_=[]
            new_index__=[]


            for index, item in zip(aux_indexes,self.df2.origin_coord):
                item = item[:index] + ',' + item[index:]
                new_index_.append(item)
                new_index_=list(dict.fromkeys(new_index_))


            for index, item in zip(aux_indexes_,self.df2.destination_coord):
                item = item[:index] + ',' + item[index:]
                new_index__.append(item)
                new_index__= list(dict.fromkeys(new_index__))

            df4 = pd.DataFrame(new_index_).rename(columns={0:'origin_coord'})
            df5 = pd.DataFrame(new_index__).rename(columns={0:'destination_coord'})

            df['origin_coord'] = df4[['origin_coord']]
            df['destination_coord'] = df5[['destination_coord']]
            df = df[['origin_coord','destination_coord']].apply(lambda x: x.str.replace(",",".",regex=True)).astype(float)
            df = df[['origin_coord','destination_coord']].apply(lambda x: x.round(1))
            self.df2 = df
            return self.df2
        except (NameError,TypeError,UnboundLocalError,ValueError) as error:
            print('Declare la variable correcta:',error) 
            raise
       
        
    def update_data(self):
        try:
            
            df2 = self.df2
            df = self.df

            df['origin_coord']= df2[['origin_coord']]
            df['destination_coord']= df2[['destination_coord']]
            df['date'] = pd.to_datetime(df['datetime']).dt.date

            df['day'] = df['date'].apply(lambda x : x.day)
            df['Hour'] = pd.to_datetime(df['datetime']).dt.hour
            df['week'] = df['day'].apply(lambda x: (x-1)//7+1) 
            df2 = df.groupby(['region','origin_coord','destination_coord','datetime'])['region'].agg(['count'], ascending=True).\
                rename(columns={'count':'Total'}).reset_index()
            df2 = df.groupby(['region'])['week'].agg(['mean'], ascending=True).\
                rename(columns={'mean':'weekly_avg'}).reset_index()
            self.df2 = df2

            df3 = pd.merge(left=df,right=df2,how="inner", on=["region"])

            df4 = df3.groupby(['region','origin_coord','destination_coord','Hour','weekly_avg'])['weekly_avg'].\
            agg(['count'],ascending=True).rename(columns={'count':'Total'}).reset_index()
            df4 = df4.drop(['Total'], axis=1)
            self.df = df4
            return df4
        except (NameError,TypeError,UnboundLocalError,ValueError) as error:
            print('Declare la variable correcta:',error) 
            raise
    
    
    def write_data(self):
            
        #load the df to jdbc conection and create the table
        df = self.df
        conn_string = 'postgresql://aquiroz:3LEEKWFAS@astronomer-dev.crgadsewf.us-east-1.redshift.amazonaws.com:5439/jobsite_raw'
        engine = create_engine(conn_string, echo=True)
        engine.connect()

        #se crea la tabla
        meta = MetaData()
        trips_data = Table(
         'trips_data', meta, 
           Column('region', String(60)), 
           Column('origin_coord', Float), 
           Column('destination_coord', Float),
           Column('Hour', Integer), 
           Column('weekly_avg', Float),
            
        )
        meta.create_all(engine)

        #insert the data from the dataframe to the table
        df.to_sql(name ='trips_data', con=engine, index=False, chunksize=500,if_exists='replace')
    

    def execute(self,context):
        self.read_data()
        self.transform_data()
        self.update_data()
        self.write_data()




with dag:

    begin = DummyOperator(task_id="begin")

    gdrive_data_to_db = gdrive_data_to_db_operator(
    task_id='gdrive_data_to_db',
    )

    begin >> gdrive_data_to_db