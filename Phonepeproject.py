import streamlit as st
import pymysql
import json
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import text
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from PIL import Image

#Connecting SQL
host='localhost'
user='root'
password='Nivi1234'
port=3306
database='phonepe'

engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')

connection=engine.connect()

#create table and loading data into tables
#create aggregated transaction table

create_table_sql = """CREATE TABLE IF NOT EXISTS aggregated_transaction (
    serial_no SERIAL PRIMARY KEY,
    state VARCHAR(300) NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    transaction_type VARCHAR(300) NOT NULL,
    transaction_count INT NOT NULL,
    transaction_amount FLOAT NOT NULL
);
"""

connection.execute(text(create_table_sql))
df_agg_trans = pd.read_csv(r"C:\Users\YOGA\Downloads\phonepecsvfile\agg_trans_dataframe.csv")
df_agg_trans.to_sql(name='aggregated_transaction' ,con=engine ,if_exists ='replace',index = False)


#create aggregated users table
create_table_sql = """CREATE TABLE IF NOT EXISTS aggregated_users (
    state VARCHAR(255) NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    device_brand VARCHAR(255) NOT NULL,
    count INT NOT NULL,
    percentage FLOAT NOT NULL
);
"""
connection.execute(text(create_table_sql))
df_agg_users=pd.read_csv(r"C:\Users\YOGA\Downloads\phonepecsvfile\agg_user_dataframe.csv")
df_agg_users.to_sql(name='aggregated_users',con=engine,if_exists='replace',index=False)

#create map_transactions table
create_table_sql = """CREATE TABLE IF NOT EXISTS map_transactions (
    state VARCHAR(255) NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    district VARCHAR(255) NOT NULL,
    count INT NOT NULL,
    amount FLOAT NOT NULL
);
"""
connection.execute(text(create_table_sql))
df_map_trans=pd.read_csv(r"C:\Users\YOGA\Downloads\phonepecsvfile\map_trans_dataframe.csv")
df_map_trans.to_sql(name='map_transactions',con=engine,if_exists='replace',index=False)


#create map user table
create_table_sql = """CREATE TABLE IF NOT EXISTS map_users (
    state VARCHAR(255) NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    district VARCHAR(255) NOT NULL,
    registered_users INT NOT NULL
     
    );
"""
connection.execute(text(create_table_sql))
df_map_users=pd.read_csv(r"C:\Users\YOGA\Downloads\phonepecsvfile\map_user_dataframe.csv")
df_map_users.to_sql(name='map_users',con=engine,if_exists='replace',index=False)


#create top  transactions table
create_table_sql = """CREATE TABLE IF NOT EXISTS top_transactions (
    state VARCHAR(255) NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    district INT NOT NULL,
    count INT NOT NULL,
    amount FLOAT NOT NULL
    );
"""

connection.execute(text(create_table_sql))
df_top_trans=pd.read_csv(r"C:\Users\YOGA\Downloads\phonepecsvfile\top_trans_dataframe.csv")
df_top_trans.to_sql(name='top_transactions',con=engine,if_exists='replace',index=False)


#create top_user table
create_table_sql = """CREATE TABLE IF NOT EXISTS top_users (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    state VARCHAR(255) NOT NULL,
    Year INT NOT NULL,
    quarter INT NOT NULL,
    district INT NOT NULL,
    registered_user INT NOT NULL
    );
"""
connection.execute(text(create_table_sql))
df_top_users=pd.read_csv(r"C:\Users\YOGA\Downloads\phonepecsvfile\top_user_dataframe.csv")
df_top_users.to_sql(name='top_users',con=engine,if_exists='replace',index=False)

#*******************************

host='localhost'
user='root'
password='Nivi1234'
port=3306
database='phonepe'

engine =sqlalchemy.create_engine(f"mysql://{user}:{password}@{host}:{port}/{database}")
connection=engine.connect()

#fetch all tables data from mysql
table_name = "aggregated_transaction"
df_agg_trans = pd.read_sql_table(table_name, connection)

table_name = "aggregated_users"
df_agg_users = pd.read_sql_table(table_name, connection)

table_name = "map_transactions"
df_map_trans = pd.read_sql_table(table_name, connection)

table_name = "map_users"
df_map_users = pd.read_sql_table(table_name, connection)

table_name = "top_transactions"
df_top_trans = pd.read_sql_table(table_name, connection)

table_name = "top_users"
df_top_users = pd.read_sql_table(table_name, connection)

#************************************

st.set_page_config(page_title='PhonePe India Dashboards', layout='wide')
image_input=Image.open(r"C:\Users\YOGA\OneDrive\Pictures\PhonePe-Logo.wine.webp")
st.title('&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;**PhonePe India Dashboard**')
st.image(image_input)

add_selectbox =st.selectbox(
    "**Select an option**",
    ("","user","transaction",)
    )

if add_selectbox =='user' or 'transaction':
    year=st.selectbox('**Select an  year**',('',2018,2019,2020,2021,2022))
    if year==2018:
        quarter=st.selectbox('Select quarter',('','Q1(Jan-March)','Q2(Apr-June)','Q3(July-Sep)','Q4(Oct-Dec)'))
    elif year==2019:
        quarter=st.selectbox('Select quarter',('','Q1(Jan-March)','Q2(Apr-June)','Q3(July-Sep)','Q4(Oct-Dec)'))
    elif year==2020:    
        quarter=st.selectbox('Select quarter',('','Q1(Jan-March)','Q2(Apr-June)','Q3(July-Sep)','Q4(Oct-Dec)'))
    elif year==2021:
        quarter=st.selectbox('Select quarter',('','Q1(Jan-March)','Q2(Apr-June)','Q3(July-Sep)','Q4(Oct-Dec)'))
    elif year==2022:
        quarter=st.selectbox('Select quarter',('','Q1(Jan-March)','Q2(Apr-June)','Q3(July-Sep)','Q4(Oct-Dec)'))

if add_selectbox == 'user':
    if year == 2018:
        if quarter == 'Q1(Jan-March)':
            filtered_df = df_agg_users[(df_agg_users['Year'] == 2018) & (df_agg_users['Quarter'] == 1)]

            fig1=px.bar(filtered_df,x='Device_brand',y='Count',hover_name='State',color='State',range_y=(0,13000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=800,height=650,)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Device_brand',values='Count')
            fig_f = fig2.update_layout(width=800,height=650,)
            st.plotly_chart(fig_f)
        elif quarter == 'Q2(Apr-June)':
            filtered_df = df_agg_users[(df_agg_users['Year'] == 2018) & (df_agg_users['Quarter'] == 2)]

            fig1=px.bar(filtered_df,x='Device_brand',y='Count',hover_name='State',color='State',range_y=(0,13000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=800,height=650,)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Device_brand',values='Count')
            fig_f = fig2.update_layout(width=800,height=650,)
            st.plotly_chart(fig_f)        
        elif quarter == 'Q2(Apr-June)':
            filtered_df = df_agg_users[(df_agg_users['Year'] == 2018) & (df_agg_users['Quarter'] == 2)]

            fig1=px.bar(filtered_df,x='Device_brand',y='Count',hover_name='State',color='State',range_y=(0,13000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=800,height=650,)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Device_brand',values='Count')
            fig_f = fig2.update_layout(width=800,height=650,)
            st.plotly_chart(fig_f)           
        elif quarter == 'Q3(July-Sep)':
            filtered_df = df_agg_users[(df_agg_users['Year'] == 2018) & (df_agg_users['Quarter'] == 3)]

            fig1=px.bar(filtered_df,x='Device_brand',y='Count',hover_name='State',color='State',range_y=(0,13000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=800,height=650,)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Device_brand',values='Count')
            fig_f = fig2.update_layout(width=800,height=650,)
            st.plotly_chart(fig_f)
        elif quarter == 'Q4(Oct-Dec)':
            filtered_df = df_agg_users[(df_agg_users['Year'] == 2018) & (df_agg_users['Quarter'] == 4)]

            fig1=px.bar(filtered_df,x='Device_brand',y='Count',hover_name='State',color='State',range_y=(0,13000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=800,height=650,)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Device_brand',values='Count')
            fig_f = fig2.update_layout(width=800,height=650,)
            st.plotly_chart(fig_f)    
    if year == 2019:
        if quarter == 'Q1(Jan-March)':
            filtered_df = df_agg_users[(df_agg_users['Year'] == 2019) & (df_agg_users['Quarter'] == 1)]

            fig1=px.bar(filtered_df,x='Device_brand',y='Count',hover_name='State',color='State',range_y=(0,13000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=800,height=650,)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Device_brand',values='Count')
            fig_f = fig2.update_layout(width=800,height=650,)
            st.plotly_chart(fig_f)  
        elif quarter == 'Q2(Apr-June)':
            filtered_df = df_agg_users[(df_agg_users['Year'] == 2019) & (df_agg_users['Quarter'] == 2)]

            fig1=px.bar(filtered_df,x='Device_brand',y='Count',hover_name='State',color='State',range_y=(0,13000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=800,height=650,)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Device_brand',values='Count')
            fig_f = fig2.update_layout(width=800,height=650,)
            st.plotly_chart(fig_f)
        elif quarter == 'Q3(July-Sep)':
            filtered_df = df_agg_users[(df_agg_users['Year'] == 2019) & (df_agg_users['Quarter'] ==3)]

            fig1=px.bar(filtered_df,x='Device_brand',y='Count',hover_name='State',color='State',range_y=(0,13000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=800,height=650,)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Device_brand',values='Count')
            fig_f = fig2.update_layout(width=800,height=650,)
            st.plotly_chart(fig_f)    
        elif quarter == 'Q4(Oct-Dec)':
            filtered_df = df_agg_users[(df_agg_users['Year'] == 2019) & (df_agg_users['Quarter'] == 4)]

            fig1=px.bar(filtered_df,x='Device_brand',y='Count',hover_name='State',color='State',range_y=(0,13000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=800,height=650,)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Device_brand',values='Count')
            fig_f = fig2.update_layout(width=800,height=650,)
            st.plotly_chart(fig_f)
   
    if year == 2020:
        if quarter == 'Q1(Jan-March)':
            filtered_df = df_agg_users[(df_agg_users['Year'] == 2020) & (df_agg_users['Quarter'] == 1)]

            fig1=px.bar(filtered_df,x='Device_brand',y='Count',hover_name='State',color='State',range_y=(0,13000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=800,height=650,)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Device_brand',values='Count')
            fig_f = fig2.update_layout(width=800,height=650,)
            st.plotly_chart(fig_f)
        elif quarter == 'Q2(Apr-June)':
            filtered_df = df_agg_users[(df_agg_users['Year'] == 2020) & (df_agg_users['Quarter'] == 2)]

            fig1=px.bar(filtered_df,x='Device_brand',y='Count',hover_name='State',color='State',range_y=(0,13000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=800,height=650,)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Device_brand',values='Count')
            fig_f = fig2.update_layout(width=800,height=650,)
            st.plotly_chart(fig_f)    
        elif quarter == 'Q3(July-Sep)':
            filtered_df = df_agg_users[(df_agg_users['Year'] == 2020) & (df_agg_users['Quarter'] == 3)]

            fig1=px.bar(filtered_df,x='Device_brand',y='Count',hover_name='State',color='State',range_y=(0,13000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=800,height=650,)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Device_brand',values='Count')
            fig_f = fig2.update_layout(width=800,height=650,)
            st.plotly_chart(fig_f)
        elif quarter == 'Q4(Oct-Dec)':
            filtered_df = df_agg_users[(df_agg_users['Year'] == 2020) & (df_agg_users['Quarter'] == 4)]

            fig1=px.bar(filtered_df,x='Device_brand',y='Count',hover_name='State',color='State',range_y=(0,13000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=800,height=650,)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Device_brand',values='Count')
            fig_f = fig2.update_layout(width=800,height=650,)
            st.plotly_chart(fig_f)

    if year == 2021:
        if quarter == 'Q1(Jan-March)':
            filtered_df = df_agg_users[(df_agg_users['Year'] == 2021) & (df_agg_users['Quarter'] == 1)]

            fig1=px.bar(filtered_df,x='Device_brand',y='Count',hover_name='State',color='State',range_y=(0,13000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=800,height=650,)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Device_brand',values='Count')
            fig_f = fig2.update_layout(width=800,height=650,)
            st.plotly_chart(fig_f)
        elif quarter == 'Q2(Apr-June)':
            filtered_df = df_agg_users[(df_agg_users['Year'] == 2021) & (df_agg_users['Quarter'] == 2)]

            fig1=px.bar(filtered_df,x='Device_brand',y='Count',hover_name='State',color='State',range_y=(0,13000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=800,height=650,)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Device_brand',values='Count')
            fig_f = fig2.update_layout(width=800,height=650,)
            st.plotly_chart(fig_f)
        elif quarter == 'Q3(July-Sep)':
            filtered_df = df_agg_users[(df_agg_users['Year'] == 2021) & (df_agg_users['Quarter'] == 3)]

            fig1=px.bar(filtered_df,x='Device_brand',y='Count',hover_name='State',color='State',range_y=(0,13000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=800,height=650,)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Device_brand',values='Count')
            fig_f = fig2.update_layout(width=800,height=650,)
            st.plotly_chart(fig_f)
        elif quarter == 'Q4(Oct-Dec)':
            filtered_df = df_agg_users[(df_agg_users['Year'] == 2021) & (df_agg_users['Quarter'] == 4)]

            fig1=px.bar(filtered_df,x='Device_brand',y='Count',hover_name='State',color='State',range_y=(0,13000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=800,height=650,)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Device_brand',values='Count')
            fig_f = fig2.update_layout(width=800,height=650,)
            st.plotly_chart(fig_f)

    if year == 2022:
        if quarter == 'Q1(Jan-March)':
            filtered_df = df_agg_users[(df_agg_users['Year'] == 2022) & (df_agg_users['Quarter'] == 1)]

            fig1=px.bar(filtered_df,x='Device_brand',y='Count',hover_name='State',color='State',range_y=(0,13000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=800,height=650,)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Device_brand',values='Count')
            fig_f = fig2.update_layout(width=800,height=650,)
            st.plotly_chart(fig_f)
        elif quarter == 'Q2(Apr-June)':
            filtered_df = df_agg_users[(df_agg_users['Year'] == 2022) & (df_agg_users['Quarter'] == 2)]

            fig1=px.bar(filtered_df,x='Device_brand',y='Count',hover_name='State',color='State',range_y=(0,13000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=800,height=650,)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Device_brand',values='Count')
            fig_f = fig2.update_layout(width=800,height=650,)
            st.plotly_chart(fig_f)
        elif quarter == 'Q3(July-Sep)':
            filtered_df = df_agg_users[(df_agg_users['Year'] == 2022) & (df_agg_users['Quarter'] == 3)]

            fig1=px.bar(filtered_df,x='Device_brand',y='Count',hover_name='State',color='State',range_y=(0,13000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=800,height=650,)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Device_brand',values='Count')
            fig_f = fig2.update_layout(width=800,height=650,)
            st.plotly_chart(fig_f) 
        elif quarter == 'Q4(Oct-Dec)':
            filtered_df = df_agg_users[(df_agg_users['Year'] == 2022) & (df_agg_users['Quarter'] == 1)]

            fig1=px.bar(filtered_df,x='Device_brand',y='Count',hover_name='State',color='State',range_y=(0,13000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=800,height=650,)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Device_brand',values='Count')
            fig_f = fig2.update_layout(width=800,height=650,)
            st.plotly_chart(fig_f)

if add_selectbox == 'transaction':
    if year == 2018:
        if quarter == 'Q1(Jan-March)':
            filtered_df=df_agg_trans[(df_agg_trans['Year']==2018)&(df_agg_trans['Quarter']==1)]

            fig1=px.bar(filtered_df,x='Transaction_type',y='Transaction_count',hover_name='Transaction_amt',color='State',range_y=(0,1000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=600,height=450,)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Transaction_type',values='Transaction_count')
            fig_f = fig2.update_layout(width=800,height=650)
            st.plotly_chart(fig_f)
        elif quarter == 'Q2(Apr-June)':
            filtered_df=df_agg_trans[(df_agg_trans['Year']==2018)&(df_agg_trans['Quarter']==2)]

            fig1=px.bar(filtered_df,x='Transaction_type',y='Transaction_count',hover_name='Transaction_amt',color='State',range_y=(0,1000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=600,height=450)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Transaction_type',values='Transaction_count')
            fig_f = fig2.update_layout(width=800,height=650)
            st.plotly_chart(fig_f)
        elif quarter == 'Q3(July-Sep)':
            filtered_df=df_agg_trans[(df_agg_trans['Year']==2018)&(df_agg_trans['Quarter']==3)]

            fig1=px.bar(filtered_df,x='Transaction_type',y='Transaction_count',hover_name='Transaction_amt',color='State',range_y=(0,1000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=600,height=450)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Transaction_type',values='Transaction_count')
            fig_f = fig2.update_layout(width=800,height=650)
            st.plotly_chart(fig_f)
        elif quarter == 'Q4(Oct-Dec)':
            filtered_df=df_agg_trans[(df_agg_trans['Year']==2018)&(df_agg_trans['Quarter']==4)]

            fig1=px.bar(filtered_df,x='Transaction_type',y='Transaction_count',hover_name='Transaction_amt',color='State',range_y=(0,1000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=600,height=450)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Transaction_type',values='Transaction_count')
            fig_f = fig2.update_layout(width=800,height=650)
            st.plotly_chart(fig_f)
    if year == 2019:
        if quarter == 'Q1(Jan-March)':
            filtered_df=df_agg_trans[(df_agg_trans['Year']==2019)&(df_agg_trans['Quarter']==1)]

            fig1=px.bar(filtered_df,x='Transaction_type',y='Transaction_count',hover_name='Transaction_amt',color='State',range_y=(0,1000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=600,height=450)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Transaction_type',values='Transaction_count')
            fig_f = fig2.update_layout(width=800,height=650)
            st.plotly_chart(fig_f)
        elif quarter == 'Q2(Apr-June)':
            filtered_df=df_agg_trans[(df_agg_trans['Year']==2019)&(df_agg_trans['Quarter']==2)]

            fig1=px.bar(filtered_df,x='Transaction_type',y='Transaction_count',hover_name='Transaction_amt',color='State',range_y=(0,1000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=600,height=450)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Transaction_type',values='Transaction_count')
            fig_f = fig2.update_layout(width=800,height=650)
            st.plotly_chart(fig_f)
        elif quarter == 'Q3(July-Sep)':
            filtered_df=df_agg_trans[(df_agg_trans['Year']==2019)&(df_agg_trans['Quarter']==3)]

            fig1=px.bar(filtered_df,x='Transaction_type',y='Transaction_count',hover_name='Transaction_amt',color='State',range_y=(0,1000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=600,height=450)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Transaction_type',values='Transaction_count')
            fig_f = fig2.update_layout(width=800,height=650)
            st.plotly_chart(fig_f)
        elif quarter == 'Q4(Oct-Dec)':
            filtered_df=df_agg_trans[(df_agg_trans['Year']==2019)&(df_agg_trans['Quarter']==4)]

            fig1=px.bar(filtered_df,x='Transaction_type',y='Transaction_count',hover_name='Transaction_amt',color='State',range_y=(0,1000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=600,height=450)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Transaction_type',values='Transaction_count')
            fig_f = fig2.update_layout(width=800,height=650)
            st.plotly_chart(fig_f)
    if year == 2020:
        if quarter == 'Q1(Jan-March)':
            filtered_df=df_agg_trans[(df_agg_trans['Year']==2020)&(df_agg_trans['Quarter']==1)]

            fig1=px.bar(filtered_df,x='Transaction_type',y='Transaction_count',hover_name='Transaction_amt',color='State',range_y=(0,1000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=600,height=450)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Transaction_type',values='Transaction_count')
            fig_f = fig2.update_layout(width=800,height=650)
            st.plotly_chart(fig_f)
        elif quarter == 'Q2(Apr-June)':
            filtered_df=df_agg_trans[(df_agg_trans['Year']==2020)&(df_agg_trans['Quarter']==2)]

            fig1=px.bar(filtered_df,x='Transaction_type',y='Transaction_count',hover_name='Transaction_amt',color='State',range_y=(0,1000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=600,height=450)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Transaction_type',values='Transaction_count')
            fig_f = fig2.update_layout(width=800,height=650)
            st.plotly_chart(fig_f)
        elif quarter == 'Q3(July-Sep)':
            filtered_df=df_agg_trans[(df_agg_trans['Year']==2020)&(df_agg_trans['Quarter']==3)]

            fig1=px.bar(filtered_df,x='Transaction_type',y='Transaction_count',hover_name='Transaction_amt',color='State',range_y=(0,1000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=600,height=450)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Transaction_type',values='Transaction_count')
            fig_f = fig2.update_layout(width=800,height=650)
            st.plotly_chart(fig_f)
        elif quarter == 'Q4(Oct-Dec)':
            filtered_df=df_agg_trans[(df_agg_trans['Year']==2020)&(df_agg_trans['Quarter']==4)]

            fig1=px.bar(filtered_df,x='Transaction_type',y='Transaction_count',hover_name='Transaction_amt',color='State',range_y=(0,1000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=600,height=450)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Transaction_type',values='Transaction_count')
            fig_f = fig2.update_layout(width=800,height=650)
            st.plotly_chart(fig_f)

    if year == 2021:
        if quarter == 'Q1(Jan-March)':
            filtered_df=df_agg_trans[(df_agg_trans['Year']==2021)&(df_agg_trans['Quarter']==1)]

            fig1=px.bar(filtered_df,x='Transaction_type',y='Transaction_count',hover_name='Transaction_amt',color='State',range_y=(0,1000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=600,height=450)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Transaction_type',values='Transaction_count')
            fig_f = fig2.update_layout(width=800,height=650)
            st.plotly_chart(fig_f)
        elif quarter == 'Q2(Apr-June)':
            filtered_df=df_agg_trans[(df_agg_trans['Year']==2021)&(df_agg_trans['Quarter']==2)]

            fig1=px.bar(filtered_df,x='Transaction_type',y='Transaction_count',hover_name='Transaction_amt',color='State',range_y=(0,1000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=600,height=450)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Transaction_type',values='Transaction_count')
            fig_f = fig2.update_layout(width=800,height=650)
            st.plotly_chart(fig_f)
        elif quarter == 'Q3(July-Sep)':
            filtered_df=df_agg_trans[(df_agg_trans['Year']==2021)&(df_agg_trans['Quarter']==3)]

            fig1=px.bar(filtered_df,x='Transaction_type',y='Transaction_count',hover_name='Transaction_amt',color='State',range_y=(0,1000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=600,height=450)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Transaction_type',values='Transaction_count')
            fig_f = fig2.update_layout(width=800,height=650)
            st.plotly_chart(fig_f)
        elif quarter == 'Q4(Oct-Dec)':
            filtered_df=df_agg_trans[(df_agg_trans['Year']==2021)&(df_agg_trans['Quarter']==4)]

            fig2=px.bar(filtered_df,x='Transaction_type',y='Transaction_count',hover_name='Transaction_amt',color='State',range_y=(0,1000000),animation_frame='State')
            fig1_f = fig2.update_layout(width=600,height=450)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Transaction_type',values='Transaction_count')
            fig_f = fig2.update_layout(width=800,height=650)
            st.plotly_chart(fig_f)

    if year == 2022:
        if quarter == 'Q1(Jan-March)':
            filtered_df=df_agg_trans[(df_agg_trans['Year']==2022)&(df_agg_trans['Quarter']==1)]

            fig1=px.bar(filtered_df,x='Transaction_type',y='Transaction_count',hover_name='Transaction_amt',color='State',range_y=(0,1000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=600,height=450)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Transaction_type',values='Transaction_count')
            fig_f = fig2.update_layout(width=800,height=650)
            st.plotly_chart(fig_f)
        elif quarter == 'Q2(Apr-June)':
            filtered_df=df_agg_trans[(df_agg_trans['Year']==2022)&(df_agg_trans['Quarter']==2)]

            fig1=px.bar(filtered_df,x='Transaction_type',y='Transaction_count',hover_name='Transaction_amt',color='State',range_y=(0,1000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=600,height=450)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Transaction_type',values='Transaction_count')
            fig_f = fig2.update_layout(width=800,height=650)
            st.plotly_chart(fig_f)
        elif quarter == 'Q3(July-Sep)':
            filtered_df=df_agg_trans[(df_agg_trans['Year']==2022)&(df_agg_trans['Quarter']==3)]

            fig1=px.bar(filtered_df,x='Transaction_type',y='Transaction_count',hover_name='Transaction_amt',color='State',range_y=(0,1000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=600,height=450)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Transaction_type',values='Transaction_count')
            fig_f = fig2.update_layout(width=800,height=650)
            st.plotly_chart(fig_f)
        elif quarter == 'Q4(Oct-Dec)':
            filtered_df=df_agg_trans[(df_agg_trans['Year']==2022)&(df_agg_trans['Quarter']==4)]

            fig1=px.bar(filtered_df,x='Transaction_type',y='Transaction_count',hover_name='Transaction_amt',color='State',range_y=(0,1000000),animation_frame='State')
            fig1_f = fig1.update_layout(width=600,height=450)
            st.plotly_chart(fig1_f)

            fig2 = px.pie(filtered_df,names='Transaction_type',values='Transaction_count')
            fig_f = fig2.update_layout(width=800,height=650)
            st.plotly_chart(fig_f)

#***************************
ques=st.selectbox('**Select Question**', ('','1. top 10 states by transaction count','2. top states by transaction amount','3. Which transaction type is most commonly used','4. Top 10 Districts by Transaction amt','5. Top 10 Registered Users by District'))
if ques == '1. top 10 states by transaction count':
    state_counts = df_agg_trans.groupby("State")["Transaction_count"].sum().reset_index()
    state_counts = state_counts.sort_values(by="Transaction_count", ascending=False)
    top_10_states = state_counts.head(10)
    st.subheader("Top 10 States by Transaction Count")
    st.dataframe(top_10_states)
    fig = px.bar(top_10_states, x="State", y="Transaction_count")
    st.plotly_chart(fig)
if ques == '2. top states by transaction amount':
    state_counts = df_agg_trans.groupby("State")["Transaction_amt"].sum().reset_index()
    state_counts = state_counts.sort_values(by="Transaction_amt", ascending=False)
    top_10_states = state_counts.head(10)
    st.subheader("Top 10 States by Transaction amt")
    st.dataframe(top_10_states)
    fig = px.bar(top_10_states, x="State", y="Transaction_amt")
    st.plotly_chart(fig)
if  ques == '3. Which transaction type is most commonly used':
    trans_type_counts = df_agg_trans.groupby('Transaction_type')['Transaction_count'].sum().reset_index()
    trans_type_counts = trans_type_counts.sort_values(by='Transaction_count', ascending=False)
    fig = px.bar(trans_type_counts, x='Transaction_type', y='Transaction_count', title='Most used Transaction Types')
    fig.update_xaxes(title_text='Transaction Type')
    fig.update_yaxes(title_text='Transaction Count')
    st.plotly_chart(fig)
if  ques == '4. Top 10 Districts by Transaction amt':
    Districts = df_map_trans.groupby("District")["Amount"].sum().reset_index()
    Districts = Districts.sort_values(by="Amount", ascending=False)
    top_10_Districts = Districts.head(10)
    st.subheader("Top 10 Districts by Amount")
    st.dataframe(top_10_Districts)
    fig = px.pie(top_10_Districts, names="District", values="Amount")
    st.plotly_chart(fig)
if  ques == '5. Top 10 Registered Users by District':
    Registered_Users = df_top_users.groupby('District')['Registered_user'].sum().sort_values(ascending=False).head(10)
    fig = px.bar(Registered_Users, x=Registered_Users.index, y=Registered_Users.values, labels={'x': 'District', 'y': 'Registered Users'})
    st.dataframe(Registered_Users)
    fig.update_layout(title='Top 10 Districts by Registered Users')
    st.plotly_chart(fig)