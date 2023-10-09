import os
import pandas as pd
import psycopg2
import json

path1 = "C:/Users/YOGA/Downloads/github/wetransfer_pulse_2023-09-27_1431/pulse/data/aggregated/transaction/country/india/state/"
agg_trans_list = os.listdir(path1)

columns1 = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_count': [],'Transaction_amount': []}
for i in agg_trans_list:
  state_i = os.path.join(path1,i)
  for j in os.listdir(state_i):
    year_j = os.path.join(state_i,j)
    for k in os.listdir(year_j):
      quarter_k = os.path.join(year_j,k)
      with open(quarter_k, 'r') as data_file:
        D = json.load(data_file)
        for item in D['data']['transactionData']:
            name = item['name']
            count = item['paymentInstruments'][0]['count']
            amount = item['paymentInstruments'][0]['amount']
            columns1['Transaction_type'].append(name)
            columns1['Transaction_count'].append(count)
            columns1['Transaction_amount'].append(amount)
            columns1['State'].append(i)
            columns1['Year'].append(j)
            columns1['Quarter'].append(int(k.strip('.json')))

df_agg_trans = pd.DataFrame(columns1)

path2 = "C:/Users/YOGA/Downloads/github/wetransfer_pulse_2023-09-27_1431/pulse/data/aggregated/user/country/india/state/"

agg_user_list = os.listdir(path2)

columns2 = {'State': [], 'Year': [], 'Quarter': [], 'Brands': [], 'Count': [],'Percentage': []}

for i in agg_user_list:
  state_i = os.path.join(path2,i)
  for j in os.listdir(state_i):
    year_j = os.path.join(state_i,j)
    for k in os.listdir(year_j):
      quarter_k = os.path.join(year_j,k)
      with open(quarter_k, 'r') as data_file:
        D = json.load(data_file)
        try:
            for item in D['data']['usersByDevice']:
                brand_name = item["brand"]
                counts = item["count"]
                percents = item["percentage"]
                columns2["Brands"].append(brand_name)
                columns2["Count"].append(counts)
                columns2["Percentage"].append(percents)
                columns2["State"].append(i)
                columns2["Year"].append(j)
                columns2["Quarter"].append(int(k.strip('.json')))
        except:
            pass
df_agg_user = pd.DataFrame(columns2)

path3 = "C:/Users/YOGA/Downloads/github/wetransfer_pulse_2023-09-27_1431/pulse/data/map/transaction/hover/country/india/state/"

map_trans_list = os.listdir(path3)

columns3 = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Count': [],'Amount': []}

for i in map_trans_list:
  state_i = os.path.join(path3,i)
  for j in os.listdir(state_i):
    year_j = os.path.join(state_i,j)
    for k in os.listdir(year_j):
      quarter_k = os.path.join(year_j,k)
      with open(quarter_k, 'r') as data_file:
        D = json.load(data_file)
        try:
          for item in D['data']['hoverDataList']:
                district = item["name"]
                count = item["metric"][0]["count"]
                amount = item["metric"][0]["amount"]
                columns3["District"].append(district)
                columns3["Count"].append(count)
                columns3["Amount"].append(amount)
                columns3['State'].append(i)
                columns3['Year'].append(j)
                columns3['Quarter'].append(int(k.strip('.json')))
        except:
           pass        

df_map_trans = pd.DataFrame(columns3)



path4 = "C:/Users/YOGA/Downloads/github/wetransfer_pulse_2023-09-27_1431/pulse/data/map/user/hover/country/india/state/"

map_user_list = os.listdir(path4)

columns4 = {"State": [], "Year": [], "Quarter": [], "District": [],"RegisteredUser": [], "AppOpens": []}

for i in map_user_list:
  state_i = os.path.join(path4, i)
  for j in os.listdir(state_i):
    year_j = os.path.join(state_i, j)
    for k in os.listdir(year_j):
      quarter_k = os.path.join(year_j, k)
      with open (quarter_k, 'r') as datafile:
        D = json.load(datafile)
        try:
          for item in D['data']['hoverData'].items():
            district = item[0]
            registereduser = item[1]["registeredUsers"]
            appOpens = item[1]['appOpens']
            columns4["District"].append(district)
            columns4["RegisteredUser"].append(registereduser)
            columns4["AppOpens"].append(appOpens)
            columns4['State'].append(i)
            columns4['Year'].append(j)
            columns4['Quarter'].append(int(k.strip('.json')))
        except:
           pass

df_map_user = pd.DataFrame(columns4)

path5 = "C:/Users/YOGA/Downloads/github/wetransfer_pulse_2023-09-27_1431/pulse/data/top/transaction/country/india/state/"

top_trans_list = os.listdir(path5)
columns5 = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [], 'Transaction_count': [],'Transaction_amount': []}

for i in top_trans_list:
  state_i = os.path.join(path5, i)
  for j in os.listdir(state_i):
    year_j = os.path.join(state_i, j)
    for k in os.listdir(year_j):
      quarter_k = os.path.join(year_j, k)
      with open(quarter_k,'r') as datafile:
        D = json.load(datafile)
        try:
          for item in D['data']['pincodes']:
            name = item['entityName']
            count = item['metric']['count']
            amount = item['metric']['amount']
            columns5['Pincode'].append(name)
            columns5['Transaction_count'].append(count)
            columns5['Transaction_amount'].append(amount)
            columns5['State'].append(i)
            columns5['Year'].append(j)
            columns5['Quarter'].append(int(k.strip('.json')))
        except:
           pass

df_top_trans = pd.DataFrame(columns5)

path6 = "C:/Users/YOGA/Downloads/github/wetransfer_pulse_2023-09-27_1431/pulse/data/top/user/country/india/state/"
top_user_list = os.listdir(path6)
columns6 = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [],'RegisteredUsers': []}

for i in top_user_list:
  state_i = os.path.join(path6, i)
  for j in os.listdir(state_i):
    year_j = os.path.join(state_i, j)
    for k in os.listdir(year_j):
      quarter_k = os.path.join(year_j, k)
      with open(quarter_k,'r') as datafile:
        D = json.load(datafile)
        try:
          for item in D['data']['pincodes']:
            name = item['name']
            registeredUsers = item['registeredUsers']
            columns6['Pincode'].append(name)
            columns6['RegisteredUsers'].append(registeredUsers)
            columns6['State'].append(i)
            columns6['Year'].append(j)
            columns6['Quarter'].append(int(k.strip('.json')))
        except:
           pass

df_top_user = pd.DataFrame(columns6)


df_agg_trans.to_csv('agg_trans.csv',index=False)
df_agg_user.to_csv('agg_user.csv',index=False)
df_map_trans.to_csv('map_trans.csv',index=False)
df_map_user.to_csv('map_user.csv',index=False)
df_top_trans.to_csv('top_trans.csv',index=False)
df_top_user.to_csv('top_user.csv',index=False)



db=psycopg2.connect(host='localhost', user='postgres', password="nivi", port=5432, database="pp3")
cursor=db.cursor()

cursor.execute("create table agg_trans (State varchar, Year int, Quarter int, Transaction_type varchar, Transaction_count bigint, Transaction_amount bigint)")
for item,row in df_agg_trans.iterrows():
    query = "INSERT INTO agg_trans VALUES (%s,%s,%s,%s,%s,%s)"
    cursor.execute(query, tuple(row))
    db.commit()

cursor.execute("create table agg_user (State varchar, Year int, Quarter int, Brands varchar, Count int, Percentage bigint)")
for item,row in df_agg_user.iterrows():
    query = "INSERT INTO agg_user VALUES (%s,%s,%s,%s,%s,%s)"
    cursor.execute(query, tuple(row))
    db.commit()

cursor.execute("create table map_trans (State varchar, Year int, Quarter int, District varchar, Count int, Amount bigint)")

for item,row in df_map_trans.iterrows():
    query = "INSERT INTO map_trans VALUES (%s,%s,%s,%s,%s,%s)"
    cursor.execute(query, tuple(row))
    db.commit()
cursor.execute("create table map_user (State varchar, Year int, Quarter int, District varchar, Registered_user int, App_opens int)")

for item,row in df_map_user.iterrows():
    query = "INSERT INTO map_user VALUES (%s,%s,%s,%s,%s,%s)"
    cursor.execute(query, tuple(row))
    db.commit()

cursor.execute("create table top_trans (State varchar, Year int, Quarter int, Pincode int, Transaction_count int, Transaction_amount bigint)")

for item,row in df_top_trans.iterrows():
    query = "INSERT INTO top_trans VALUES (%s,%s,%s,%s,%s,%s)"
    cursor.execute(query, tuple(row))
    db.commit()

cursor.execute("create table top_user (State varchar, Year int, Quarter int, Pincode int, Registered_users bigint)")

for item,row in df_top_user.iterrows():
    query = "INSERT INTO top_user VALUES (%s,%s,%s,%s,%s)"
    cursor.execute(query, tuple(row))
    db.commit()