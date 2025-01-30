import pandas as pd
import csv, sqlite3
import prettytable 
import os

directory = '/Users/mac/Documents/SQL PROJECTS/CRM-archive'
dataframes = []
table_names = []

for filename in os.listdir(directory):
    if filename.endswith('.csv'):
        file_path = os.path.join(directory,filename)
        df = pd.read_csv(file_path)
        dataframes.append(df)
        table_names.append(filename[:-4])
    #print(table_names)
    #print(dataframes)

conn = sqlite3.connect('CRM Database.db')
for df, table_name in zip(dataframes,table_names):
    df.to_sql(table_name,conn,if_exists='replace', index=False)    
cur = conn.cursor()
cur.execute('SELECT * FROM sales_pipeline')
#results = cur.fetchall()
#for row in results[:6]:
    #print(row)

#creating a new table in the CRM Database showing only entries with either won or lost deals

cur.execute('DROP TABLE IF EXISTS sales_pipeline_completed')
cur.execute('CREATE TABLE sales_pipeline_completed AS SELECT * \
            FROM sales_pipeline WHERE deal_stage != "In Progress"')
#cur.execute('SELECT * FROM sales_pipeline_completed')
#results = cur.fetchall()
#for row in results[:6]:
 #   print(row)

#Creating a new csv file from our new table ('sales_pipeline_completed')
data = pd.read_sql_query('SELECT * FROM sales_pipeline_completed',conn)
data.to_csv('sales_pipeline_completed.csv', index=False)

#cur.execute('SELECT name FROM sqlite_master WHERE type = "table"')
#tables = cur.fetchall()
#for table in tables:
#    print(table[0])

#cur.execute('SELECT * FROM sales_pipeline_completed WHERE deal_stage = "In Progress"')
#print(cur.fetchall())

#defining a function to export retrieved data from queries to csv for visualization.
def export(query,filename):
    data = pd.read_sql_query(query,conn)
    data.to_csv(filename+'.csv',index=False)

print('')

#Comparing close value on deals based on each sales agent.
cur.execute('SELECT sales_agent,SUM(close_value) AS total_close_value \
     FROM sales_pipeline_completed GROUP BY sales_agent ORDER BY 2 DESC')
print(cur.fetchall())

print('')

#Retrieving acount with the highest close value
cur.execute('SELECT account, MAX(total_close_value) FROM \
    (SELECT account,SUM(close_value) AS total_close_value \
     FROM sales_pipeline_completed GROUP BY account)')
print(cur.fetchall())

print('')

#number of deals made, lost and won by agent.
query = 'SELECT sales_agent, COUNT(*) AS no_of_deals,\
    COUNT(CASE WHEN deal_stage = "Won" THEN 1 END) AS won_deals,\
    COUNT(CASE WHEN deal_stage = "Lost" THEN 1 END) AS lost_deals\
    FROM sales_pipeline_completed GROUP BY sales_agent'
cur.execute(query)
print(cur.fetchall())


print('')

cur.close()
conn.close()