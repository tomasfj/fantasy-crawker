# Databricks notebook source
# MAGIC %pip install requests-html

# COMMAND ----------

# MAGIC %pip install nest_asyncio

# COMMAND ----------

# databricks is already running an event loop, and requests_html also runs an event loop
# but these cannot be nested
# nest_asyncio library solves this issue: https://github.com/erdewit/nest_asyncio
import nest_asyncio
nest_asyncio.apply()

# COMMAND ----------

import requests
from requests_html import HTMLSession
from requests_html import AsyncHTMLSession
from bs4 import BeautifulSoup

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType

schema = StructType ([ \
    StructField("teamN", IntegerType(), True), \
    StructField("name1", StringType(), True ), \
    StructField("name2", StringType(), True ), \
    StructField("name3", StringType(), True ), \
    StructField("name4", StringType(), True ), \
    StructField("name5", StringType(), True ), \
])

# COMMAND ----------

base_url = "https://www.hltv.org/fantasy/243/league/118157/team/"
#url_start_value = 1762516
url_start_value = 1763338 

current_team_value = url_start_value
count_empty_responses = 0
teamsList = []

asession = AsyncHTMLSession()

async def getResp():
    r = await asession.get(url)
    await r.html.arender(timeout=20, sleep=1)
    #item = r.html.find('span')
    
    return r


while( count_empty_responses < 10 ):
    # update url  
    url = base_url + str(current_team_value)
    
    # get html code from request response
    resp = asession.run(getResp)
    html_source = resp[0].html.html
    
    # parse html code
    soup = BeautifulSoup(html_source, "html.parser")
    players = soup.find_all('span', class_='text-ellipsis')
    players = list( set([x.text for x in players]) )
    
    if(players != []):
        count_empty_responses = 0
        
        # save results: sql tables are not permanent in Comunity Edition
        #spark.sql(f"insert into default.funsparkTeams values({current_team_value}, '{players[0]}', '{players[1]}', '{players[2]}', '{players[3]}', '{players[4]}')")  
        # add to teamsList w/ format: [123, 'player1', 'player2', 'player3', 'player4', 'player5']
        teamsList.append([current_team_value] + players)
        #print( '[' + str(len(teamsList)) + '] ' + str(current_team_value) + ' ' + str(players) )
        
        # when 100 teams are found, create a dataframe and save to csv file
        if( len(teamsList) >= 100 ):
            df = spark.createDataFrame(spark.sparkContext.parallelize(teamsList), schema)
            df.repartition(1).write.format("csv").option("header", True).mode("append").save("dbfs:/user/tomas_jeronimo_ubi_pt/fantasyData/funsparkTeams.csv")

            teamsList = []
            #print('team list saved...')
    else:
        count_empty_responses += 1
        #print( '#'+str(current_team_value) + ' not found (count: ' + str(count_empty_responses) + ')' )
    
    current_team_value += 1
    
print("DONE")

# COMMAND ----------

# read data in "dbfs:/user/tomas_jeronimo_ubi_pt/fantasyData/funsparkTeams.csv"
df_read = spark.read.option("header", "true").csv("dbfs:/user/tomas_jeronimo_ubi_pt/fantasyData/funsparkTeams.csv")
display(df_read)

# COMMAND ----------

# remove file
#dbutils.fs.rm('dbfs:/user/tomas_jeronimo_ubi_pt/fantasyData/funsparkTeams.csv', True)

# COMMAND ----------

url = "https://www.hltv.org/fantasy/243/league/118157/team/1763157"

asession = AsyncHTMLSession()

async def getResp():
    r = await asession.get(url)
    await r.html.arender(timeout=20, sleep=1)
    #item = r.html.find('span')
    
    return r

resp = asession.run(getResp)

html_source = resp[0].html.html
#print( html_source )

print( html_source )


# COMMAND ----------

soup = BeautifulSoup(html_source, "html.parser")
players = soup.find_all('span', class_='text-ellipsis')
players = set([x.text for x in players])
print(list(players))


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS fantasyData LOCATION 'dbfs:/user/hive/warehouse/fantasyData.db';
# MAGIC create table fantasyData.funsparkTeams (
# MAGIC   teamN int,
# MAGIC   player1 string,
# MAGIC   player2 string,
# MAGIC   player3 string,
# MAGIC   player4 string,
# MAGIC   player5 string
# MAGIC )

# COMMAND ----------

sql_str = "insert into default.funsparkTeams values(1, 'p1', 'p2', 'p3', 'p4', 'p5')"
spark.sql(sql_str)

# COMMAND ----------

display(dbutils.fs.ls(f"dbfs:/user/hive/warehouse/funsparkteams"))
#/user/hive/warehouse/funsparkteams

# COMMAND ----------

import pandas as pd
lst = [[123, 'nameA', 'nameB'],[456, 'nameC', 'nameD']]
df = pd.DataFrame(lst, columns =['teamN', 'name1', 'name2'])
display(df)

# COMMAND ----------

dbutils.fs.rm(f"dbfs:/user/tomas_jeronimo_ubi_pt/testingcsv", True)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType

lst = [[123, 'nameA', 'nameB'],[456, 'nameC', 'nameD']]

schema = StructType ([ \
    StructField("teamN", IntegerType(), True), \
    StructField("name1", StringType(), True ), \
    StructField("name2", StringType(), True ), \
])

df = spark.createDataFrame(spark.sparkContext.parallelize(lst), schema)
df.display()

df.repartition(1).write.format("csv").option("header", True).mode("override").save("dbfs:/user/tomas_jeronimo_ubi_pt/testingcsv3.csv")

# COMMAND ----------

df_read = spark.read.option("header", "true").csv("dbfs:/user/tomas_jeronimo_ubi_pt/testingcsv3.csv")
display(df_read)

# COMMAND ----------

df_read = spark.read.format("csv").option("header", "true").load("dbfs:/user/tomas_jeronimo_ubi_pt/testingcsv2.csv")
display(df_read)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table funsparkTeams location "dbfs:/user/hive/warehouse/funsparkteams"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from funsparkTeams