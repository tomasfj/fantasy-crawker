# Databricks notebook source
#dbutils.widgets.text("baseUrl", "")
#dbutils.widgets.text("startValue", "")
#dbutils.widgets.text("endValue", "")
#dbutils.widgets.text("outputFilePath", "")

# COMMAND ----------

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

asession = AsyncHTMLSession()
async def getResp():
    r = await asession.get(url)
    await r.html.arender(timeout=20, sleep=1)
    #item = r.html.find('span')
    return r

baseUrl = dbutils.widgets.get("baseUrl")
startValue = int(dbutils.widgets.get("startValue"))
endValue = int(dbutils.widgets.get("endValue"))
outputFilePath = dbutils.widgets.get("outputFilePath")

current_team_value = startValue
count_empty_responses = 0
teamsList = []

while( (count_empty_responses < 10) or (current_team_value == endValue) ):
    # update url  
    url = baseUrl + str(current_team_value)

    # get html code from request response
    resp = asession.run(getResp)
    html_source = resp[0].html.html

    # parse html code
    soup = BeautifulSoup(html_source, "html.parser")
    players = soup.find_all('span', class_='text-ellipsis')
    players = list( set([x.text for x in players]) )

    if(players != []):
        count_empty_responses = 0
            
        # add to teamsList w/ format: [123, 'player1', 'player2', 'player3', 'player4', 'player5']
        teamsList.append([current_team_value] + players)
        print( '[' + str(len(teamsList)) + '] ' + str(current_team_value) + ' ' + str(players) ) 
    else:
        count_empty_responses += 1
        print( '#'+str(current_team_value) + ' not found (count: ' + str(count_empty_responses) + ')' )

    current_team_value += 1
    
df = spark.createDataFrame(spark.sparkContext.parallelize(teamsList), schema)
df.repartition(1).write.format("csv").option("header", True).mode("append").save(outputFilePath)
print('Team list saved to: ' + str(outputFilePath))