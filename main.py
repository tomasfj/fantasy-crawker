# Databricks notebook source
# MAGIC %pip install bs4

# COMMAND ----------

from bs4 import BeautifulSoup
import csv
import time

# COMMAND ----------



# COMMAND ----------

# try to crawl all data 
urlTeamStartValue = 1763152

baseUrl = "https://www.hltv.org/fantasy/243/league/118157/team/"

currentTeamValue = urlTeamStartValue
allTeamsLists = []
countEmptyResponses = 0

# create .csv file
with open('all-teams-list.csv', mode='w') as f:
  f_writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

  f_writer.writerow( ['team#', 'player1', 'player2', 'player3', 'player4', 'player5'] )

# request data
while(countEmptyResponses < 10):
  startTime = time.time()

  # build url 
  url = baseUrl + str(currentTeamValue)
  html_source = webpageRequests.getHtmlFromUrl(url)

  # parse html response
  soup = BeautifulSoup(html_source, "html.parser")
  teamList = webpageProcessing.getTeamList(soup)

  # request returns data
  if( teamList != [] ):
    countEmptyResponses = 0
    
    allTeamsLists.append(teamList)
    
    with open('all-teams-list.csv', mode='a') as f:
      f_writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

      f_writer.writerow( [str(currentTeamValue), teamList[0], teamList[1], teamList[2], teamList[3], teamList[4]] )

    print('Team#' + str(currentTeamValue) + ' -> ' + str(teamList))
    print( 'Execution time: ' + str(time.time()-startTime) )
  
  # request does not return data
  else:
    countEmptyResponses += 1
    print( 'Team#' + str(currentTeamValue) + ' not found (countEmptyResponses: ' + str(countEmptyResponses) + ')' )

  # increase page number 
  currentTeamValue += 1

print('\nDONE RUNNING')
