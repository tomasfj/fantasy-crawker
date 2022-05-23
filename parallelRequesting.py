# Databricks notebook source
import concurrent.futures

# COMMAND ----------

base_url = "https://www.hltv.org/fantasy/243/league/118157/team/"
url_start_value = 1762516
base_output_file_path = "dbfs:/user/tomas_jeronimo_ubi_pt/fantasyData/funsparkTeamsParallelize" 

def runNotebooks(baseUrl, startValue, endValue, outputFilePath):
    dbutils.notebook.run(path="./parallelFunctions",
                        timeout_seconds=0,
                        arguments={"baseUrl": baseUrl, "startValue": startValue, "endValue": endValue, "outputFilePath": outputFilePath})

args = (
    (base_url, 1762516, 1762516+100, base_output_file_path+"_1.csv"),
    (base_url, 1762617, 1762617+100, base_output_file_path+"_2.csv"),
    (base_url, 1762718, 1762718+100, base_output_file_path+"_3.csv"),
    (base_url, 1762819, 1762819+100, base_output_file_path+"_4.csv")
)
#with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
#    executor.map(createThreads, args)

for i in range(4):
    runNotebooks(base_url, 1762516, 1762516+100, base_output_file_path+"_1.csv")
    runNotebooks(base_url, 1762617, 1762617+100, base_output_file_path+"_2.csv")
    runNotebooks(base_url, 1762718, 1762718+100, base_output_file_path+"_3.csv")
    runNotebooks(base_url, 1762819, 1762819+100, base_output_file_path+"_4.csv")