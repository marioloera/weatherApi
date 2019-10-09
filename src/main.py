import os
import json
import datetime
from src import weatherApi 
from src import etl


def run():
    clear = lambda: os.system('cls')
    clear()
    startTime = str(datetime.datetime.now())
    config = json.load(open("./config/config.json", "r"))
    logFolder = config["Log"]["Folder"]
    logFile = logFolder + config["Log"]["LogFile"]
    etlAdvanceMode = config["ETL"]["AdvanceMode"]
    saveRawData = config["Read"]["SaveRawData"]
    accuWeatherData = config["Read"]["AccuWeatherData"]
    # Initialize last run file
    logHeader = "\n\n{0}\nETL AdvanceMode:{1}\n".format(startTime,
                                                   str(etlAdvanceMode))
    with open(logFile, "a") as file:
        file.write(logHeader)

    # Fetch Api data
    [dataFetched, ApiDataJson] = weatherApi.fetchData(config, startTime)

    if(dataFetched):
        # Save AccuWeatherData to StoreAccuWeatherData Folder
        if (saveRawData):
            st = startTime.replace(":", "_")
            rawDataFile = accuWeatherData+st+".json"
            with open(rawDataFile, 'w') as outfile:
                json.dump(ApiDataJson, outfile, sort_keys=True,
                          indent=4, ensure_ascii=False)        
        if(etlAdvanceMode):
            etl.advanceETL(config, ApiDataJson)
        else:
            etl.simpleETL(config, ApiDataJson)
