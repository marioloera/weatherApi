import os
import json
import datetime
from Modules import weatherApi
from Modules import etl


def run():
    clear = lambda: os.system('cls')
    clear()
    startTime = str(datetime.datetime.now()).replace(":", "_")
    config = json.load(open("./Config/config.json", "r"))
    runLogFolder = config["RunLog"]["Folder"]
    runLog = config["RunLog"]["LogFile"]
    etlAdvanceMode = config["ETL"]["AdvanceMode"]
    # Initialize last run file
    logHeader = "{0}\nETL SimpleMode:{1}\n".format(startTime,
                                                   str(etlAdvanceMode))
    with open(runLogFolder+runLog, "a") as file:
        file.write(logHeader)

    # Fetch Api data
    [dataFetched, ApiDataJson] = weatherApi.ReadData(config, startTime)

    if(dataFetched):
        if(etlAdvanceMode):
            etl.AdvanceETL(config, ApiDataJson)
        else:
            etl.SimpleETL(config, ApiDataJson)
