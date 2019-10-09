import requests
import json


def ReadData(config, startTime):
    print("**********************Reading Data****************************")
    runLogFolder = config["RunLog"]["Folder"]
    runLog = config["RunLog"]["LogFile"]
    mockinghMode = config["Read"]["MockinghMode"]
    mockingDataFile = config["Read"]["MockingDataFile"]
    saveRawData = config["Read"]["SaveRawData"]
    accuWeatherData = config["Read"]["AccuWeatherData"]
    dataFetched = False
    log = runLogFolder + runLog

    if (mockinghMode):
        dataFetched = True
        fetchDataCmd = "MockinghMode"
        apiData = json.load(open(mockingDataFile, "r"))

    else:
        numDays = config["Read"]["AccuWeatherApi"]["DaysOfForecasts"]
        locKey = config["Read"]["AccuWeatherApi"]["LocationKey"]
        apiKey = config["Read"]["AccuWeatherApi"]["ApiKey"]
        [dataFetched, fetchDataCmd, apiData] = FetchApiData(numDays,
                                                            locKey,
                                                            apiKey,
                                                            log)

    # Save CommandGetData to LastRun
    with open(runLogFolder+'commandGetData.txt', "w") as file:
        file.write(str(startTime) + '\n'+fetchDataCmd)

    if (dataFetched):
        # Save AccuWeatherData to LastRun
        with open(runLogFolder+"accuWeatherData.json", 'w') as outfile:
            json.dump(apiData, outfile, sort_keys=True,
                      indent=4, ensure_ascii=False)

        # Save AccuWeatherData to StoreAccuWeatherData Folder
        if (saveRawData):
            with open(accuWeatherData+startTime+".json", 'w') as outfile:
                json.dump(apiData, outfile, sort_keys=True,
                          indent=4, ensure_ascii=False)
    return [dataFetched, apiData]


def FetchApiData(numDays, locationKey, apiKey, log):
    wasReaded = False
    cmd = "http://dataservice.accuweather.com/forecasts/v1/daily/{num}day/{loc}?apikey={key}&details=true&metric=true".format(
                                num=str(numDays),
                                loc=str(locationKey),
                                key=apiKey)
    try:
        r = requests.get(cmd)
        apiData = json.loads(r.text)
        wasReaded = True
    except Exception as ex:
        statusMessage = "Reading Api Data Error!\n{}".format(repr(ex))
        apiData = {}
        print(statusMessage)
        with open(log, "a") as file:
            file.write("{}\n".format(statusMessage))
        pass
    return[wasReaded, cmd, apiData]
