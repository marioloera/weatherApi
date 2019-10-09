import requests
import json


def fetchData(config, startTime):
    print("**********************Reading Data****************************")
    runLogFolder = config["Log"]["Folder"]
    runLog = config["Log"]["LogFile"]
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
        [dataFetched, fetchDataCmd, apiData] = _fetchApiData(numDays,
                                                            locKey,
                                                            apiKey,
                                                            log)

    # append CommandGetData to log
    with open(log, "a") as file:
        file.write("fetch Data Cmd: " + fetchDataCmd)

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


def _fetchApiData(numDays, locationKey, apiKey, log):
    dataFetched = False
    cmd = "http://dataservice.accuweather.com/forecasts/v1/daily/{num}day/{loc}?apikey={key}&details=true&metric=true".format(
                                num=str(numDays),
                                loc=str(locationKey),
                                key=apiKey)
    try:
        r = requests.get(cmd)
        apiData = json.loads(r.text)
        dataFetched = True
    except Exception as ex:
        statusMessage = "Reading Api Data Error!\n{}".format(repr(ex))
        apiData = {}
        print(statusMessage)
        with open(log, "a") as file:
            file.write("{}\n".format(statusMessage))
        pass
    return[dataFetched,cmd, apiData]
