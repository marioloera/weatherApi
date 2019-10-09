import json
import avro
import datetime
from src import avgTempClass
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import pprint

pp = pprint.PrettyPrinter(indent=1, width=3)
LastRunFolder = ''
RunLog = ''
DateISO8601Format = ''
DaySuperSchemaJson = ''


def SimpleETL(config, rawJsonData):
    print("**********************Simple ET*************************")
    FillVariables(config)
    daysOfForecasts = len(rawJsonData["DailyForecasts"])
    days = []
    try:
        # ET
        for dayNumer in range(daysOfForecasts):
            dayDic = {}  # create an empty dictionary
            d = rawJsonData["DailyForecasts"][dayNumer]
            # print str(dayNumer)+'-----------'
            # read accu weather format
            date = d["Date"]
            minTemp = d["Temperature"]["Minimum"]["Value"]
            maxTemp = d["Temperature"]["Maximum"]["Value"]

            # load desire avro format
            dayDic["temperatureMin_C"] = minTemp
            dayDic["temperatureMax_C"] = maxTemp
            dayDic["date"] = date

            # print(date + " " + str(minTemp) + " " + str(maxTemp))
            days.append(dayDic)
        # L
        schemaFile = config["ETL"]["Load"]["Avro"]["SchemaFile"]
        schemaJson = json.load(open(schemaFile, "r"))
        # pp.pprint(schemaJson)
        dayAvroSchemaString = json.dumps(schemaJson)
        schema = avro.schema.Parse(dayAvroSchemaString)

        # create a writer
        dataAvro = LastRunFolder+"simpleETL.avro"
        writer = DataFileWriter(open(dataAvro, "wb"),
                                DatumWriter(), schema)

        # append each day
        for day in days:
            # pp.pprint(day)
            writer.append(day)
        # close writer
        writer.close()
        print("**********************Simple Check**********************")
        reader = DataFileReader(open(dataAvro, "rb"),
                                DatumReader())
        for day in reader:
            pp.pprint(day)

    except Exception as ex:
        print(ex)


def AdvanceETL(config, ApiDataJson):
    FillVariables(config)
    [extractSuccesful, DayKeyArray] = ExtractData(config, ApiDataJson)

    if(extractSuccesful):
        [tranformSuccesful, DayArray, DayAccuTemp] = Transform(DayKeyArray)

    if(tranformSuccesful):
        # Load AverageTemp Datafile
        LoadAveTemp(config, DayAccuTemp)

        # Load Forecast in Avro
        LoadAvro(config, DayArray)

        CheckAvroLoad(config)


def FillVariables(config):
    # extTan
    global LastRunFolder
    global RunLog
    global DateISO8601Format
    global DaySuperSchemaJson
    LastRunFolder = config["RunLog"]["Folder"]
    RunLog = config["RunLog"]["LogFile"]
    DateISO8601Format = config["ETL"]["Extract"]["DateISO8601Format"]
    daySuperSchemaFile = config["ETL"]["Extract"]["DaySuperSchemaFile"]
    DaySuperSchemaJson = json.load(open(daySuperSchemaFile, "r"))


def ExtractData(config, rawJsonData):
    print("**********************Extracting Data*************************")
    daysToExtract = config["ETL"]["Extract"]["Days"]
    daysOfForecasts = len(rawJsonData[DaySuperSchemaJson["name"]])
    dayKeyArray = []
    try:
        for dayNumer in range(0, min(daysToExtract, daysOfForecasts)):
            dicDay = {}  # create an empty dictionary
            Day = rawJsonData[DaySuperSchemaJson["name"]][dayNumer]
            # print str(dayNumer)+'-----------'
            for fiel in DaySuperSchemaJson["fields"]:
                # restar day object for the next fiel
                day = Day
                # go throug the labels in accWeather json
                for label in fiel["accWeatherLabels"]:
                    obj = day[label]
                    day = obj
                if (fiel["multiple"] != 0):
                    obj = obj*fiel["multiple"]
                # print  str(fiel["name"]) + '  '+str(obj)
                dicDay[fiel["fieldKey"]] = obj
                # dicDay[fiel["name"]]= obj
                # pp.pprint(dicDay)
            dayKeyArray.append(dicDay)
        statusMessage = "Extracting accuWeatherDataJson was successfull!"
        status = True
        # pp.pprint(dayKeyArray)
    except Exception as ex:
        statusMessage = "Reading accuWeatherDataJson Error!\n{}".format(ex)
        print(ex)
        status = False
        pass
        # Save RunStatusMessage to LastRun
    with open(LastRunFolder+RunLog, "a") as file:
        file.write("{}\n".format(statusMessage))
    return [status, dayKeyArray]


def Transform(dayKeyArray):
    print("**********************Transforming Data***********************")
    # Create dictinary for fielKey and "fiel"name maping
    status = False
    mapKeyNames = {}
    for fiel in DaySuperSchemaJson["fields"]:
        mapKeyNames[fiel["fieldKey"]] = fiel["name"]
    # print MapKeyNames[5] gets the name of key=5

    # object for average temperatures
    dayAvgTemp = avgTempClass.AvgTempClass(str(datetime.datetime.now()))

    # object for other fiels
    ofields = DaySuperSchemaJson["otherFields"]

    # Creates empty array for Days
    dayArray = []
    try:
        for dayKey in dayKeyArray:
            dicDay = {}  # create an empty dictionary
            for key in dayKey:
                value = dayKey[key]
                name = mapKeyNames[key]
                dicDay[name] = value
                # fieldKey:1 date
                if (key == 1):
                    dt = datetime.datetime.strptime(value, DateISO8601Format)
                    dicDay[ofields["date.year"]["name"]] = dt.year
                    dicDay[ofields["date.month"]["name"]] = dt.month
                    dicDay[ofields["date.day"]["name"]] = dt.day
                # fieldKey:2 temperatureMin_C
                if (key == 2):
                    minTem = value
                # fieldKey:3 temperatureMax_C
                if (key == 3):
                    maxTemp = value
            # add the min and max temps to comput the average
            dayAvgTemp.AddTemValues(minTem, maxTemp)
            # append the day to the array
            dayArray.append(dicDay)
            # pp.pprint(dicDay)
        status = True
    except Exception as ex:
        print(ex)
    return [status, dayArray, dayAvgTemp]


def AutogenerateSchema(baseShcema):
    print("**********************Autogenerate Schema*********************")
    # target = json.load(open(DayAvroSchemaFile, "r"))
    # pp.pprint(target)
    autGenSchema = {}
    autGenSchema["name"] = baseShcema["name"]
    autGenSchema["namespace"] = baseShcema["namespace"]
    autGenSchema["type"] = baseShcema["type"]
    autGenSchema["fields"] = []
    # **********************fields
    for fullField in baseShcema["fields"]:
        field = {}
        field["name"] = fullField["name"]
        field["type"] = fullField["type"]
        autGenSchema["fields"].append(field)
    # **********************other fields
    for oField in baseShcema["otherFields"]:
        field = {}
        fullField = baseShcema["otherFields"][oField]
        field["name"] = fullField["name"]
        field["type"] = fullField["type"]
        autGenSchema["fields"].append(field)
        # pp.pprint(synteticSchemaJson)
    return autGenSchema


def LoadAvro(config, daysArray):
    print("**********************Loading ForecastDataAvro****************")
    autGenSchemaFile = config["ETL"]["Extract"]["AutGenSchemaFile"]
    forecastAvroFile = config["ETL"]["Load"]["Avro"]["File"]
    dWHForecastPath = config["ETL"]["Load"]["AvgData"]["DWHForecastPath"]
    dayAvroSchema = AutogenerateSchema(DaySuperSchemaJson)

    with open(LastRunFolder+autGenSchemaFile, "w") as file:
        file.write(json.dumps(dayAvroSchema, indent=4))
    # create avro.schema from json schema
    dayAvroSchemaString = json.dumps(dayAvroSchema)
    schema = avro.schema.Parse(dayAvroSchemaString)

    # create a writer for DWH
    writer = DataFileWriter(open(dWHForecastPath+forecastAvroFile, "wb"),
                            DatumWriter(), schema)

    # create a writer for LasRun
    writerLR = DataFileWriter(open(LastRunFolder+forecastAvroFile, "wb"),
                              DatumWriter(), schema)

    # append each day
    for day in daysArray:
        # pp.pprint(day)
        writer.append(day)
        writerLR.append(day)

    # close writer
    writer.close()
    writerLR.close()
    # pp.pprint(writer)


def CheckAvroLoad(config):
    print("***********This information was store in avro format *********")
    forecastAvroFile = config["ETL"]["Load"]["Avro"]["File"]
    reader = DataFileReader(open(LastRunFolder+forecastAvroFile, "rb"),
                            DatumReader())
    for day in reader:
        pp.pprint(day)


def LoadAveTemp(config, dayAccuTemp):
    print("**********************Loading Average Temp Data***************")
    # load AverageForecastData
    avgDataFolder = config["ETL"]["Load"]["AvgData"]["Folder"]
    strSep = config["ETL"]["Load"]["AvgData"]["StringSeparator"]
    # Get average temperatures in one row
    averageForecastData = dayAccuTemp.GetOneRowInfo(strSep)
    # Save AccuWeatherData to LastRun
    with open(LastRunFolder+'avgData.txt', "w") as file:
        file.write(averageForecastData)
    # Save AccuWeatherData to AverageForecastDataFolder
    with open(avgDataFolder+'avgData.txt', "a") as file:
        file.write("{}\n".format(averageForecastData))
