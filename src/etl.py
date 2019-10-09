import json
import avro
import datetime
from src import avgTempClass
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import pprint

pp = pprint.PrettyPrinter(indent=1, width=3)


def simpleETL(config, rawJsonData):
    print("**********************Simple ET*************************")
    daysOfForecasts = len(rawJsonData["DailyForecasts"])
    logFolder = config["Log"]["Folder"]
    logFile = logFolder + config["Log"]["LogFile"]
    dWHForecastPath = config["ETL"]["Load"]["AvgData"]["DWHForecastPath"]
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
        dataAvro = dWHForecastPath+"simpleETL.avro"
        writer = DataFileWriter(open(dataAvro, "wb"),
                                DatumWriter(), schema)

        # append each day
        for day in days:
            # pp.pprint(day)
            writer.append(day)
        # close writer
        writer.close()
        print("**********************Simple Check**********************")
        _readAvro(dataAvro)

    except Exception as ex:
        print(ex)
        with open(logFile, "a") as file:
            file.write("{}\n".format(ex))


def advanceETL(config, apiDataJson):
    superSchemaFile = config["ETL"]["Extract"]["DaySuperSchemaFile"]
    superSchema = json.load(open(superSchemaFile, "r"))
    [extractSuccesful, dayKeyArray] = _extractData(config, superSchema, apiDataJson)

    if(extractSuccesful):
        [tranformSuccesful, dayArray, avgTemp] = _transform(config, superSchema, dayKeyArray)

    if(tranformSuccesful):
        # Load AverageTemp Datafile
        _loadAveTemp(config, avgTemp)

        # Load Forecast in Avro
        _loadAvro(config, superSchema, dayArray)


def _extractData(config, superSchema, rawDataJson):
    print("**********************Extracting Data*************************")
    logFolder = config["Log"]["Folder"]
    logFile = logFolder + config["Log"]["LogFile"]
    daysToExtract = config["ETL"]["Extract"]["Days"]
    daysOfForecasts = len(rawDataJson[superSchema["name"]])
    dayKeyArray = []
    try:
        for dayNumer in range(0, min(daysToExtract, daysOfForecasts)):
            dicDay = {}  # create an empty dictionary
            dayJson = rawDataJson[superSchema["name"]][dayNumer]
            # print str(dayNumer)+'-----------'
            for field in superSchema["fields"]:
                # restar day object for the next field
                day = dayJson
                # go throug the labels in accWeather json
                for label in field["accWeatherLabels"]:
                    obj = day[label]
                    day = obj
                if (field["multiple"] != 0):
                    obj = obj * field["multiple"]
                # print  str(field["name"]) + '  '+str(obj)
                dicDay[field["fieldKey"]] = obj
                # dicDay[field["name"]]= obj
                # pp.pprint(dicDay)
            dayKeyArray.append(dicDay)
        statusMessage = "\nExtracting accuWeatherDataJson was successfull!"
        status = True
        # pp.pprint(dayKeyArray)
    except Exception as ex:
        statusMessage = "R\neading accuWeatherDataJson Error!\n{}".format(ex)
        print(ex)
        status = False
        pass
        # log message
    with open(logFile, "a") as file:
        file.write("{}\n".format(statusMessage))
    return [status, dayKeyArray]


def _transform(config, superSchema, dayKeyArray):
    print("**********************Transforming Data***********************")
    # Create dictinary for fielKey and "fiel"name maping
    status = False
    mapKeyNames = {}
    for fiel in superSchema["fields"]:
        mapKeyNames[fiel["fieldKey"]] = fiel["name"]
    # print MapKeyNames[5] gets the name of key=5

    # object for average temperatures
    avgTemp = avgTempClass.AvgTempClass(str(datetime.datetime.now()))

    # object for other fiels
    ofields = superSchema["otherFields"]
    dateFormatISO8601 = config["ETL"]["Extract"]["DateISO8601Format"]

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
                    dt = datetime.datetime.strptime(value, dateFormatISO8601)
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
            avgTemp.addTemValues(minTem, maxTemp)
            # append the day to the array
            dayArray.append(dicDay)
            # pp.pprint(dicDay)
        status = True
    except Exception as ex:
        print(ex)
    return [status, dayArray, avgTemp]


def _loadAvro(config, superSchema, daysArray):
    print("**********************Loading ForecastDataAvro****************")
    autGenSchemaFile = config["ETL"]["Extract"]["AutGenSchemaFile"]
    forecastAvroFile = config["ETL"]["Load"]["Avro"]["File"]
    dWHForecastPath = config["ETL"]["Load"]["AvgData"]["DWHForecastPath"]
    
    dayAvroSchema = _autogenerateSchema(superSchema)

    with open(dWHForecastPath+autGenSchemaFile, "w") as file:
        file.write(json.dumps(dayAvroSchema, indent=4))
    # create avro.schema from json schema
    dayAvroSchemaString = json.dumps(dayAvroSchema)
    schema = avro.schema.Parse(dayAvroSchemaString)

    avroFile = dWHForecastPath + forecastAvroFile
    # create a writer for DWH
    writer = DataFileWriter(open(avroFile, "wb"),
                            DatumWriter(), schema)

    # append each day
    for day in daysArray:
        # pp.pprint(day)
        writer.append(day)

    # close writer
    writer.close()
    # pp.pprint(writer)
    _readAvro(avroFile)


def _readAvro(file):
    print("***********This information was store in avro format *********")
    reader = DataFileReader(open(file,"rb"), DatumReader())
    for r in reader:
        pp.pprint(r)


def _autogenerateSchema(baseShcema):
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


def _loadAveTemp(config, avgTemp):
    print("**********************Loading Average Temp Data***************")
    # load AverageForecastData
    avgDataFolder = config["ETL"]["Load"]["AvgData"]["Folder"]
    strSep = config["ETL"]["Load"]["AvgData"]["StringSeparator"]
    # Get average temperatures in one row
    averageForecastData = avgTemp.getOneRowInfo(strSep)
    # Save AccuWeatherData to AverageForecastDataFolder
    with open(avgDataFolder+'avgData.txt', "a") as file:
        file.write("{}\n".format(averageForecastData))
