2019-10-09 by 
Mario Loera 
marioll@kth.se

This program fetch  forecast data from accuweather.com
it process the data and sotere in avro format and 
csv file

To excecute the program
	python run.py

You may need to install the packeges from requirements.txt
	python3 -m venv /path/to/new/virtual/env
	source ./env/bin/activate
	pip install -r requirements.txt


config.json file has attributes to modify the execution of the program

	READ
		MockinghMode : determines wether or not tu use sample file or fetch data form api
		AccuWeatherApi
			LocationKey : determies where the location for forecast to be perform
			ApiKey	    : AccuWeather key to acces data
			DaysOfForecasts: day to fetch forecast data  [1, 5]
	ETL
		AdvanceMode: to run advance ETL mode


data folder contains:
	accWWeatherData
			*AccuWeatherData.json [raw api data] 
	averageForecastData
			*AvgData.txt [one line]
	warehouseForecastData
			*forecastAvroData.avro [data extracted from api data]
			*autGenAvroSchema.json [schema for avro data in json format]
			*simpleETL.avro [data extracted from api data]
			*DayForecastData []
	logs
			*run.log [ ] log information
	