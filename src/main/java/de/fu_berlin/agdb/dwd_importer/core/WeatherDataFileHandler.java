package de.fu_berlin.agdb.dwd_importer.core;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Date;
import java.util.StringTokenizer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import de.fu_berlin.agdb.importer.payload.DataType;
import de.fu_berlin.agdb.importer.payload.LocationWeatherData;
import de.fu_berlin.agdb.importer.payload.StationMetaData;

public class WeatherDataFileHandler extends DataFileHandler{

	private static final Logger logger = LogManager.getLogger(WeatherDataFileHandler.class);
	private IDWDDataHandler dwDataHandler;
	
	public WeatherDataFileHandler(IWeatherDataFileProvider weatherDataProvider, IDWDDataHandler dwDataHandler) {
		super(weatherDataProvider.getWeatherDataFile());
		this.dwDataHandler = dwDataHandler;
	}

	@Override
	public void handleDataFile() throws IOException {
		logger.debug("Analyzing weather data File " + getFile().getName());
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(getFile()), "ISO-8859-1"));
		//ignore first line because it has no relevant information
		//maybe only the last line should be used due to the fact that all other lines
		//have duplicated information
		String line = reader.readLine();
		
		while((line = reader.readLine()) != null && line.length() > 1){
			injectDataLine(line);
		}
		reader.close();
		logger.debug("Analyzed weather data File " + getFile().getName());
		getFile().delete();
		logger.debug("Deleted weather data File " + getFile().getName());
	}
	
	@SuppressWarnings("unused")
	private void injectDataLine(String line) {
		line = line.replace(" ", "");
		
		StringTokenizer tokenizer = new StringTokenizer(line, ";");
		
		//Stations_ID
		Long stationId = Long.valueOf(tokenizer.nextToken());
		
		//Mess_Datum
		String stringDate = tokenizer.nextToken();
		stringDate = stringDate.substring(0, 4) + "-" + stringDate.substring(4, 6)+ "-" + stringDate.substring(6);
		Date date = Date.valueOf(stringDate);
		
		//Qualitaets_Niveau (Values form 1 to 10)
		int qualityLevel = Integer.parseInt(tokenizer.nextToken());
		
		//LUFTTEMPERATUR (°C)
		double averageAirTemperature = Double.valueOf(tokenizer.nextToken());
		
		//DAMPFDRUCK (hpa)
		double steamPressure = Double.valueOf(tokenizer.nextToken());

		//BEDECKUNGSGRAD (eight) (1/8, 2/8, ..., 8/8)
		double cloudage = Double.valueOf(tokenizer.nextToken());
		
		//LUFTDRUCK_STATIONSHOEHE (hpa)
		double airPressure = Double.valueOf(tokenizer.nextToken());
		
		//REL_FEUCHTE (%)
		double relativeHumidityOfTheAir = Double.valueOf(tokenizer.nextToken());
		
		//WINDGESCHWINDIGKEIT (m/sec)
		double windSpeed = Double.valueOf(tokenizer.nextToken());

		//LUFTTEMPERATUR_MAXIMUM (°C)
		double maximumAirTemperature = Double.valueOf(tokenizer.nextToken());
		
		//LUFTTEMPERATUR_MINIMUM (°C)
		double minimumAirTemperature = Double.valueOf(tokenizer.nextToken());
		
		//LUFTTEMP_AM_ERDB_MINIMUM (°C)
		double minimumAirTemperatureGround = Double.valueOf(tokenizer.nextToken());
		
		//WINDSPITZE_MAXIMUM (m/sec)
		double maximumWindSpeed = Double.valueOf(tokenizer.nextToken());
		
		//NIEDERSCHLAGSHOEHE (mm)
		double precipitationDepth = Double.valueOf(tokenizer.nextToken());
		
		//SONNENSCHEINDAUER
		double sunshineDuration = Double.valueOf(tokenizer.nextToken());
		
		//SCHNEEHOEHE
		double snowHeight = Double.valueOf(tokenizer.nextToken());
		
		StationMetaData metaDataForStation = dwDataHandler.getMetaDataForStation(stationId);
		if(dwDataHandler.getMetaDataForStation(stationId) != null){
			LocationWeatherData locationWeatherData = new LocationWeatherData(dwDataHandler.getMetaDataForStation(stationId), System.currentTimeMillis(), DataType.REPORT);
			
			locationWeatherData.setQualityLevel(qualityLevel);
			locationWeatherData.setTemperature(averageAirTemperature);
			locationWeatherData.setSteamPressure(steamPressure);
			locationWeatherData.setCloudage(cloudage);
			locationWeatherData.setAtmospherePressure(airPressure);
			locationWeatherData.setAtmosphereHumidity(relativeHumidityOfTheAir);
			locationWeatherData.setWindSpeed(windSpeed);
			locationWeatherData.setTemperatureHigh(maximumAirTemperature);
			locationWeatherData.setTemperatureLow(minimumAirTemperature);
			locationWeatherData.setMinimumAirGroundTemperature(minimumAirTemperatureGround);
			locationWeatherData.setMaximumWindSpeed(maximumWindSpeed);
			locationWeatherData.setPrecipitationDepth(precipitationDepth);
			locationWeatherData.setSunshineDuration(sunshineDuration);
			locationWeatherData.setSnowHeight(snowHeight);
			
			dwDataHandler.addData(locationWeatherData);
		}
	}
}
