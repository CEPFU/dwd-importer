package de.fu_berlin.agdb.dwd_importer.core;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Date;
import java.util.StringTokenizer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import de.fu_berlin.agdb.importer.payload.LocationWeatherData;

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
		BufferedReader reader = new BufferedReader(new FileReader(getFile()));
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
		double steampressure = Double.valueOf(tokenizer.nextToken());

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
		
		LocationWeatherData locationWeatherData = new LocationWeatherData(dwDataHandler.getMetaDataForStation(stationId), System.currentTimeMillis());
		//TODO think about how to add the data below
		dwDataHandler.addData(locationWeatherData);
		
//		String statement = "INSERT INTO dwd_station_weather_data "
//				+ "(station_id, date, quality_level, average_air_temperature, steampressure, "
//				+ "    cloudage, air_pressure, relative_humidity_of_the_air, wind_speed, maximum_air_temperature,"
//				+ "    minimum_air_temperature, minimum_air_temperature_ground, maximum_wind_speed,"
//				+ "    precipitationDepth, sunshine_duration, snow_height) "
//				+ "  SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? "
//				+ "  WHERE NOT EXISTS( "
//				+ "    SELECT station_id, date "
//				+ "    FROM dwd_station_weather_data "
//				+ "    WHERE station_id = ?"
//				+ "    AND date = ?); ";
//		
//		PreparedStatement preparedStatemend = getConnection().prepareStatement(statement);
//		preparedStatemend.setLong(1, stationId);
//		preparedStatemend.setDate(2, date);
//		preparedStatemend.setInt(3, qualityLevel);
//		preparedStatemend.setDouble(4, averageAirTemperature);
//		preparedStatemend.setDouble(5, steampressure);
//		preparedStatemend.setDouble(6, cloudage);
//		preparedStatemend.setDouble(7, airPressure);
//		preparedStatemend.setDouble(8, relativeHumidityOfTheAir);
//		preparedStatemend.setDouble(9, windSpeed);
//		preparedStatemend.setDouble(10, maximumAirTemperature);
//		preparedStatemend.setDouble(11, minimumAirTemperature);
//		preparedStatemend.setDouble(12, minimumAirTemperatureGround);
//		preparedStatemend.setDouble(13, maximumWindSpeed);
//		preparedStatemend.setDouble(14, precipitationDepth);
//		preparedStatemend.setDouble(15, sunshineDuration);
//		preparedStatemend.setDouble(16, snowHeight);
//		
//		preparedStatemend.setLong(17, stationId);
//		preparedStatemend.setDate(18, date);
//		preparedStatemend.execute();
//		preparedStatemend.close();
	}
}
