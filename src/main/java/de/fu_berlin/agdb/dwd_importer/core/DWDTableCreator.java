package de.fu_berlin.agdb.dwd_importer.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DWDTableCreator {
	private Connection connection;

	public DWDTableCreator(Connection connection){
		this.connection = connection;
	}

	public void createTables() throws SQLException{
		createMetaDataTable();
		createWeatherDataTable();
	}

	private void createMetaDataTable() throws SQLException {
		String statement = "CREATE TABLE public.dwd_station_meta_data " + 
				"( " +
					"station_id bigint NOT NULL, " +
					"station_position geometry NOT NULL, " +
					"from_date date, " +
					"until_date date, " +
					"station_height integer, " +
					"station_name character varying, " +
					"federal_state character varying, " +
					"CONSTRAINT dwd_station_meta_data_primary_key PRIMARY KEY (station_id) " +
				") " +
				"WITH ( " +
					"OIDS = FALSE " +
				") " +
				"; " ;
		PreparedStatement preparedStatemend = connection.prepareStatement(statement);
		preparedStatemend.execute();
		preparedStatemend.close();
	}

	private void createWeatherDataTable() throws SQLException {
		String statement = "CREATE TABLE public.dwd_station_weather_data " +
				"( " +
					"station_id bigint NOT NULL, " +
					"date date NOT NULL, " +
					"quality_level integer, " +
					"average_air_temperature double precision, " +
					"steampressure double precision, " +
					"cloudage double precision, " +
					"air_pressure double precision, " +
					"relative_humidity_of_the_air double precision, " +
					"wind_speed double precision, " +
					"maximum_air_temperature double precision, " +
					"minimum_air_temperature double precision, " +
					"minimum_air_temperature_ground double precision, " +
					"maximum_wind_speed double precision, " +
					"precipitationDepth double precision, " +
					"sunshine_duration double precision, " +
					"snow_height double precision, " +
					"CONSTRAINT dwd_station_weather_data_primary_key PRIMARY KEY (station_id, date), " +
					"CONSTRAINT dwd_station_weather_data_foreign_key FOREIGN KEY (station_id) REFERENCES dwd_station_meta_data (station_id) ON UPDATE CASCADE ON DELETE CASCADE " +
				") " + 
				"WITH ( " +
					"OIDS = FALSE " +
				") " +
				"; ";
		PreparedStatement preparedStatemend = connection.prepareStatement(statement);
		preparedStatemend.execute();
		preparedStatemend.close();

	}
}
