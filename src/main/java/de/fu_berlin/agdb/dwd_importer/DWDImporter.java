package de.fu_berlin.agdb.dwd_importer;

import java.io.IOException;
import java.net.SocketException;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import de.fu_berlin.agdb.dwd_importer.core.DataGatherer;
import de.fu_berlin.agdb.dwd_importer.core.FTPFileWorker;
import de.fu_berlin.agdb.importer.AWeatherImporter;
import de.fu_berlin.agdb.importer.payload.LocationWeatherData;
import de.fu_berlin.agdb.importer.payload.StationMetaData;

public class DWDImporter extends AWeatherImporter {

	private static final Logger logger = LogManager.getLogger(FTPFileWorker.class);
	
	private static final int NUMBER_OF_THREADS = 10;
	
	@Override
	protected List<LocationWeatherData> getWeatherDataForLocations(List<StationMetaData> locations) {
		DataGatherer dataGatherer = new DataGatherer(NUMBER_OF_THREADS);
		try {
			return dataGatherer.gatherData();
		} catch (SocketException e) {
			logger.error("Error while communicating with the DWD service: ", e);
		} catch (IOException e) {
			logger.error("IO-Error: ", e);
		}
		return null;
	}
	
	@Override
	protected long getServiceTimeout() {
		//12 hours
		return 12*60*60*1000;
	}
}
