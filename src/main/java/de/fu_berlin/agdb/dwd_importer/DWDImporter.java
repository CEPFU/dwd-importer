package de.fu_berlin.agdb.dwd_importer;

import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import de.fu_berlin.agdb.importer.AWeatherImporter;
import de.fu_berlin.agdb.importer.payload.LocationWeatherData;
import de.fu_berlin.agdb.importer.payload.StationMetaData;

public class DWDImporter extends AWeatherImporter{
	private static final Logger logger = LogManager.getLogger(DWDImporter.class);
	
	public DWDImporter() {
	}

	@Override
	protected List<LocationWeatherData> getWeatherDataForLocations(
			List<StationMetaData> locations) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	protected long getServiceTimeout() {
		//12 hours
		return 12*60*60*1000;
	}
}
