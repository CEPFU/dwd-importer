package de.fu_berlin.agdb.dwd_importer.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class ZipDataHandler implements IWeatherDataFileProvider{
	
	private static final Logger logger = LogManager.getLogger(ZipDataHandler.class);
	
	private File weatherDataFile;
	
	public ZipDataHandler(File file) throws IOException {
		handle(file);
	}
	
	public File getWeatherDataFile() {
		return weatherDataFile;
	}
	
	private void handle(File file) throws IOException {
		ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(file));
		while(handleZipEntry(zipInputStream)){
			logger.debug("Zip entry analyzed.");
		}
		zipInputStream.close();
	}
	
	private boolean handleZipEntry(ZipInputStream zipInputStream) throws FileNotFoundException, IOException{
		ZipEntry zipEntry = zipInputStream.getNextEntry();
		if(zipEntry == null){
			return false;
		} else if(zipEntry.getName().contains("produkt_klima_Tageswerte")){
			weatherDataFile = new File(zipEntry.getName() + ".tmp");
			unzipFile(zipInputStream, weatherDataFile);
			logger.debug("Unziped weather data file to " + weatherDataFile.getName());
		}
		return true;
	}

	private void unzipFile(ZipInputStream zipInputStream, File dataFile)
			throws FileNotFoundException, IOException {
		FileOutputStream fileOutputStream = new FileOutputStream(dataFile);
		byte[] buffer = new byte[1024];
		int len;
		while((len = zipInputStream.read(buffer)) > 0){
			fileOutputStream.write(buffer, 0, len);
		}
		fileOutputStream.close();
	}
}
