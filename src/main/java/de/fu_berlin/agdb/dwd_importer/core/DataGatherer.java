package de.fu_berlin.agdb.dwd_importer.core;

import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import de.fu_berlin.agdb.importer.payload.LocationWeatherData;
import de.fu_berlin.agdb.importer.payload.StationMetaData;

public class DataGatherer implements IFTPFileWorkerProvider, IDWDDataHandler{
	
	public static final String server = "ftp-cdc.dwd.de";
	public static final String dataDirectory = "/pub/CDC/observations_germany/climate/daily/kl/recent/";
	
	private final int numberOfThreads;
	private List<FTPFile> ftpFiles;
	
	private ArrayList<LocationWeatherData> gatheredLocationWeatherData;
	private List<StationMetaData> locations;
	private HashMap<Thread, FTPClient> ftpClients;

	public DataGatherer(int numberOfThreads, List<StationMetaData> locations){
		this.numberOfThreads = numberOfThreads;
		this.locations = locations;
		gatheredLocationWeatherData = new ArrayList<LocationWeatherData>();
		ftpClients = new HashMap<Thread, FTPClient>();
	}
	
	public List<LocationWeatherData> gatherData() throws SocketException, IOException {
		FTPClient ftpClient = setupFTPClient(dataDirectory);
		
		ftpFiles = new ArrayList<FTPFile>(Arrays.asList(ftpClient.listFiles()));
		
		List<Thread> threads = new ArrayList<Thread>();
		for(int i = 0;  i < numberOfThreads; i++){
			FTPClient threadFtpClient = setupFTPClient(dataDirectory);
			Thread thread = new Thread(new FTPFileWorker(this, threadFtpClient, this));
			ftpClients.put(thread, threadFtpClient);
			threads.add(thread);
			thread.start();
		}
		
		waitForThreads(threads);
		
		shutDownFTPClient(ftpClient);
		return gatheredLocationWeatherData;
	}
	
	private void waitForThreads(List<Thread> threads) throws IOException{
		for (Thread thread : threads) {
			try {
				thread.join();
				shutDownFTPClient(ftpClients.get(thread));
			} catch (InterruptedException e) {
				waitForThreads(threads);
			}
		}
	}

	private FTPClient setupFTPClient(String directory) throws SocketException,
			IOException {
		FTPClient ftpClient = new FTPClient();
		ftpClient.connect(server);
		ftpClient.login("anonymous", "");
		
		ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
		
		ftpClient.changeWorkingDirectory(directory);
		return ftpClient;
	}
	
	public synchronized FTPFile getFTPFileToWorkWith(){
		if(ftpFiles.size() > 0){
			FTPFile ftpFile = ftpFiles.get(0);
			ftpFiles.remove(0);
			return ftpFile;
		}
		return null;
	}
	
	private void shutDownFTPClient(FTPClient ftpClient) throws IOException {
		ftpClient.logout();
		ftpClient.disconnect();
	}

	@Override
	public synchronized void addData(LocationWeatherData locationWeatherData) {
		gatheredLocationWeatherData.add(locationWeatherData);
	}

	@Override
	public StationMetaData getMetaDataForStation(long id){
		for (StationMetaData stationMetaData : locations) {
			if(stationMetaData.getStationId() == id){
				return stationMetaData;
			}
		}
		return null;
	}
}
