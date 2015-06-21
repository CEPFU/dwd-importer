package de.fu_berlin.agdb.dwd_importer.core;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
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

	public DataGatherer(int numberOfThreads){
		this.numberOfThreads = numberOfThreads;
		gatheredLocationWeatherData = new ArrayList<LocationWeatherData>();
	}
	
	public List<LocationWeatherData> gatherData() throws SocketException, IOException {
		FTPClient ftpClient = setupFTPClient(dataDirectory);
		ftpFiles = new ArrayList<FTPFile>(Arrays.asList(ftpClient.listFiles()));
		
		List<Thread> threads = new ArrayList<Thread>();
		for(int i = 0;  i < numberOfThreads; i++){
			Thread thread = new Thread(new FTPFileWorker(this, ftpClient, this));
			threads.add(thread);
			thread.start();
		}
		
		waitForThreads(threads);
		shutDownFTPClient(ftpClient);
		
		return gatheredLocationWeatherData;
	}
	
	private void waitForThreads(List<Thread> threads){
		for (Thread thread : threads) {
			try {
				thread.join();
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
	
	public synchronized boolean retriveFTPFile(FTPClient ftpClient, FTPFile ftpFile,
			File file) throws FileNotFoundException, IOException {
		OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(file));
		boolean success = ftpClient.retrieveFile(ftpFile.getName(), outputStream);
		outputStream.close();
		return success;
	}

	private void shutDownFTPClient(FTPClient ftpClient)
			throws IOException {
		ftpClient.logout();
		ftpClient.disconnect();
	}

	@Override
	public synchronized void addData(LocationWeatherData locationWeatherData) {
		gatheredLocationWeatherData.add(locationWeatherData);
	}

	@Override
	public StationMetaData getMetaDataForStation(long id) {
		// TODO Auto-generated method stub
		return null;
	}
}
