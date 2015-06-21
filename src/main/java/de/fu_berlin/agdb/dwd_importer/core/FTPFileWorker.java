package de.fu_berlin.agdb.dwd_importer.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class FTPFileWorker implements Runnable {

	private static final Logger logger = LogManager.getLogger(FTPFileWorker.class);
	
	private IFTPFileWorkerProvider workerProvider;
	private FTPClient ftpClient;

	private IDWDDataHandler dwDataHandler;

	public FTPFileWorker(IFTPFileWorkerProvider workerProvider, FTPClient ftpClient, IDWDDataHandler dwDataHandler) {
		this.workerProvider = workerProvider;
		this.ftpClient = ftpClient;
		this.dwDataHandler = dwDataHandler;
	}

	public void run() {
			FTPFile ftpFileToWorkWith;
			while((ftpFileToWorkWith = workerProvider.getFTPFileToWorkWith()) != null) {
				if(ftpFileToWorkWith.isFile() && ftpFileToWorkWith.getName().contains(".zip")){
					File loadedFile = new File(ftpFileToWorkWith.getName());
					try {
						if(workerProvider.retriveFTPFile(ftpClient, ftpFileToWorkWith, loadedFile)){
							logger.debug("Downloaded " + ftpFileToWorkWith.getName());
							ZipDataHandler zipDataHandler = new ZipDataHandler(loadedFile);
							WeatherDataFileHandler weatherDataFileHandler = new WeatherDataFileHandler(zipDataHandler, dwDataHandler);
							weatherDataFileHandler.handleDataFile();
							logger.debug("Handeled " + ftpFileToWorkWith.getName());
							loadedFile.delete();
							logger.debug("Deleted " + ftpFileToWorkWith.getName());
						} else {
							logger.debug("Failed loading + " + ftpFileToWorkWith.getName());
						}
					} catch (FileNotFoundException e) {
						logger.error(e);
					} catch (IOException e) {
						logger.error(e);
					}
				}
			}
	}
}
