package de.fu_berlin.agdb.dwd_importer.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

public interface IFTPFileWorkerProvider {
	public FTPFile getFTPFileToWorkWith();

	public boolean retriveFTPFile(FTPClient ftpClient, FTPFile ftpFile, File file)
			throws FileNotFoundException, IOException;
}
