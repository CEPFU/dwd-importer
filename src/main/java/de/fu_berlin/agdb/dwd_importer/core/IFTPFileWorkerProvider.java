package de.fu_berlin.agdb.dwd_importer.core;

import org.apache.commons.net.ftp.FTPFile;

public interface IFTPFileWorkerProvider {
	public FTPFile getFTPFileToWorkWith();
}
