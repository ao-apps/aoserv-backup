/*
 * Copyright 2003-2013, 2017, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.backup;

import com.aoindustries.aoserv.client.AOServConnector;
import com.aoindustries.aoserv.client.backup.FailoverFileReplication;
import com.aoindustries.aoserv.client.net.Server;
import com.aoindustries.aoserv.client.validator.MySQLServerName;
import com.aoindustries.net.InetAddress;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.logging.Logger;

/**
 * A <code>BackupEnvironment</code> tells the <code>BackupDaemon</code> how to run.
 *
 * All paths are represented as a <code>String</code> to allow anything to be backed-up, not just local files.
 *
 * @author  AO Industries, Inc.
 */
public interface BackupEnvironment {

	/**
	 * Gets the stat mode (or a generated and equivilent one) for a file.
	 */
	long getStatMode(FailoverFileReplication ffr, String filename) throws IOException;

	/**
	 * Gets the listing for a directory.
	 */
	String[] getDirectoryList(FailoverFileReplication ffr, String filename) throws IOException;

	/**
	 * Gets the user ID
	 */
	int getUid(FailoverFileReplication ffr, String filename) throws IOException;

	/**
	 * Gets the group ID
	 */
	int getGid(FailoverFileReplication ffr, String filename) throws IOException;

	/**
	 * Gets the modified time
	 */
	long getModifyTime(FailoverFileReplication ffr, String filename) throws IOException;

	/**
	 * Gets the length of the file
	 */
	long getLength(FailoverFileReplication ffr, String filename) throws IOException;

	/**
	 * Reads a symbolic link
	 */
	String readLink(FailoverFileReplication ffr, String filename) throws IOException;

	/**
	 * Gets the device file major and minor
	 */
	long getDeviceIdentifier(FailoverFileReplication ffr, String filename) throws IOException;

	/**
	 * Gets a stream reading the file
	 */
	InputStream getInputStream(FailoverFileReplication ffr, String filename) throws IOException;

	/**
	 * Gets the name of a file (the part after the last slash)
	 */
	String getNameOfFile(FailoverFileReplication ffr, String filename);

	/**
	 * Gets the connection to the master server.
	 */
	AOServConnector getConnector() throws IOException, SQLException;

	/**
	 * Gets the server this process is running on.
	 */
	Server getThisServer() throws IOException, SQLException;

	/**
	 * Gets the number of items per batch.  A higher value will consume
	 * more RAM but better hide latency.
	 */
	int getFailoverBatchSize(FailoverFileReplication ffr) throws IOException, SQLException;

	/**
	 * Called right before a backup pass begins.
	 * Implementations should call super.preBackup first.
	 * 
	 * The process is:
	 * <ol>
	 *   <li>preBackup</li>
	 *   <li>init</li>
	 *   <li>cleanup (always)</li>
	 *   <li>postBackup (only when backup pass successful)</li>
	 * </ol>
	 */
	void preBackup(FailoverFileReplication ffr) throws IOException, SQLException;

	/**
	 * Called before any backup-pass data is retrieved from the environment.
	 * <code>cleanup()</code> will also be called in a finally block.
	 * Implementations should call super.init first.
	 *
	 * @see  #cleanup(FailoverFileReplication)
	 * @see  #preBackup(FailoverFileReplication)
	 */
	void init(FailoverFileReplication ffr) throws IOException, SQLException;

	/**
	 * Called in a finally block after any backup-pass completes, no data
	 * will be obtained from the environment after this is called.
	 * Implementations should call super.cleanup in a finally block.
	 * 
	 * @see  #init(FailoverFileReplication)
	 * @see  #preBackup(FailoverFileReplication)
	 */
	void cleanup(FailoverFileReplication ffr) throws IOException, SQLException;

	/**
	 * Called right after a backup pass ends.
	 * Implementations should call super.postBackup last.
	 *
	 * @see  #preBackup(FailoverFileReplication)
	 */
	void postBackup(FailoverFileReplication ffr) throws IOException, SQLException;

	/**
	 * Gets the set of paths that must be found in the backup set.  These paths
	 * must not include any trailing separators.
	 */
	Set<String> getRequiredFilenames(FailoverFileReplication ffr) throws IOException, SQLException;

	/**
	 * Gets the <code>Iterator</code> of filenames or <code>null</code> if completed.
	 */
	Iterator<String> getFilenameIterator(FailoverFileReplication ffr) throws IOException, SQLException;

	/**
	 * Gets the default source IP address or <code>InetAddress.UNSPECIFIED</code> to use the system default.
	 */
	InetAddress getDefaultSourceIPAddress() throws IOException, SQLException;

	/**
	 * Gets the list of MySQL server instance names that are being replicated.
	 * This is only used for failover mode (retention==1), and should only
	 * be used for a replication that includes MySQL replication (AOServer only)
	 * 
	 * @see  #getReplicatedMySQLMinorVersions(FailoverFileReplication)
	 */
	List<MySQLServerName> getReplicatedMySQLServers(FailoverFileReplication ffr) throws IOException, SQLException;

	/**
	 * Gets the list of MySQL server versions (in the same order as the list returned by <code>getReplicatedMySQLServers</code>.
	 * This is only used for failover mode (retention==1), and should only
	 * be used for a replication that includes MySQL replication (AOServer only)
	 * 
	 * @see  #getReplicatedMySQLServers(FailoverFileReplication)
	 */
	List<String> getReplicatedMySQLMinorVersions(FailoverFileReplication ffr) throws IOException, SQLException;

	/**
	 * Gets a Random source for the backup daemon.  This does not need to be cryptographically strong
	 * because it is only used to randomize some sleep times to distribute load on the server
	 * processes.
	 */
	Random getRandom() throws IOException, SQLException;

	/**
	 * Gets the logger for this environment.
	 */
	Logger getLogger();

	/**
	 * Converts an environment-specific filename into a server path.  The server
	 * path must begin with /, may not contain /../, and must use / as the path
	 * separator.
	 */
	String getServerPath(FailoverFileReplication ffr, String filename);
}
