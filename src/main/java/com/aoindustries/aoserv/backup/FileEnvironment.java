/*
 * aoserv-backup - Backup client for the AOServ Platform.
 * Copyright (C) 2003-2013, 2017, 2018, 2019, 2020, 2021  AO Industries, Inc.
 *     support@aoindustries.com
 *     7262 Bull Pen Cir
 *     Mobile, AL 36695
 *
 * This file is part of aoserv-backup.
 *
 * aoserv-backup is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * aoserv-backup is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with aoserv-backup.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.aoindustries.aoserv.backup;

import com.aoapps.hodgepodge.io.FilesystemIterator;
import com.aoapps.hodgepodge.io.FilesystemIteratorRule;
import com.aoapps.io.posix.PosixFile;
import com.aoapps.net.InetAddress;
import com.aoindustries.aoserv.client.AOServConnector;
import com.aoindustries.aoserv.client.backup.FileReplication;
import com.aoindustries.aoserv.client.backup.FileReplicationSetting;
import com.aoindustries.aoserv.client.mysql.Server;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * A <code>BackupEnvironment</code> for files.
 *
 * @author  AO Industries, Inc.
 */
public abstract class FileEnvironment implements BackupEnvironment {

	private final Object fileCacheLock=new Object();
	private final Map<FileReplication, String> lastFilenames = new HashMap<>();
	private final Map<FileReplication, File> lastFiles = new HashMap<>();

	protected File getFile(FileReplication ffr, String filename) {
		if(filename==null) throw new AssertionError("filename is null");
		synchronized(fileCacheLock) {
			File lastFile;
			if(!filename.equals(lastFilenames.get(ffr))) {
				lastFile = new File(filename);
				lastFiles.put(ffr, lastFile);
				lastFilenames.put(ffr, filename);
			} else {
				lastFile = lastFiles.get(ffr);
			}
			return lastFile;
		}
	}

	@Override
	public long getStatMode(FileReplication ffr, String filename) throws IOException {
		File file=getFile(ffr, filename);
		if(file.isDirectory()) return PosixFile.IS_DIRECTORY|0750;
		if(file.isFile()) return PosixFile.IS_REGULAR_FILE|0640;
		return 0;
	}

	@Override
	public String[] getDirectoryList(FileReplication ffr, String filename) throws IOException {
		return getFile(ffr, filename).list();
	}

	@Override
	public int getUid(FileReplication ffr, String filename) throws IOException {
		return PosixFile.ROOT_UID;
	}

	@Override
	public int getGid(FileReplication ffr, String filename) throws IOException {
		return PosixFile.ROOT_GID;
	}

	@Override
	public long getModifyTime(FileReplication ffr, String filename) throws IOException {
		return getFile(ffr, filename).lastModified();
	}

	@Override
	public long getLength(FileReplication ffr, String filename) throws IOException {
		return getFile(ffr, filename).length();
	}

	@Override
	public String readLink(FileReplication ffr, String filename) throws IOException {
		throw new IOException("readLink not supported");
	}

	@Override
	public long getDeviceIdentifier(FileReplication ffr, String filename) throws IOException {
		throw new IOException("getDeviceIdentifier not supported");
	}

	@Override
	public InputStream getInputStream(FileReplication ffr, String filename) throws IOException {
		return new FileInputStream(getFile(ffr, filename));
	}

	@Override
	public String getNameOfFile(FileReplication ffr, String filename) {
		return getFile(ffr, filename).getName();
	}

	@Override
	public int getFailoverBatchSize(FileReplication ffr) throws IOException, SQLException {
		return 1000;
	}

	@Override
	@SuppressWarnings("NoopMethodInAbstractClass")
	public void preBackup(FileReplication ffr) throws IOException, SQLException {
	}

	@Override
	@SuppressWarnings("NoopMethodInAbstractClass")
	public void init(FileReplication ffr) throws IOException, SQLException {
	}

	@Override
	public void cleanup(FileReplication ffr) throws IOException, SQLException {
		synchronized(fileCacheLock) {
			lastFilenames.remove(ffr);
			lastFiles.remove(ffr);
		}
	}

	@Override
	@SuppressWarnings("NoopMethodInAbstractClass")
	public void postBackup(FileReplication ffr) throws IOException, SQLException {
	}

	@Override
	public Set<String> getRequiredFilenames(FileReplication ffr) throws IOException, SQLException {
		Set<String> requiredFilenames = new LinkedHashSet<>();
		for(FileReplicationSetting setting : ffr.getFileBackupSettings()) {
			String path = setting.getPath();
			if(path.length()>1 && path.endsWith(File.separator)) path = path.substring(0, path.length()-1);
			if(setting.isRequired()) requiredFilenames.add(path);
		}
		return Collections.unmodifiableSet(requiredFilenames);
	}

	@Override
	public Iterator<String> getFilenameIterator(FileReplication ffr) throws IOException, SQLException {
		// Build the skip list
		Map<String, FilesystemIteratorRule> filesystemRules = getFilesystemIteratorRules(ffr);
		Map<String, FilesystemIteratorRule> filesystemPrefixRules = getFilesystemIteratorPrefixRules(ffr);

		for(FileReplicationSetting setting : ffr.getFileBackupSettings()) {
			filesystemRules.put(
				setting.getPath(),
				setting.getBackupEnabled() ? FilesystemIteratorRule.OK : FilesystemIteratorRule.SKIP
			);
		}

		return new FilesystemIterator(filesystemRules, filesystemPrefixRules).getFilenameIterator();
	}

	/**
	 * Gets the default set of filesystem rules for this environment.
	 * This should not include file backup settings, they will override the
	 * values returned here.  The returned map may be modified, to maintain
	 * internal consistency, return a copy of the map if needed.
	 */
	protected abstract Map<String, FilesystemIteratorRule> getFilesystemIteratorRules(FileReplication ffr) throws IOException, SQLException;

	/**
	 * Gets the default set of filesystem prefix rules for this environment.
	 */
	protected abstract Map<String, FilesystemIteratorRule> getFilesystemIteratorPrefixRules(FileReplication ffr) throws IOException, SQLException;

	@Override
	public InetAddress getDefaultSourceIPAddress() throws IOException, SQLException {
		return InetAddress.UNSPECIFIED_IPV4;
	}

	@Override
	public List<Server.Name> getReplicatedMySQLServers(FileReplication ffr) throws IOException, SQLException {
		return Collections.emptyList();
	}

	@Override
	public List<String> getReplicatedMySQLMinorVersions(FileReplication ffr) throws IOException, SQLException {
		return Collections.emptyList();
	}

	/**
	 * Uses the random source from AOServClient
	 */
	@Override
	public Random getRandom() {
		return AOServConnector.getFastRandom();
	}

	@Override
	public String getServerPath(FileReplication ffr, String filename) {
		String serverPath;
		if(filename.length()==0) throw new AssertionError("Empty filename not expected");
		else if(filename.charAt(0)!=File.separatorChar) serverPath = '/' + filename.replace(File.separatorChar, '/');
		else serverPath = filename.replace(File.separatorChar, '/');
		return serverPath;
	}
}
