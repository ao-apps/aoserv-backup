/*
 * Copyright 2003-2009, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.backup;

import com.aoindustries.aoserv.client.backup.FileReplication;
import com.aoindustries.io.unix.Stat;
import com.aoindustries.io.unix.UnixFile;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * A <code>UnixEnvironment</code> controls the backup system on
 * a standalone Unix <code>Host</code>.
 *
 * @see  Host
 *
 * @author  AO Industries, Inc.
 */
abstract public class UnixFileEnvironment extends FileEnvironment {

	private final Object unixFileCacheLock=new Object();
	private Map<FileReplication,File> lastFiles = new HashMap<FileReplication,File>();
	private Map<FileReplication,UnixFile> lastUnixFiles = new HashMap<FileReplication,UnixFile>();
	private Map<FileReplication,Stat> lastStats = new HashMap<FileReplication,Stat>();

	protected UnixFile getUnixFile(FileReplication ffr, String filename) throws IOException {
		File file = getFile(ffr, filename);
		synchronized(unixFileCacheLock) {
			UnixFile lastUnixFile;
			if(file!=lastFiles.get(ffr)) {
				lastUnixFile = new UnixFile(file);
				lastUnixFiles.put(ffr, lastUnixFile);
				lastStats.put(ffr, lastUnixFile.getStat());
				lastFiles.put(ffr, file);
			} else {
				lastUnixFile = lastUnixFiles.get(ffr);
			}
			return lastUnixFile;
		}
	}

	protected Stat getStat(FileReplication ffr, String filename) throws IOException {
		if(filename==null) throw new AssertionError("filename is null");
		File file = getFile(ffr, filename);
		if(file==null) throw new AssertionError("file is null");
		synchronized(unixFileCacheLock) {
			Stat lastStat;
			if(file!=lastFiles.get(ffr)) {
				UnixFile lastUnixFile = new UnixFile(file);
				lastUnixFiles.put(ffr, lastUnixFile);
				lastStat = lastUnixFile.getStat();
				lastStats.put(ffr, lastStat);
				lastFiles.put(ffr, file);
			} else {
				lastStat = lastStats.get(ffr);
			}
			return lastStat;
		}
	}

	@Override
	public long getStatMode(FileReplication ffr, String filename) throws IOException {
		return getStat(ffr, filename).getRawMode();
	}

	@Override
	public int getUid(FileReplication ffr, String filename) throws IOException {
		return getStat(ffr, filename).getUid();
	}

	@Override
	public int getGid(FileReplication ffr, String filename) throws IOException {
		return getStat(ffr, filename).getGid();
	}

	@Override
	public long getModifyTime(FileReplication ffr, String filename) throws IOException {
		return getStat(ffr, filename).getModifyTime();
	}

	@Override
	public long getLength(FileReplication ffr, String filename) throws IOException {
		return getStat(ffr, filename).getSize();
	}

	@Override
	public String readLink(FileReplication ffr, String filename) throws IOException {
		return getUnixFile(ffr, filename).readLink();
	}

	@Override
	public long getDeviceIdentifier(FileReplication ffr, String filename) throws IOException {
		return getStat(ffr, filename).getDeviceIdentifier();
	}

	@Override
	public void cleanup(FileReplication ffr) throws IOException, SQLException {
		try {
			synchronized(unixFileCacheLock) {
				lastFiles.remove(ffr);
				lastUnixFiles.remove(ffr);
				lastStats.remove(ffr);
			}
		} finally {
			super.cleanup(ffr);
		}
	}
}
