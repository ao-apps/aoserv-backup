/*
 * Copyright 2008-2009, 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.backup;

import com.aoindustries.aoserv.client.backup.FileReplication;
import com.aoindustries.io.FilesystemIteratorRule;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author  AO Industries, Inc.
 */
abstract public class WindowsEnvironment extends FileEnvironment {

	@Override
	protected Map<String,FilesystemIteratorRule> getFilesystemIteratorRules(FileReplication ffr) throws IOException, SQLException {
		Map<String,FilesystemIteratorRule> filesystemRules = new HashMap<>();
		return filesystemRules;
	}

	@Override
	protected Map<String,FilesystemIteratorRule> getFilesystemIteratorPrefixRules(FileReplication ffr) throws IOException, SQLException {
		Map<String,FilesystemIteratorRule> filesystemPrefixRules = new HashMap<>();
		return filesystemPrefixRules;
	}

	/**
	 * Because some of the backup servers use ext2/ext3 filesystems, which have only one-second
	 * time accuracy, timestamps are rounded down to the nearest second.  (Truncates the milliseconds component)
	 * When all servers support more granular file timestamps, or if we store the more
	 * accurate timestamps in extended attributes, then we can remove this for potentially greater
	 * accuracy in detecting file changes.
	 */
	@Override
	public long getModifyTime(FileReplication ffr, String filename) throws IOException {
		return (super.getModifyTime(ffr, filename)/1000) * 1000;
	}
}
