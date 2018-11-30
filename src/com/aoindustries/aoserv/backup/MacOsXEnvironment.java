/*
 * Copyright 2008-2009, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.backup;

import com.aoindustries.aoserv.client.backup.FailoverFileReplication;
import com.aoindustries.io.FilesystemIteratorRule;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author  AO Industries, Inc.
 */
abstract public class MacOsXEnvironment extends FileEnvironment {

	@Override
	protected Map<String,FilesystemIteratorRule> getFilesystemIteratorRules(FailoverFileReplication ffr) throws IOException, SQLException {
		Map<String,FilesystemIteratorRule> filesystemRules=new HashMap<String,FilesystemIteratorRule>();
		return filesystemRules;
	}

	@Override
	protected Map<String,FilesystemIteratorRule> getFilesystemIteratorPrefixRules(FailoverFileReplication ffr) throws IOException, SQLException {
		Map<String,FilesystemIteratorRule> filesystemPrefixRules = new HashMap<String, FilesystemIteratorRule>();
		return filesystemPrefixRules;
	}
}
