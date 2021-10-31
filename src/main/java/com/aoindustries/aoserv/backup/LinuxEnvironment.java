/*
 * aoserv-backup - Backup client for the AOServ Platform.
 * Copyright (C) 2008, 2009, 2018, 2019, 2021  AO Industries, Inc.
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
 * along with aoserv-backup.  If not, see <https://www.gnu.org/licenses/>.
 */
package com.aoindustries.aoserv.backup;

import com.aoapps.hodgepodge.io.FilesystemIteratorRule;
import com.aoindustries.aoserv.client.backup.FileReplication;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * A <code>LinuxEnvironment</code> extends <code>PosixFileEnvironment</code> to
 * have some default exclusions, such as <code>/proc/</code>.
 *
 * @author  AO Industries, Inc.
 */
public abstract class LinuxEnvironment extends PosixFileEnvironment {

	@Override
	protected Map<String, FilesystemIteratorRule> getFilesystemIteratorRules(FileReplication ffr) throws IOException, SQLException {
		Map<String, FilesystemIteratorRule> filesystemRules = new HashMap<>();
		filesystemRules.put("/dev/log", FilesystemIteratorRule.SKIP);
		filesystemRules.put("/dev/pts/", FilesystemIteratorRule.SKIP);
		filesystemRules.put("/dev/shm/", FilesystemIteratorRule.SKIP);
		filesystemRules.put("/proc/", FilesystemIteratorRule.SKIP);
		filesystemRules.put("/selinux/", FilesystemIteratorRule.SKIP);
		filesystemRules.put("/sys/", FilesystemIteratorRule.SKIP);
		return filesystemRules;
	}

	@Override
	protected Map<String, FilesystemIteratorRule> getFilesystemIteratorPrefixRules(FileReplication ffr) throws IOException, SQLException {
		Map<String, FilesystemIteratorRule> filesystemPrefixRules = new HashMap<>();
		return filesystemPrefixRules;
	}
}
