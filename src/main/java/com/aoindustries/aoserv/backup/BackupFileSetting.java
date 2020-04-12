/*
 * Copyright 2003-2009, 2018, 2019, 2020 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.backup;

import com.aoindustries.aoserv.client.backup.FileReplicationSetting;

/**
 * Stores the settings for one file that is being backed-up by <code>BackupDaemon</code>.
 *
 * @see  FileReplicationSetting
 *
 * @author  AO Industries, Inc.
 */
public class BackupFileSetting {

	private String filename;
	private boolean backupEnabled;

	public void clear() {
		filename=null;
		backupEnabled=false;
	}

	public String getFilename() {
		return filename;
	}

	public boolean getBackupEnabled() {
		return backupEnabled;
	}

	public void setSettings(
		boolean backupEnabled
	) {
		this.backupEnabled=backupEnabled;
	}

	public void setFilename(
		String filename
	) {
		this.filename=filename;
	}
}
