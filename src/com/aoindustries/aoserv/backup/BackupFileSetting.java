package com.aoindustries.aoserv.backup;

/*
 * Copyright 2003-2008 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */

/**
 * Stores the settings for one file that is being backed-up by <code>BackupDaemon</code>.
 *
 * @see  BackupEnvironment#getBackupFileSetting
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