package com.aoindustries.aoserv.backup;

/*
 * Copyright 2003-2007 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.Package;
import com.aoindustries.aoserv.client.*;
import com.aoindustries.profiler.*;

/**
 * Stores the settings for one file that is being backed-up by <code>BackupDaemon</code>.
 *
 * @see  BackupEnvironment#getBackupFileSetting
 *
 * @author  AO Industries, Inc.
 */
public class BackupFileSetting {

    private String filename;

    private Package packageObj;
    private BackupLevel backupLevel;
    private BackupRetention backupRetention;
    private boolean recurse;

    public void clear() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, BackupFileSetting.class, "clear()", null);
        try {
            filename=null;
            packageObj=null;
            backupLevel=null;
            backupRetention=null;
            recurse=false;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }
    
    public String getFilename() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, BackupFileSetting.class, "getFilename()", null);
        try {
            return filename;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public Package getPackage() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, BackupFileSetting.class, "getPackage()", null);
        try {
            return packageObj;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public BackupLevel getBackupLevel() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, BackupFileSetting.class, "getBackupLevel()", null);
        try {
            return backupLevel;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public BackupRetention getBackupRetention() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, BackupFileSetting.class, "getBackupRetention()", null);
        try {
            return backupRetention;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }
    
    public boolean isRecursible() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, BackupFileSetting.class, "isRecursible()", null);
        try {
            return recurse;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void setSettings(
        Package packageObj,
        BackupLevel backupLevel,
        BackupRetention backupRetention
    ) {
        Profiler.startProfile(Profiler.INSTANTANEOUS, BackupFileSetting.class, "setSettings(Package,BackupLevel,BackupRetention)", null);
        try {
            setSettings(
                packageObj,
                backupLevel,
                backupRetention,
                backupLevel.getLevel()>BackupLevel.DO_NOT_BACKUP
            );
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void setSettings(
        Package packageObj,
        BackupLevel backupLevel,
        BackupRetention backupRetention,
        boolean recurse
    ) {
        Profiler.startProfile(Profiler.INSTANTANEOUS, BackupFileSetting.class, "setSettings(Package,BackupLevel,BackupRetention,boolean)", null);
        try {
            this.packageObj=packageObj;
            this.backupLevel=backupLevel;
            this.backupRetention=backupRetention;
            this.recurse=recurse;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void setRecurse(boolean recurse) {
        this.recurse=recurse;
    }

    public void setFilename(
        String filename
    ) {
        Profiler.startProfile(Profiler.INSTANTANEOUS, BackupFileSetting.class, "setFilename(String)", null);
        try {
            this.filename=filename;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }
}