package com.aoindustries.aoserv.backup;

/*
 * Copyright 2003-2007 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.Package;
import com.aoindustries.aoserv.client.*;
import com.aoindustries.email.*;
import com.aoindustries.profiler.*;
import com.aoindustries.sql.*;
import com.aoindustries.util.*;
import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * A <code>StandaloneEnvironment</code> controls the backup system on
 * running in standalone (non-embedded) mode.
 *
 * @see  Server
 *
 * @author  AO Industries, Inc.
 */
abstract public class StandaloneEnvironment extends BackupEnvironment implements ErrorHandler {
    
    protected Server thisServer;
    protected Package defaultPackage;
    protected FileBackupSettingTable fileBackupSettingTable;
    protected BackupLevel defaultBackupLevel;
    protected BackupLevel doNotBackup;
    protected BackupRetention defaultBackupRetention;

    public void init() throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, StandaloneEnvironment.class, "init()", null);
        try {
            AOServConnector conn=getConnector();
            thisServer=getThisServer();
            String defaultPackageName=BackupDaemonConfiguration.getProperty("aoserv.backup.default.package");
            defaultPackage=conn.packages.get(defaultPackageName);
            if(defaultPackage==null) throw new SQLException("Unable to find default Package: "+defaultPackageName);
            fileBackupSettingTable=conn.fileBackupSettings;
            defaultBackupLevel=conn.backupLevels.get(BackupLevel.DEFAULT_BACKUP_LEVEL);
            if(defaultBackupLevel==null) throw new SQLException("Unable to find BackupLevel: "+BackupLevel.DEFAULT_BACKUP_LEVEL);
            doNotBackup=conn.backupLevels.get(BackupLevel.DO_NOT_BACKUP);
            if(doNotBackup==null) throw new SQLException("Unable to find BackupLevel: "+BackupLevel.DO_NOT_BACKUP);
            defaultBackupRetention=conn.backupRetentions.get(BackupRetention.DEFAULT_BACKUP_RETENTION);
            if(defaultBackupRetention==null) throw new SQLException("Unable to find BackupRetention: "+BackupRetention.DEFAULT_BACKUP_RETENTION);
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public void cleanup() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, StandaloneEnvironment.class, "cleanup()", null);
        try {
            thisServer=null;
            defaultPackage=null;
            fileBackupSettingTable=null;
            defaultBackupLevel=null;
            doNotBackup=null;
            defaultBackupRetention=null;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    private AOServConnector conn;
    public AOServConnector getConnector() throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, StandaloneEnvironment.class, "getConnector()", null);
        try {
	    synchronized(this) {
		if(conn==null) conn=AOServConnector.getConnector(this);
		return conn;
	    }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public String getErrorEmailFrom() throws IOException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, StandaloneEnvironment.class, "getErrorEmailFrom()", null);
        try {
            return BackupDaemonConfiguration.getProperty("aoserv.backup.error.email.from");
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public String getWarningEmailFrom() throws IOException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, StandaloneEnvironment.class, "getWarningEmailFrom()", null);
        try {
            return BackupDaemonConfiguration.getProperty("aoserv.backup.warning.email.from");
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public String getErrorEmailTo() throws IOException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, StandaloneEnvironment.class, "getErrorEmailTo()", null);
        try {
            return BackupDaemonConfiguration.getProperty("aoserv.backup.error.email.to");
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public String getWarningEmailTo() throws IOException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, StandaloneEnvironment.class, "getWarningEmailTo()", null);
        try {
            return BackupDaemonConfiguration.getProperty("aoserv.backup.warning.email.to");
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public Random getRandom() throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, StandaloneEnvironment.class, "getRandom()", null);
        try {
            return getConnector().getRandom();
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public String getErrorSmtpServer() throws IOException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, StandaloneEnvironment.class, "getErrorSmtpServer()", null);
        try {
            return BackupDaemonConfiguration.getProperty("aoserv.backup.error.smtp.server");
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public String getWarningSmtpServer() throws IOException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, StandaloneEnvironment.class, "getWarningSmtpServer()", null);
        try {
            return BackupDaemonConfiguration.getProperty("aoserv.backup.warning.smtp.server");
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public Server getThisServer() throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, StandaloneEnvironment.class, "getThisServer()", null);
        try {
            String hostname=BackupDaemonConfiguration.getProperty("aoserv.backup.server.hostname");
            Server server=getConnector().servers.get(hostname);
            if(server==null) throw new SQLException("Unable to find Server: "+hostname);
            return server;
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }
    
    public void reportError(Throwable T, Object[] extraInfo) {
        Profiler.startProfile(Profiler.UNKNOWN, StandaloneEnvironment.class, "reportError(Throwable,Object[])", null);
        try {
            ErrorPrinter.printStackTraces(T, extraInfo);
            try {
                String smtp=getErrorSmtpServer();
                if(smtp!=null && smtp.length()>0) {
                    String from=getErrorEmailFrom();
                    List<String> addys=StringUtility.splitStringCommaSpace(getErrorEmailTo());
                    for(int c=0;c<addys.size();c++) {
                        ErrorMailer.reportError(
                            getRandom(),
                            T,
                            extraInfo,
                            smtp,
                            from,
                            addys.get(c),
                            "AOServBackup Error"
                        );
                    }
                }
            } catch(IOException err) {
                ErrorPrinter.printStackTraces(err);
            } catch(SQLException err) {
                ErrorPrinter.printStackTraces(err);
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    
    public void reportWarning(Throwable T, Object[] extraInfo) {
        Profiler.startProfile(Profiler.UNKNOWN, StandaloneEnvironment.class, "reportWarning(Throwable,Object[])", null);
        try {
            ErrorPrinter.printStackTraces(T, extraInfo);
            try {
                String smtp=getWarningSmtpServer();
                if(smtp!=null && smtp.length()>0) {
                    String from=getWarningEmailFrom();
                    List<String> addys=StringUtility.splitStringCommaSpace(getWarningEmailTo());
                    for(int c=0;c<addys.size();c++) {
                        ErrorMailer.reportError(
                            getRandom(),
                            T,
                            extraInfo,
                            smtp,
                            from,
                            addys.get(c),
                            "AOServBackup Warning"
                        );
                    }
                }
            } catch(IOException err) {
                ErrorPrinter.printStackTraces(err);
            } catch(SQLException err) {
                ErrorPrinter.printStackTraces(err);
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public ErrorHandler getErrorHandler() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, StandaloneEnvironment.class, "getErrorHandler()", null);
        try {
            return this;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    /**
     * Block transfer sizes used by the <code>BitRateOutputStream</code>.
     */
    public int getBlockSize() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, StandaloneEnvironment.class, "getBlockSize()", null);
        try {
            return BufferManager.BUFFER_SIZE;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }
    
    /**
     * The bits per second for transfers.
     */
    public int getBitRate() throws IOException {
        Profiler.startProfile(Profiler.FAST, StandaloneEnvironment.class, "getBitRate()", null);
        try {
            String S=BackupDaemonConfiguration.getProperty("aoserv.backup.bandwidth.limit");
            return S==null || S.length()==0?-1:Integer.parseInt(S);
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }
}