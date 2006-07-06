package com.aoindustries.aoserv.backup;

/*
 * Copyright 2003-2006 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.*;
import com.aoindustries.aoserv.daemon.client.AOServDaemonConnector;
import com.aoindustries.io.*;
import com.aoindustries.profiler.*;
import com.aoindustries.util.*;
import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * A <code>BackupEnvironment</code> tells the <code>BackupDaemon</code> how to run.
 *
 * @author  AO Industries, Inc.
 */
abstract public class BackupEnvironment implements BitRateProvider {

    /**
     * The file size where a connection will be made directly to the aoserv-daemon
     * instead of going through the aoserv-master.
     */
    public static final long SEND_DATA_DIRECT_TO_DAEMON_THRESHOLD=1024;
    
    /**
     * The pool size for direct daemon access.
     */
    public static final int DAEMON_ACCESS_POOL_SIZE=8;

    /**
     * Cleanup any resources allocated by <code>init()</code>.
     *
     * @see #init()
     */
    public void cleanup() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, BackupEnvironment.class, "cleanup()", null);
        Profiler.endProfile(Profiler.INSTANTANEOUS);
    }

    /**
     * Gets the settings for a single file.
     */
    public abstract void getBackupFileSetting(BackupFileSetting setting) throws IOException, SQLException;

    /**
     * Gets the list of roots in the filesystems.
     */
    public abstract String[] getFilesystemRoots();

    /**
     * Determines if a path is a possible filesystem root
     */
    public boolean isFilesystemRoot(String filename) {
        Profiler.startProfile(Profiler.FAST, BackupEnvironment.class, "isFilesystemRoot(String)", null);
        try {
            String[] roots=getFilesystemRoots();
            int len=roots.length;
            for(int c=0;c<roots.length;c++) {
                if(roots[c].equals(filename)) return true;
            }
            return false;
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    /**
     * Gets the stat mode (or a generated and equivilent one) for a file.
     */
    public abstract long getStatMode(String filename) throws IOException;

    /**
     * Gets the listing for a directory.
     */
    public abstract String[] getDirectoryList(String filename);

    /**
     * Gets the FileBackupDevice.
     */
    public abstract FileBackupDevice getFileBackupDevice(String filename) throws IOException, SQLException;
    
    /**
     * Gets the inode number, or -1 for none
     */
    public abstract long getInode(String filename) throws IOException;

    /**
     * Gets the user ID, or -1 for none
     */
    public abstract int getUID(String filename) throws IOException;

    /**
     * Gets the group ID, or -1 for none
     */
    public abstract int getGID(String filename) throws IOException;

    /**
     * Gets the modified time
     */
    public abstract long getModifyTime(String filename) throws IOException;

    /**
     * Gets the length of the file
     */
    public abstract long getFileLength(String filename) throws IOException;

    /**
     * Reads a symbolic link
     */
    public abstract String readLink(String filename) throws IOException;

    /**
     * Gets the device file major and minor
     */
    public abstract long getDeviceIdentifier(String filename) throws IOException;

    /**
     * Gets a stream reading the file
     */
    public abstract InputStream getInputStream(String filename) throws IOException;

    /**
     * Gets the name of a file (the part after the last slash)
     */
    public abstract String getNameOfFile(String filename);

    /**
     * Gets the connection to the master server.
     */
    public abstract AOServConnector getConnector() throws IOException, SQLException;

    /**
     * Gets the number of milliseconds before the backup system warns the adminstrator
     * about taking too long.
     */
    public long getWarningTime() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, BackupEnvironment.class, "getWarningTime()", null);
        try {
            return 18L*60*60*1000;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    /**
     * Gets the number of milliseconds between warning reminders.
     */
    public long getWarningReminderInterval() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, BackupEnvironment.class, "getWarningReminderInterval()", null);
        try {
            return 6L*60*60*1000;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    /**
     * Gets the number of milliseconds to wait after the inital program start.
     */
    public long getInitialSleepTime() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, BackupEnvironment.class, "getInitialSleepTime()", null);
        try {
            return 10L*60*1000;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }
    
    /**
     * Gets the number of times a retry will be attempted before reporting an error.
     */
    public int getRetryCount() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, BackupEnvironment.class, "getRetryCount()", null);
        try {
            return 8;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }
    
    /**
     * Gets the number of milliseconds to wait between retry attempts.
     */
    public long getRetryDelay() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, BackupEnvironment.class, "getRetryDelay()", null);
        try {
            return 10L*1000;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }
    
    /**
     * Gets the number of files to process in each batch.
     */
    public int getFileBatchSize() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, BackupEnvironment.class, "getFileBatchSize()", null);
        try {
            return 1000;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }
    
    /**
     * Gets the number of files to flag as deleted per batch.
     */
    public int getDeleteBatchSize() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, BackupEnvironment.class, "getDeleteBatchSize()", null);
        try {
            return 1000;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    /**
     * Gets the from address of emails sent.
     */
    public abstract String getErrorEmailFrom() throws IOException;
    
    /**
     * Gets the from address of emails sent.
     */
    public abstract String getWarningEmailFrom() throws IOException;

    /**
     * Gets the list of email addresses to send messages to.
     */
    public abstract String getErrorEmailTo() throws IOException;

    /**
     * Gets the list of email addresses to send messages to.
     */
    public abstract String getWarningEmailTo() throws IOException;

    /**
     * Gets the maximum number of milliseconds to sleep between batches.
     */
    public long getMaximumBatchSleepTime() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, BackupEnvironment.class, "getMaximumBatchSleepTime()", null);
        try {
            return 10L*60*1000;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    /**
     * Gets the random number generator for the backup process.
     */
    public abstract Random getRandom() throws IOException, SQLException;

    /**
     * Gets the SMTP server used for email.  <code>null</code> indicates
     * that mail should not be sent.
     */
    public abstract String getErrorSmtpServer() throws IOException;

    /**
     * Gets the SMTP server used for email.  <code>null</code> indicates
     * that mail should not be sent.
     */
    public abstract String getWarningSmtpServer() throws IOException;

    /**
     * Gets the server this process is running on.
     */
    public abstract Server getThisServer() throws IOException, SQLException;

    /**
     * Initialize any resources used while processing a backup run.
     */
    public void init() throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, BackupEnvironment.class, "init()", null);
        Profiler.endProfile(Profiler.INSTANTANEOUS);
    }
    
    /**
     * Gets the <code>ErrorHandler</code> for the environment.
     */
    public abstract ErrorHandler getErrorHandler();

    /**
     * Called right before a backup begins.
     */
    public void preBackup() throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, BackupEnvironment.class, "preBackup()", null);
        Profiler.endProfile(Profiler.INSTANTANEOUS);
    }

    /**
     * Called right after a backup ends.
     */
    public void postBackup() throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, BackupEnvironment.class, "postBackup()", null);
        Profiler.endProfile(Profiler.INSTANTANEOUS);
    }
    
    /**
     * Sends the backup data to the backup storage server. By
     * default, files less than 1024 bytes are sent through the
     * master server (lower latency), while larger files establish
     * a direct connection with the relevant aoserv-daemon (higher
     * bandwidth).
     */
    public boolean sendData(
        AOServConnector conn,
        int backupData,
        InputStream in,
        String tempFilename,
        long length,
        long md5_hi,
        long md5_lo,
        boolean fileIsCompressed,
        boolean shouldBeCompressed
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupEnvironment.class, "sendData(AOServConnector,int,InputStream,String,long,long,long,boolean,boolean)", null);
        try {
            if(length>=SEND_DATA_DIRECT_TO_DAEMON_THRESHOLD) {
                return sendDataToDaemon(
                    conn,
                    backupData,
                    in,
                    tempFilename,
                    length,
                    md5_hi,
                    md5_lo,
                    fileIsCompressed,
                    shouldBeCompressed
                );
            } else {
                return sendDataToMaster(
                    conn,
                    backupData,
                    in,
                    tempFilename,
                    length,
                    md5_hi,
                    md5_lo,
                    fileIsCompressed,
                    shouldBeCompressed
                );
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    } 

    public boolean sendDataToMaster(
        AOServConnector conn,
        int backupData,
        InputStream in,
        String tempFilename,
        long length,
        long md5_hi,
        long md5_lo,
        boolean fileIsCompressed,
        boolean shouldBeCompressed
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupEnvironment.class, "sendDataToMaster(AOServConnector,int,InputStream,String,long,long,long,boolean,boolean)", null);
        try {
            return conn.backupDatas.sendData(
                backupData,
                in,
                tempFilename,
                length,
                md5_hi,
                md5_lo,
                fileIsCompressed,
                shouldBeCompressed,
                this
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    } 

    public boolean sendDataToDaemon(
        AOServConnector conn,
        int backupData,
        InputStream in,
        String tempFilename,
        long length,
        long md5_hi,
        long md5_lo,
        boolean fileIsCompressed,
        boolean shouldBeCompressed
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupEnvironment.class, "sendDataToDaemon(AOServConnector,int,InputStream,String,long,long,long,boolean,boolean)", null);
        try {
            // 1) Request direct daemon access from the master server
            BackupDataTable.SendDataToDaemonAccess access = conn.backupDatas.requestSendBackupDataToDaemon(
                backupData,
                md5_hi,
                md5_lo
            );
            if(access==null) {
                // Already stored by this or another process, consider as committed
                return true;
            }
            AOServDaemonConnector connector = AOServDaemonConnector.getConnector(
                access.getAOServerPKey(),
                access.getHost(),
                access.getPort(),
                access.getProtocol().getProtocol(),
                null,
                DAEMON_ACCESS_POOL_SIZE,
                AOPool.DEFAULT_MAX_CONNECTION_AGE,
                SSLConnector.class,
                SSLConnector.sslProviderLoaded,
                AOServClientConfiguration.getProperty("aoserv.client.ssl.truststore.path"),
                AOServClientConfiguration.getProperty("aoserv.client.ssl.truststore.password"),
                getErrorHandler()
            );
            connector.sendData(
                access.getKey(),
                in,
                tempFilename,
                length,
                fileIsCompressed,
                shouldBeCompressed,
                this
            );
            // If no exception is thrown, then the daemon has accepted the data
            return true;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    } 
}