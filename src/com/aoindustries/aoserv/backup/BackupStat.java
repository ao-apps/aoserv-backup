package com.aoindustries.aoserv.backup;

/*
 * Copyright 2001-2006 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.*;
import com.aoindustries.profiler.*;
import java.io.*;
import java.sql.*;

/**
 * Stores the statistics as they are being collected by <code>BackupDaemon</code>.
 *
 * @see  BackupDaemon
 *
 * @author  AO Industries, Inc.
 */
public class BackupStat {

    final private BackupEnvironment environment;

    final long startTime=System.currentTimeMillis();
    int scanned=0;
    int file_backup_attribute_matches=0;
    int not_matched_md5_files=0;
    int not_matched_md5_failures=0;
    int send_missing_backup_data_files=0;
    int send_missing_backup_data_failures=0;
    int temp_files=0;
    int temp_send_backup_data_files=0;
    int temp_failures=0;
    int added=0;
    int deleted=0;
    boolean is_successful=false;

    public BackupStat(BackupEnvironment environment) {
        Profiler.startProfile(Profiler.INSTANTANEOUS, BackupStat.class, "<init>(BackupEnvironment)", null);
        try {
            this.environment=environment;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    void commit() throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupStat.class, "commit()", null);
        try {
            int retries=0;
            while(retries<environment.getRetryCount()) {
                retries++;
                try {
                    environment.getThisServer().addFileBackupStat(
                        startTime,
                        System.currentTimeMillis(),
                        scanned,
                        file_backup_attribute_matches,
                        not_matched_md5_files,
                        not_matched_md5_failures,
                        send_missing_backup_data_files,
                        send_missing_backup_data_failures,
                        temp_files,
                        temp_send_backup_data_files,
                        temp_failures,
                        added,
                        deleted,
                        is_successful
                    );
                    break;
                } catch(IOException err) {
                    if(retries<environment.getRetryCount()) {
                        environment.getErrorHandler().reportWarning(
                            err,
                            new Object[] {"retries="+retries, "environment.getRetryCount()="+environment.getRetryCount()}
                        );
                    } else throw err;
                } catch(SQLException err) {
                    if(retries<environment.getRetryCount()) {
                        environment.getErrorHandler().reportWarning(
                            err,
                            new Object[] {"retries="+retries, "environment.getRetryCount()="+environment.getRetryCount()}
                        );
                    } else throw err;
                }
                try {
                    Thread.sleep(environment.getRetryDelay());
                } catch(InterruptedException err) {
                    InterruptedIOException ioErr=new InterruptedIOException();
                    ioErr.initCause(err);
                    throw ioErr;
                }
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
}