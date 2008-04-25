package com.aoindustries.aoserv.backup;

/*
 * Copyright 2003-2008 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.FailoverFileReplication;
import com.aoindustries.io.unix.Stat;
import com.aoindustries.io.unix.UnixFile;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * A <code>UnixEnvironment</code> controls the backup system on
 * a standalone Unix <code>Server</code>.
 *
 * @see  Server
 *
 * @author  AO Industries, Inc.
 */
abstract public class UnixFileEnvironment extends FileEnvironment {
    
    private final Object unixFileCacheLock=new Object();
    private Map<FailoverFileReplication,File> lastFiles = new HashMap<FailoverFileReplication,File>();
    private Map<FailoverFileReplication,UnixFile> lastUnixFiles = new HashMap<FailoverFileReplication,UnixFile>();
    private Map<FailoverFileReplication,Stat> lastStats = new HashMap<FailoverFileReplication,Stat>();

    protected UnixFile getUnixFile(FailoverFileReplication ffr, String filename) throws IOException {
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

    protected Stat getStat(FailoverFileReplication ffr, String filename) throws IOException {
        File file = getFile(ffr, filename);
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
    public long getStatMode(FailoverFileReplication ffr, String filename) throws IOException {
        return getStat(ffr, filename).getRawMode();
    }

    @Override
    public int getUID(FailoverFileReplication ffr, String filename) throws IOException {
        return getStat(ffr, filename).getUID();
    }

    @Override
    public int getGID(FailoverFileReplication ffr, String filename) throws IOException {
        return getStat(ffr, filename).getGID();
    }

    @Override
    public long getModifyTime(FailoverFileReplication ffr, String filename) throws IOException {
        return getStat(ffr, filename).getModifyTime();
    }

    @Override
    public long getLength(FailoverFileReplication ffr, String filename) throws IOException {
        return getStat(ffr, filename).getSize();
    }

    @Override
    public String readLink(FailoverFileReplication ffr, String filename) throws IOException {
        return getUnixFile(ffr, filename).readLink();
    }

    @Override
    public long getDeviceIdentifier(FailoverFileReplication ffr, String filename) throws IOException {
        return getStat(ffr, filename).getDeviceIdentifier();
    }
    
    @Override
    public void cleanup(FailoverFileReplication ffr) throws IOException, SQLException {
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
