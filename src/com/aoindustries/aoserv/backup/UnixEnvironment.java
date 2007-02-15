package com.aoindustries.aoserv.backup;

/*
 * Copyright 2003-2007 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.Package;
import com.aoindustries.aoserv.client.*;
import com.aoindustries.email.*;
import com.aoindustries.io.unix.*;
import com.aoindustries.profiler.*;
import com.aoindustries.sql.*;
import com.aoindustries.util.*;
import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * A <code>UnixEnvironment</code> controls the backup system on
 * a standalone Unix <code>Server</code>.
 *
 * @see  Server
 *
 * @author  AO Industries, Inc.
 */
public class UnixEnvironment extends StandaloneEnvironment {
    
    protected FileBackupDeviceTable fileBackupDeviceTable;

    public void init() throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, UnixEnvironment.class, "init()", null);
        try {
            super.init();
            AOServConnector conn=getConnector();
            fileBackupDeviceTable=conn.fileBackupDevices;
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public void cleanup() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, UnixEnvironment.class, "cleanup()", null);
        try {
            super.cleanup();
            fileBackupDeviceTable=null;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    private final Object unixFileCacheLock=new Object();
    private String lastFilename;
    private UnixFile lastUnixFile;
    private UnixFile getUnixFile(String filename) {
        Profiler.startProfile(Profiler.FAST, UnixEnvironment.class, "getUnixFile(String)", null);
        try {
            synchronized(unixFileCacheLock) {
                if(!filename.equals(lastFilename)) {
                    lastFilename=filename;
                    lastUnixFile=new UnixFile(filename);
                }
                return lastUnixFile;
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    private static final String[] roots={"/"};

    public String[] getFilesystemRoots() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, UnixEnvironment.class, "getFilesystemRoots()", null);
        try {
            return roots;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public long getStatMode(String filename) throws IOException {
        Profiler.startProfile(Profiler.FAST, UnixEnvironment.class, "getStatMode(String)", null);
        try {
            return getUnixFile(filename).getStat().getRawMode();
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public String[] getDirectoryList(String filename) {
        Profiler.startProfile(Profiler.FAST, UnixEnvironment.class, "getDirectoryList(String)", null);
        try {
            return getUnixFile(filename).list();
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public FileBackupDevice getFileBackupDevice(String filename) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, UnixEnvironment.class, "getFileBackupDevice(String)", null);
        try {
            long device=getUnixFile(filename).getStat().getDevice();
            FileBackupDevice dev=fileBackupDeviceTable.get(device);
            if(dev==null) throw new IOException("Unable to find FileBackupDevice for '"+filename+"': "+device);
            return dev;
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public long getInode(String filename) throws IOException {
        Profiler.startProfile(Profiler.FAST, UnixEnvironment.class, "getInode(String)", null);
        try {
            long inode=getUnixFile(filename).getStat().getInode();
            if(inode==-1) throw new IOException("Inode value of -1 conflicts with internal use of -1 as null");
            return inode;
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public int getUID(String filename) throws IOException {
        Profiler.startProfile(Profiler.FAST, UnixEnvironment.class, "getUID(String)", null);
        try {
            return getUnixFile(filename).getStat().getUID();
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public int getGID(String filename) throws IOException {
        Profiler.startProfile(Profiler.FAST, UnixEnvironment.class, "getGID(String)", null);
        try {
            return getUnixFile(filename).getStat().getGID();
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public long getModifyTime(String filename) throws IOException {
        Profiler.startProfile(Profiler.FAST, UnixEnvironment.class, "getModifyTime(String)", null);
        try {
            return getUnixFile(filename).getStat().getModifyTime();
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public long getFileLength(String filename) {
        Profiler.startProfile(Profiler.FAST, UnixEnvironment.class, "getFileLength(String)", null);
        try {
            return getUnixFile(filename).getFile().length();
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public String readLink(String filename) throws IOException {
        Profiler.startProfile(Profiler.FAST, UnixEnvironment.class, "readLink(String)", null);
        try {
            return getUnixFile(filename).readLink();
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public long getDeviceIdentifier(String filename) throws IOException {
        Profiler.startProfile(Profiler.FAST, UnixEnvironment.class, "getDeviceIdentifier(String)", null);
        try {
            return getUnixFile(filename).getStat().getDeviceIdentifier();
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public InputStream getInputStream(String filename) throws IOException {
        Profiler.startProfile(Profiler.FAST, UnixEnvironment.class, "getInputStream(String)", null);
        try {
            return new FileInputStream(getUnixFile(filename).getFile());
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public String getNameOfFile(String filename) {
        Profiler.startProfile(Profiler.FAST, UnixEnvironment.class, "getNameOfFile(String)", null);
        try {
            return getUnixFile(filename).getFile().getName();
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public void getBackupFileSetting(BackupFileSetting fileSetting) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, UnixEnvironment.class, "getBackupFileSetting(BackupFileSetting)", null);
        try {
            String filename=fileSetting.getFilename();

            // By default, just recurse deeper looking for stuff to backup
            fileSetting.setSettings(defaultPackage, doNotBackup, defaultBackupRetention, true);

            // First, the <code>FileBackupSetting</code>s come into play.
            List<FileBackupSetting> fbss=fileBackupSettingTable.getRows();
            final int len=fbss.size();
            FileBackupSetting bestMatch=null;
            for(int c=0;c<fbss.size();c++) {
                FileBackupSetting fbs=fbss.get(c);
                if(
                    fbs.getServer().equals(thisServer)
                    && filename.startsWith(fbs.getPath())
                    && (
                        bestMatch==null
                        || fbs.getPath().length()>bestMatch.getPath().length()
                    )
                ) bestMatch=fbs;
            }
            if(bestMatch!=null) {
                fileSetting.setSettings(
                    bestMatch.getPackage(),
                    bestMatch.getBackupLevel(),
                    bestMatch.getBackupRetention(),
                    bestMatch.isRecursible()
                );
            }

            // Second, the FileBackupDevices take effect overlaying a no recurse over previous settings
            UnixFile unixFile=getUnixFile(filename);
            long device=unixFile.getStat().getDevice();
            FileBackupDevice fileDevice=fileBackupDeviceTable.get(device);
            if(fileDevice==null) throw new IOException("Unable to find FileBackupDevice for device #"+device+": filename='"+filename+'\'');
            if(!fileDevice.canBackup()) {
                fileSetting.setRecurse(false);
            } else if(bestMatch==null) {
                // Third, some hard-coded no-recurse directories
                if(
                    filename.equals("/mnt/cdrom")
                    || filename.equals("/mnt/floppy")
                    || filename.equals("/tmp")
                    || filename.equals("/var/tmp")
                ) {
                    fileSetting.setRecurse(false);
                }
            }
            
            // If flagged for recursion but no-backup, then make not recurse if no backed-up children paths may exist
            if(fileSetting.isRecursible() && fileSetting.getBackupLevel().getLevel()==BackupLevel.DO_NOT_BACKUP) {
                boolean found=false;
                for(int c=0;c<len;c++) {
                    FileBackupSetting fbs=fbss.get(c);
                    if(
                        fbs.getBackupLevel().getLevel()>BackupLevel.DO_NOT_BACKUP
                        && fbs.getPath().startsWith(filename)
                    ) {
                        found=true;
                        break;
                    }
                }
                if(!found) fileSetting.setRecurse(false);
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }
}