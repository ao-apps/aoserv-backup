package com.aoindustries.aoserv.backup;

/*
 * Copyright 2003-2006 by AO Industries, Inc.,
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
 * A <code>JavaEnvironment</code> controls the backup system on
 * any Java platform.
 *
 * @see  Server
 *
 * @author  AO Industries, Inc.
 */
public class JavaEnvironment extends StandaloneEnvironment {

    private String[] roots;

    public void init() throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, JavaEnvironment.class, "init()", null);
        try {
            super.init();
            List<FileBackupSetting> settings=getConnector().fileBackupSettings.getRows();

            List<String> tempRoots=new ArrayList<String>();
            File[] fileRoots=File.listRoots();
            for(int c=0;c<fileRoots.length;c++) {
                String root=fileRoots[c].getPath().replace('\\', '/');
                // Only add if this root is used for at least one backup setting
                boolean found=false;
                for(int d=0;d<settings.size();d++) {
                    if(settings.get(d).getPath().startsWith(root)) {
                        found=true;
                        break;
                    }
                }
                if(found) tempRoots.add(root);
            }
            roots=new String[tempRoots.size()];
            tempRoots.toArray(roots);
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public void cleanup() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, JavaEnvironment.class, "cleanup()", null);
        try {
            super.cleanup();
            roots=null;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    private final Object fileCacheLock=new Object();
    private String lastFilename;
    private File lastFile;
    private File getFile(String filename) {
        Profiler.startProfile(Profiler.FAST, JavaEnvironment.class, "getFile(String)", null);
        try {
            synchronized(fileCacheLock) {
                if(!filename.equals(lastFilename)) {
                    lastFilename=filename;
                    lastFile=new File(filename);
                }
                return lastFile;
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public String[] getFilesystemRoots() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, JavaEnvironment.class, "getFilesystemRoots()", null);
        try {
            return roots;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public long getStatMode(String filename) throws IOException {
        Profiler.startProfile(Profiler.FAST, JavaEnvironment.class, "getStatMode(String)", null);
        try {
            File file=getFile(filename);
            if(file.isDirectory()) return UnixFile.IS_DIRECTORY;
            if(file.isFile()) return UnixFile.IS_REGULAR_FILE;
            return 0;
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public String[] getDirectoryList(String filename) {
        Profiler.startProfile(Profiler.FAST, JavaEnvironment.class, "getDirectoryList(String)", null);
        try {
            return getFile(filename).list();
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public FileBackupDevice getFileBackupDevice(String filename) throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, JavaEnvironment.class, "getFileBackupDevice(String)", null);
        try {
            return null;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public long getInode(String filename) {
        Profiler.startProfile(Profiler.INSTANTANEOUS, JavaEnvironment.class, "getInode(String)", null);
        try {
            return -1;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public int getUID(String filename) {
        Profiler.startProfile(Profiler.INSTANTANEOUS, JavaEnvironment.class, "getUID(String)", null);
        try {
            return -1;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public int getGID(String filename) {
        Profiler.startProfile(Profiler.INSTANTANEOUS, JavaEnvironment.class, "getGID(String)", null);
        try {
            return -1;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public long getModifyTime(String filename) {
        Profiler.startProfile(Profiler.FAST, JavaEnvironment.class, "getModifyTime(String)", null);
        try {
            return getFile(filename).lastModified();
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public long getFileLength(String filename) {
        Profiler.startProfile(Profiler.FAST, JavaEnvironment.class, "getFileLength(String)", null);
        try {
            return getFile(filename).length();
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public String readLink(String filename) throws IOException {
        Profiler.startProfile(Profiler.FAST, JavaEnvironment.class, "readLink(String)", null);
        try {
            throw new IOException("readLink not supported");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public long getDeviceIdentifier(String filename) throws IOException {
        Profiler.startProfile(Profiler.FAST, JavaEnvironment.class, "getDeviceIdentifier(String)", null);
        try {
            throw new IOException("getDeviceIdentifier not supported");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public InputStream getInputStream(String filename) throws IOException {
        Profiler.startProfile(Profiler.FAST, JavaEnvironment.class, "getInputStream(String)", null);
        try {
            return new FileInputStream(getFile(filename));
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public String getNameOfFile(String filename) {
        Profiler.startProfile(Profiler.FAST, JavaEnvironment.class, "getNameOfFile(String)", null);
        try {
            return getFile(filename).getName();
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public void getBackupFileSetting(BackupFileSetting fileSetting) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, JavaEnvironment.class, "getBackupFileSetting(BackupFileSetting)", null);
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