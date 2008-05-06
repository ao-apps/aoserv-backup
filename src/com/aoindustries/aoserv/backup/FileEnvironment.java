package com.aoindustries.aoserv.backup;

/*
 * Copyright 2003-2008 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.FailoverFileReplication;
import com.aoindustries.aoserv.client.FileBackupSetting;
import com.aoindustries.aoserv.client.IPAddress;
import com.aoindustries.io.FilesystemIterator;
import com.aoindustries.io.FilesystemIteratorRule;
import com.aoindustries.io.unix.UnixFile;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * A <code>BackupEnvironment</code> for files.
 *
 * @author  AO Industries, Inc.
 */
abstract public class FileEnvironment implements BackupEnvironment {

    private final Object fileCacheLock=new Object();
    private Map<FailoverFileReplication,String> lastFilenames = new HashMap<FailoverFileReplication,String>();
    private Map<FailoverFileReplication,File> lastFiles = new HashMap<FailoverFileReplication, File>();

    protected File getFile(FailoverFileReplication ffr, String filename) {
        if(filename==null) throw new AssertionError("filename is null");
        synchronized(fileCacheLock) {
            File lastFile;
            if(!filename.equals(lastFilenames.get(ffr))) {
                lastFile = new File(filename);
                lastFiles.put(ffr, lastFile);
                lastFilenames.put(ffr, filename);
            } else {
                lastFile = lastFiles.get(ffr);
            }
            return lastFile;
        }
    }

    @Override
    public long getStatMode(FailoverFileReplication ffr, String filename) throws IOException {
        File file=getFile(ffr, filename);
        if(file.isDirectory()) return UnixFile.IS_DIRECTORY|0750;
        if(file.isFile()) return UnixFile.IS_REGULAR_FILE|0640;
        return 0;
    }

    @Override
    public String[] getDirectoryList(FailoverFileReplication ffr, String filename) throws IOException {
        return getFile(ffr, filename).list();
    }

    @Override
    public int getUID(FailoverFileReplication ffr, String filename) throws IOException {
        return UnixFile.ROOT_UID;
    }

    @Override
    public int getGID(FailoverFileReplication ffr, String filename) throws IOException {
        return UnixFile.ROOT_GID;
    }

    @Override
    public long getModifyTime(FailoverFileReplication ffr, String filename) throws IOException {
        return getFile(ffr, filename).lastModified();
    }

    @Override
    public long getLength(FailoverFileReplication ffr, String filename) throws IOException {
        return getFile(ffr, filename).length();
    }

    @Override
    public String readLink(FailoverFileReplication ffr, String filename) throws IOException {
        throw new IOException("readLink not supported");
    }

    @Override
    public long getDeviceIdentifier(FailoverFileReplication ffr, String filename) throws IOException {
        throw new IOException("getDeviceIdentifier not supported");
    }

    @Override
    public InputStream getInputStream(FailoverFileReplication ffr, String filename) throws IOException {
        return new FileInputStream(getFile(ffr, filename));
    }

    @Override
    public String getNameOfFile(FailoverFileReplication ffr, String filename) {
        return getFile(ffr, filename).getName();
    }

    @Override
    public int getFailoverBatchSize(FailoverFileReplication ffr) throws IOException, SQLException {
        return 1000;
    }

    @Override
    public void preBackup(FailoverFileReplication ffr) throws IOException, SQLException {
    }

    @Override
    public void init(FailoverFileReplication ffr) throws IOException, SQLException {
    }

    @Override
    public void cleanup(FailoverFileReplication ffr) throws IOException, SQLException {
        synchronized(fileCacheLock) {
            lastFilenames.remove(ffr);
            lastFiles.remove(ffr);
        }
    }

    @Override
    public void postBackup(FailoverFileReplication ffr) throws IOException, SQLException {
    }

    @Override
    public Iterator<String> getFilenameIterator(FailoverFileReplication ffr) throws IOException, SQLException {
        // Build the skip list
        Map<String,FilesystemIteratorRule> filesystemRules = getFilesystemIteratorRules(ffr);
        Map<String,FilesystemIteratorRule> filesystemPrefixRules = getFilesystemIteratorPrefixRules(ffr);

        for(FileBackupSetting setting : ffr.getFileBackupSettings()) {
            filesystemRules.put(
                setting.getPath(),
                setting.getBackupEnabled() ? FilesystemIteratorRule.OK : FilesystemIteratorRule.SKIP
            );
        }

        return new FilesystemIterator(filesystemRules, filesystemPrefixRules).getFilenameIterator();
    }

    /**
     * Gets the default set of filesystem rules for this environment.
     * This should not include file backup settings, they will override the
     * values returned here.  The returned map may be modified, to maintain
     * internal consistency, return a copy of the map if needed.
     */
    protected abstract Map<String,FilesystemIteratorRule> getFilesystemIteratorRules(FailoverFileReplication ffr) throws IOException, SQLException;

    /**
     * Gets the default set of filesystem prefix rules for this environment.
     */
    protected abstract Map<String,FilesystemIteratorRule> getFilesystemIteratorPrefixRules(FailoverFileReplication ffr) throws IOException, SQLException;

    @Override
    public String getDefaultSourceIPAddress() throws IOException, SQLException {
        return IPAddress.WILDCARD_IP;
    }

    @Override
    public List<String> getReplicatedMySQLServers(FailoverFileReplication ffr) throws IOException, SQLException {
        return Collections.emptyList();
    }

    @Override
    public List<String> getReplicatedMySQLMinorVersions(FailoverFileReplication ffr) throws IOException, SQLException {
        return Collections.emptyList();
    }
    
    /**
     * Uses the random source from AOServClient
     */
    @Override
    public Random getRandom() throws IOException, SQLException {
        return getConnector().getRandom();
    }
    
    @Override
    public String getServerPath(FailoverFileReplication ffr, String filename) {
        String serverPath;
        if(filename.length()==0) throw new AssertionError("Empty filename not expected");
        else if(filename.charAt(0)!=File.separatorChar) serverPath = '/' + filename.replace(File.separatorChar, '/');
        else serverPath = filename.replace(File.separatorChar, '/');
        return serverPath;
    }
}
