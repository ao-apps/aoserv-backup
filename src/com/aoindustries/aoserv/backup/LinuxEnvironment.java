package com.aoindustries.aoserv.backup;

/*
 * Copyright 2008-2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.FailoverFileReplication;
import com.aoindustries.io.FilesystemIteratorRule;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * A <code>LinuxEnvironment</code> extends <code>UnixFileEnvironment</code> to
 * have some default exclusions, such as <code>/proc/</code>.
 *
 * @author  AO Industries, Inc.
 */
abstract public class LinuxEnvironment extends UnixFileEnvironment {
    
    @Override
    protected Map<String,FilesystemIteratorRule> getFilesystemIteratorRules(FailoverFileReplication ffr) throws IOException, SQLException {
        Map<String,FilesystemIteratorRule> filesystemRules=new HashMap<String,FilesystemIteratorRule>();
        filesystemRules.put("/dev/log", FilesystemIteratorRule.SKIP);
        filesystemRules.put("/dev/pts/", FilesystemIteratorRule.SKIP);
        filesystemRules.put("/dev/shm/", FilesystemIteratorRule.SKIP);
        filesystemRules.put("/proc/", FilesystemIteratorRule.SKIP);
        filesystemRules.put("/selinux/", FilesystemIteratorRule.SKIP);
        filesystemRules.put("/sys/", FilesystemIteratorRule.SKIP);
        return filesystemRules;
    }

    @Override
    protected Map<String,FilesystemIteratorRule> getFilesystemIteratorPrefixRules(FailoverFileReplication ffr) throws IOException, SQLException {
        Map<String,FilesystemIteratorRule> filesystemPrefixRules = new HashMap<String, FilesystemIteratorRule>();
        return filesystemPrefixRules;
    }
}
