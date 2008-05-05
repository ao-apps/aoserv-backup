package com.aoindustries.aoserv.backup;

/*
 * Copyright 2008 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.FailoverFileReplication;
import com.aoindustries.io.FilesystemIteratorRule;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author  AO Industries, Inc.
 */
abstract public class WindowsEnvironment extends FileEnvironment {

    @Override
    protected Map<String,FilesystemIteratorRule> getFilesystemIteratorRules(FailoverFileReplication ffr) throws IOException, SQLException {
        Map<String,FilesystemIteratorRule> filesystemRules=new HashMap<String,FilesystemIteratorRule>();
        return filesystemRules;
    }

    @Override
    protected Map<String,FilesystemIteratorRule> getFilesystemIteratorPrefixRules(FailoverFileReplication ffr) throws IOException, SQLException {
        Map<String,FilesystemIteratorRule> filesystemPrefixRules = new HashMap<String, FilesystemIteratorRule>();
        return filesystemPrefixRules;
    }
}
