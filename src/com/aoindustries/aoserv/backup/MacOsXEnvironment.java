package com.aoindustries.aoserv.backup;

/*
 * Copyright 2008-2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.FailoverFileReplication;
import com.aoindustries.io.FilesystemIteratorRule;
import java.util.HashMap;
import java.util.Map;

/**
 * @author  AO Industries, Inc.
 */
abstract public class MacOsXEnvironment extends FileEnvironment {

    @Override
    protected Map<String,FilesystemIteratorRule> getFilesystemIteratorRules(FailoverFileReplication ffr) {
        Map<String,FilesystemIteratorRule> filesystemRules=new HashMap<String,FilesystemIteratorRule>();
        return filesystemRules;
    }

    @Override
    protected Map<String,FilesystemIteratorRule> getFilesystemIteratorPrefixRules(FailoverFileReplication ffr) {
        Map<String,FilesystemIteratorRule> filesystemPrefixRules = new HashMap<String, FilesystemIteratorRule>();
        return filesystemPrefixRules;
    }
}
