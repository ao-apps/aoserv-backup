package com.aoindustries.aoserv.backup;

/*
 * Copyright 2003-2006 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.profiler.*;
import java.io.*;
import java.util.*;

/**
 * The configuration for the standalone <code>BackupDaemon</code>.  When running as part of the <code>AOServDaemon</code>,
 * these values are not used.
 *
 * @author  AO Industries, Inc.
 */
final public class BackupDaemonConfiguration {

    private static Properties props;

    public static String getProperty(String name) throws IOException {
        Profiler.startProfile(Profiler.IO, BackupDaemonConfiguration.class, "getProperty(String)", null);
        try {
	    synchronized(BackupDaemonConfiguration.class) {
		if(props==null) {
		    Properties newProps = new Properties();
		    InputStream in = new BufferedInputStream(BackupDaemonConfiguration.class.getResourceAsStream("aoserv-backup.properties"));
		    try {
			newProps.load(in);
		    } finally {
			in.close();
		    }
		    props=newProps;
		}
		return props.getProperty(name);
	    }
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }
}
