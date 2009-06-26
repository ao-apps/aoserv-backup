package com.aoindustries.aoserv.backup;

/*
 * Copyright 2001-2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOServClientConfiguration;
import com.aoindustries.aoserv.client.AOServConnector;
import com.aoindustries.aoserv.client.AOServer;
import com.aoindustries.aoserv.client.FailoverFileLog;
import com.aoindustries.aoserv.client.FailoverFileReplication;
import com.aoindustries.aoserv.client.FailoverFileSchedule;
import com.aoindustries.aoserv.client.NetBind;
import com.aoindustries.aoserv.client.NetPort;
import com.aoindustries.aoserv.client.Protocol;
import com.aoindustries.aoserv.client.SSLConnector;
import com.aoindustries.aoserv.client.Server;
import com.aoindustries.aoserv.daemon.client.AOServDaemonConnection;
import com.aoindustries.aoserv.daemon.client.AOServDaemonConnector;
import com.aoindustries.aoserv.daemon.client.AOServDaemonProtocol;
import com.aoindustries.io.AOPool;
import com.aoindustries.io.BitRateOutputStream;
import com.aoindustries.io.BitRateProvider;
import com.aoindustries.io.ByteCountInputStream;
import com.aoindustries.io.ByteCountOutputStream;
import com.aoindustries.io.CompressedDataInputStream;
import com.aoindustries.io.CompressedDataOutputStream;
import com.aoindustries.io.TerminalWriter;
import com.aoindustries.io.unix.UnixFile;
import com.aoindustries.md5.MD5;
import com.aoindustries.sql.SQLUtility;
import com.aoindustries.table.Table;
import com.aoindustries.table.TableListener;
import com.aoindustries.util.BufferManager;
import com.aoindustries.util.ErrorHandler;
import com.aoindustries.util.ErrorPrinter;
//import com.aoindustries.util.zip.AutoFinishGZIPOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * The <code>FailoverFileReplicationDaemon</code> runs on every server that is backed-up.
 *
 * @author  AO Industries, Inc.
 */
final public class BackupDaemon {

    final private BackupEnvironment environment;

    private boolean isStarted = false;
    final private Map<FailoverFileReplication,BackupDaemonThread> threads = new HashMap<FailoverFileReplication,BackupDaemonThread>();

    public BackupDaemon(BackupEnvironment environment) {
        this.environment=environment;
    }

    final private TableListener tableListener = new TableListener() {
        @Override
        public void tableUpdated(Table table) {
            try {
                verifyThreads();
            } catch(IOException err) {
                environment.error(getClass(), "tableUpdated", null, err);
            } catch(SQLException err) {
                environment.error(getClass(), "tableUpdated", null, err);
            }
        }
    };

    /**
     * Starts the backup daemon (as one thread per FailoverFileReplication.
     */
    synchronized public void start() throws IOException, SQLException {
        if(!isStarted) {
            AOServConnector conn = environment.getConnector();
            conn.getFailoverFileReplications().addTableListener(tableListener);
            isStarted = true;
            new Thread(
                new Runnable() {
                    public void run() {
                        while(true) {
                            try {
                                verifyThreads();
                                break;
                            } catch(RuntimeException err) {
                                environment.error(getClass(), "run", null, err);
                                try {
                                    Thread.sleep(60000);
                                } catch(InterruptedException err2) {
                                    environment.warn(getClass(), "run", null, err2);
                                }
                            } catch(IOException err) {
                                environment.error(getClass(), "run", null, err);
                                try {
                                    Thread.sleep(60000);
                                } catch(InterruptedException err2) {
                                    environment.warn(getClass(), "run", null, err2);
                                }
                            } catch(SQLException err) {
                                environment.error(getClass(), "run", null, err);
                                try {
                                    Thread.sleep(60000);
                                } catch(InterruptedException err2) {
                                    environment.warn(getClass(), "run", null, err2);
                                }
                            }
                        }
                    }
                }
            ).start();
        }
    }

    synchronized private void verifyThreads() throws IOException, SQLException {
        // Ignore events coming in after shutdown
        if(isStarted) {
            Server thisServer = environment.getThisServer();
            //AOServConnector conn = environment.getConnector();
            List<FailoverFileReplication> removedList = new ArrayList<FailoverFileReplication>(threads.keySet());
            for(FailoverFileReplication ffr : thisServer.getFailoverFileReplications()) {
                removedList.remove(ffr);
                if(!threads.containsKey(ffr)) {
                    if(environment.isDebugEnabled()) environment.debug(getClass(), "verifyThreads", "Starting BackupDaemonThread for "+ffr, null);
                    BackupDaemonThread thread = new BackupDaemonThread(environment, ffr);
                    threads.put(ffr, thread);
                    thread.start();
                }
            }
            for(FailoverFileReplication ffr : removedList) {
                BackupDaemonThread thread = threads.get(ffr);
                if(environment.isDebugEnabled()) environment.debug(getClass(), "verifyThreads", "Stopping BackupDaemonThread for "+ffr, null);
                thread.stop();
            }
            for(FailoverFileReplication ffr : removedList) {
                BackupDaemonThread thread = threads.remove(ffr);
                if(environment.isDebugEnabled()) environment.debug(getClass(), "verifyThreads", "Joining BackupDaemonThread for "+ffr, null);
                try {
                    thread.join();
                } catch(InterruptedException err) {
                    environment.warn(getClass(), "verifyThreads", null, err);
                }
            }
        }
    }

    /**
     * Stops the backup daemon and any currently running backups.
     */
    synchronized public void stop() throws IOException, SQLException {
        if(isStarted) {
            AOServConnector conn = environment.getConnector();
            conn.getFailoverFileReplications().removeTableListener(tableListener);
            isStarted = false;
            // Stop each thread
            for(Map.Entry<FailoverFileReplication,BackupDaemonThread> entry : threads.entrySet()) {
                if(environment.isDebugEnabled()) environment.debug(getClass(), "stop", "Stopping BackupDaemonThread for "+entry.getKey(), null);
                entry.getValue().stop();
            }
            // Join each thread (wait for actual stop)
            for(Map.Entry<FailoverFileReplication,BackupDaemonThread> entry : threads.entrySet()) {
                if(environment.isDebugEnabled()) environment.debug(getClass(), "stop", "Joining BackupDaemonThread for "+entry.getKey(), null);
                try {
                    entry.getValue().join();
                } catch(InterruptedException err) {
                    if(environment.isWarnEnabled()) environment.warn(getClass(), "stop", null, err);
                }
            }
            threads.clear();
        }
    }

    synchronized public void runNow(FailoverFileReplication ffr) {
        BackupDaemonThread thread = threads.get(ffr);
        if(thread==null) thread.runNow();
    }

    private static class DynamicBitRateProvider implements BitRateProvider {
        
        final private BackupEnvironment environment;
        final private FailoverFileReplication originalFfr;
        
        private DynamicBitRateProvider(BackupEnvironment environment, FailoverFileReplication ffr) {
            this.environment = environment;
            this.originalFfr = ffr;
        }

        public int getBitRate() {
            try {
                // Try to get the latest version of originalFfr
                FailoverFileReplication newFfr = originalFfr.getTable().get(originalFfr.getKey());
                if(newFfr!=null) return newFfr.getBitRate();
            } catch(IOException err) {
                environment.error(DynamicBitRateProvider.class, "getBitRate", null, err);
            } catch(SQLException err) {
                environment.error(DynamicBitRateProvider.class, "getBitRate", null, err);
            } catch(RuntimeException err) {
                environment.error(DynamicBitRateProvider.class, "getBitRate", null, err);
            }
            return originalFfr.getBitRate();
        }

        public int getBlockSize() {
            return originalFfr.getBlockSize();
        }
    }

    private static class BackupDaemonThread implements Runnable {

        private static String convertExtraInfo(Object[] extraInfo) {
            if(extraInfo==null) return null;
            StringBuilder SB = new StringBuilder();
            for(Object o : extraInfo) SB.append(o).append('\n');
            return SB.toString();
        }

        /**
         * Gets the next filenames, up to batchSize.
         * @return the number of files in the array, zero (0) indicates iteration has completed
         */
        private static int getNextFilenames(Iterator<String> filenameIterator, String[] filenames, int batchSize) throws IOException {
            int c=0;
            while(c<batchSize) {
                if(!filenameIterator.hasNext()) break;
                String filename = filenameIterator.next();
                filenames[c++]=filename;
            }
            return c;
        }

        private final BackupEnvironment environment;
        private final FailoverFileReplication ffr;
        volatile private boolean runNow;
        private Thread thread;
        private Thread lastThread;

        final private ErrorHandler errorHandler = new ErrorHandler() {
            public void reportError(Throwable T, Object[] extraInfo) {
                if(environment.isErrorEnabled()) environment.error(getClass(), "reportError", convertExtraInfo(extraInfo), T);
            }

            public void reportWarning(Throwable T, Object[] extraInfo) {
                if(environment.isWarnEnabled()) environment.warn(getClass(), "reportWarning", convertExtraInfo(extraInfo), T);
            }
        };

        private BackupDaemonThread(BackupEnvironment environment, FailoverFileReplication ffr) {
            this.environment = environment;
            this.ffr = ffr;
        }
        
        synchronized private void start() {
            if(thread==null) {
                lastThread = null;
                (thread = new Thread(this)).start();
            }
        }
        
        synchronized private void stop() {
            Thread curThread = thread;
            if(curThread!=null) {
                lastThread = curThread;
                thread = null;
                curThread.interrupt();
            }
        }
        
        synchronized private void runNow() {
            Thread curThread = thread;
            if(curThread!=null) {
                runNow = true;
            }
        }

        private void join() throws InterruptedException {
            Thread localThread;
            synchronized(this) {
                localThread = this.lastThread;
            }
            if(localThread!=null) {
                localThread.join();
            }
        }

        /**
         * Each replication runs in its own thread.  Also, each replication may run concurrently with other replications.
         * However, each replication may not run concurrently with itself as this could cause problems on the server.
         */
        public void run() {
            final Thread currentThread = Thread.currentThread();
            while(true) {
                synchronized(this) {
                    if(currentThread!=thread) return;
                }
                try {
                    Random random = environment.getRandom();
                    short retention = ffr.getRetention().getDays();

                    // Get the last start time and success flag from the database (will be cached locally unless an error occurs
                    long lastStartTime = -1;
                    boolean lastPassSuccessful = false;
                    List<FailoverFileLog> ffls = ffr.getFailoverFileLogs(1);
                    if(!ffls.isEmpty()) {
                        FailoverFileLog lastLog = ffls.get(0);
                        if(environment.isDebugEnabled()) environment.debug(getClass(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "lastLog="+lastLog, null);
                        lastStartTime = lastLog.getStartTime();
                        if(environment.isDebugEnabled()) environment.debug(getClass(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "lastStartTime="+SQLUtility.getDateTime(lastStartTime), null);
                        lastPassSuccessful = lastLog.isSuccessful();
                        if(environment.isDebugEnabled()) environment.debug(getClass(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "lastPassSuccessful="+lastPassSuccessful, null);
                    }
                    // Single calendar instance is used
                    Calendar cal = Calendar.getInstance();
                    long lastCheckTime = -1;
                    int lastCheckHour = -1; // The last hour that the schedule was checked
                    int lastCheckMinute = -1; // The last minute that was checked
                    while(true) {
                        synchronized(this) {
                            if(currentThread!=thread) return;
                        }
                        // Sleep then look for the next (possibly missed) schedule
                        if(!runNow) {
                            try {
                                // Sleep some before checking again, this is randomized so schedules don't start exactly as scheduled
                                // But they should start within 5 minutes of the schedule.  This is because many people
                                // may schedule for certain times (like 6:00 am exactly)
                                //long sleepyTime = 60*1000 + random.nextInt(4*60*1000);
                                long sleepyTime = 55*1000;
                                if(environment.isDebugEnabled()) environment.debug(getClass(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "Sleeping for "+sleepyTime+" milliseconds before checking if backup pass needed.", null);
                                Thread.sleep(sleepyTime);
                            } catch(InterruptedException err) {
                                // May be interrupted by stop call
                            }
                        }
                        synchronized(this) {
                            if(currentThread!=thread) return;
                        }

                        // Get the latest ffr object (if cache was invalidated) to adhere to changes in enabled flag
                        FailoverFileReplication newFFR = environment.getConnector().getFailoverFileReplications().get(ffr.getPkey());
                        synchronized(this) {
                            if(currentThread!=thread) return;
                        }

                        if(newFFR==null) {
                            // Don't try to run removed ffr
                            if(environment.isDebugEnabled()) environment.debug(getClass(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "Replication removed", null);
                        } else if(!newFFR.getEnabled()) {
                            // Don't try to run disabled ffr
                            if(environment.isDebugEnabled()) environment.debug(getClass(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "Replication not enabled", null);
                        } else {
                            long currentTime = System.currentTimeMillis();
                            cal.setTimeInMillis(currentTime);
                            int currentHour = cal.get(Calendar.HOUR_OF_DAY);
                            int currentMinute = cal.get(Calendar.MINUTE);

                            if(environment.isDebugEnabled()) environment.debug(getClass(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "newFFR="+newFFR, null);
                            Server thisServer=environment.getThisServer();
                            if(environment.isDebugEnabled()) environment.debug(getClass(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "thisServer="+thisServer, null);
                            AOServer thisAOServer = thisServer.getAOServer();
                            AOServer failoverServer = thisAOServer==null ? null : thisAOServer.getFailoverServer();
                            if(environment.isDebugEnabled()) environment.debug(getClass(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "failoverServer="+failoverServer, null);
                            AOServer toServer=newFFR.getBackupPartition().getAOServer();
                            if(environment.isDebugEnabled()) environment.debug(getClass(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "toServer="+toServer, null);
                            synchronized(this) {
                                if(currentThread!=thread) return;
                            }

                            // Should it run now?
                            boolean shouldRun;
                            if(
                                // Will not replicate if the to server is our parent server in failover mode
                                toServer.equals(failoverServer)
                            ) {
                                shouldRun = false;
                                if(environment.isDebugEnabled()) environment.debug(getClass(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "Refusing to replication to our failover parent: "+failoverServer, null);
                            } else if(runNow) {
                                shouldRun = true;
                                runNow = false;
                                if(environment.isDebugEnabled()) environment.debug(getClass(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "runNow causing immediate run of backup", null);
                            } else if(
                                // Never ran before
                                lastStartTime==-1
                            ) {
                                shouldRun = true;
                                if(environment.isDebugEnabled()) environment.debug(getClass(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "Never ran this mirror", null);
                            } else if(
                                // If the last attempt failed, run now
                                !lastPassSuccessful
                            ) {
                                shouldRun = true;
                                if(environment.isDebugEnabled()) environment.debug(getClass(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "The last attempt at this mirror failed",null);
                            } else if(
                                // Last pass in the future (time reset)
                                lastStartTime > currentTime
                            ) {
                                shouldRun = false;
                                if(environment.isDebugEnabled()) environment.debug(getClass(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "Last pass in the future (time reset)", null);
                            } else if(
                                // Last pass more than 24 hours ago (this handles replications without schedules)
                                (currentTime - lastStartTime)>=(24*60*60*1000)
                            ) {
                                shouldRun = true;
                                if(environment.isDebugEnabled()) environment.debug(getClass(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "Last pass more than 24 hours ago", null);
                            } else if(
                                // The system time was set back
                                lastCheckTime!=-1 && lastCheckTime>currentTime
                            ) {
                                shouldRun = false;
                                if(environment.isDebugEnabled()) environment.debug(getClass(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "Last check time in the future (time reset)", null);
                            } else {
                                // If there is currently no schedule, this will not flag shouldRun and the check for 24-hours passed (above) will force the backup
                                List<FailoverFileSchedule> schedules = newFFR.getFailoverFileSchedules();
                                synchronized(this) {
                                    if(currentThread!=thread) return;
                                }
                                shouldRun = false;
                                for(FailoverFileSchedule schedule : schedules) {
                                    if(schedule.isEnabled()) {
                                        int scheduleHour = schedule.getHour();
                                        int scheduleMinute = schedule.getMinute();
                                        if(
                                            // Look for exact time match
                                            currentHour==scheduleHour
                                            && currentMinute==scheduleMinute
                                        ) {
                                            shouldRun = true;
                                            if(environment.isDebugEnabled()) environment.debug(getClass(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "It is the scheduled time: scheduleHour="+scheduleHour+" and scheduleMinute="+scheduleMinute, null);
                                            break;
                                        }
                                    //} else {
                                    //    if(environment.isDebugEnabled()) environment.debug((retention!=1 ? "Backup: " : "Failover: ") + "Skipping disabled schedule time: scheduleHour="+scheduleHour+" and scheduleMinute="+scheduleMinute);
                                    }
                                }
                                if(
                                    !shouldRun // If exact schedule already found, don't need to check here
                                    && lastCheckTime!=-1 // Don't look for missed schedule if last check time not set
                                    && (
                                        // Don't double-check the same minute (in case sleeps inaccurate or time reset)
                                        lastCheckHour!=currentHour
                                        || lastCheckMinute!=currentMinute
                                    )
                                ) {
                                    CHECK_LOOP:
                                    while(true) {
                                        // Increment minute first
                                        lastCheckMinute++;
                                        if(lastCheckMinute>=60) {
                                            lastCheckMinute=0;
                                            lastCheckHour++;
                                            if(lastCheckHour>=24) lastCheckHour=0;
                                        }
                                        // Current minute already checked above, terminate loop
                                        if(lastCheckHour==currentHour && lastCheckMinute==currentMinute) break CHECK_LOOP;
                                        // Look for a missed schedule
                                        for(FailoverFileSchedule schedule : schedules) {
                                            if(schedule.isEnabled()) {
                                                int scheduleHour = schedule.getHour();
                                                int scheduleMinute = schedule.getMinute();
                                                if(lastCheckHour==scheduleHour && lastCheckMinute==scheduleMinute) {
                                                    shouldRun = true;
                                                    if(environment.isDebugEnabled()) environment.debug(getClass(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "Missed a scheduled time: scheduleHour="+scheduleHour+" and scheduleMinute="+scheduleMinute, null);                                                    break CHECK_LOOP;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            lastCheckTime = currentTime;
                            lastCheckHour = currentHour;
                            lastCheckMinute = currentMinute;
                            if(shouldRun) {
                                synchronized(this) {
                                    if(currentThread!=thread) return;
                                }
                                try {
                                    lastStartTime = currentTime;
                                    lastPassSuccessful = false;
                                    try {
                                        backupPass(newFFR);
                                    } finally {
                                        runNow = false;
                                    }
                                    lastPassSuccessful = true;
                                } catch(RuntimeException T) {
                                    if(environment.isErrorEnabled()) environment.error(getClass(), "run", null, T);
                                    synchronized(this) {
                                        if(currentThread!=thread) return;
                                    }
                                    //Randomized sleep interval to reduce master load on startup (5-15 minutes)
                                    int sleepyTime = 5*60*1000 + random.nextInt(10*60*1000);
                                    if(environment.isDebugEnabled()) environment.debug(getClass(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "Sleeping for "+sleepyTime+" milliseconds after an error", null);
                                    try {
                                        Thread.sleep(sleepyTime); 
                                    } catch(InterruptedException err) {
                                        // May be interrupted by stop call
                                    }
                                } catch(IOException T) {
                                    if(environment.isErrorEnabled()) environment.error(getClass(), "run", null, T);
                                    synchronized(this) {
                                        if(currentThread!=thread) return;
                                    }
                                    //Randomized sleep interval to reduce master load on startup (5-15 minutes)
                                    int sleepyTime = 5*60*1000 + random.nextInt(10*60*1000);
                                    if(environment.isDebugEnabled()) environment.debug(getClass(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "Sleeping for "+sleepyTime+" milliseconds after an error", null);
                                    try {
                                        Thread.sleep(sleepyTime); 
                                    } catch(InterruptedException err) {
                                        // May be interrupted by stop call
                                    }
                                } catch(SQLException T) {
                                    if(environment.isErrorEnabled()) environment.error(getClass(), "run", null, T);
                                    synchronized(this) {
                                        if(currentThread!=thread) return;
                                    }
                                    //Randomized sleep interval to reduce master load on startup (5-15 minutes)
                                    int sleepyTime = 5*60*1000 + random.nextInt(10*60*1000);
                                    if(environment.isDebugEnabled()) environment.debug(getClass(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "Sleeping for "+sleepyTime+" milliseconds after an error", null);
                                    try {
                                        Thread.sleep(sleepyTime); 
                                    } catch(InterruptedException err) {
                                        // May be interrupted by stop call
                                    }
                                }
                            }
                        }
                    }
                } catch(ThreadDeath TD) {
                    throw TD;
                } catch(Throwable T) {
                    if(environment.isErrorEnabled()) environment.error(getClass(), "run", null, T);
                    synchronized(this) {
                        if(currentThread!=thread) return;
                    }
                    //Randomized sleep interval to reduce master load on startup (5-15 minutes)
                    int sleepyTime = 5*60*1000 + (int)(Math.random()*(10*60*1000));
                    if(environment.isDebugEnabled()) environment.debug(getClass(), "run", "Sleeping for "+sleepyTime+" milliseconds after an error", null);
                    try {
                        Thread.sleep(sleepyTime);
                    } catch(InterruptedException err) {
                        // May be interrupted by stop call
                    }
                }
            }
        }

        private void backupPass(FailoverFileReplication ffr) throws IOException, SQLException {
            final Thread currentThread = Thread.currentThread();

            environment.preBackup(ffr);
            synchronized(this) {
                if(currentThread!=thread) return;
            }
            environment.init(ffr);
            try {
                synchronized(this) {
                    if(currentThread!=thread) return;
                }
                final Server thisServer = environment.getThisServer();
                final int failoverBatchSize = environment.getFailoverBatchSize(ffr);
                final AOServer toServer=ffr.getBackupPartition().getAOServer();
                final boolean useCompression = ffr.getUseCompression();
                final short retention = ffr.getRetention().getDays();
                synchronized(this) {
                    if(currentThread!=thread) return;
                }

                if(environment.isDebugEnabled()) environment.debug(getClass(), "backupPass", (retention>1 ? "Backup: " : "Failover: ") + "Running failover from "+thisServer+" to "+toServer, null);

                Calendar cal = Calendar.getInstance();
                final long startTime=cal.getTimeInMillis();

                if(environment.isDebugEnabled()) environment.debug(getClass(), "backupPass", (retention>1 ? "Backup: " : "Failover: ") + "useCompression="+useCompression, null);
                if(environment.isDebugEnabled()) environment.debug(getClass(), "backupPass", (retention>1 ? "Backup: " : "Failover: ") + "retention="+retention, null);

                // Keep statistics during the replication
                int scanned=0;
                int updated=0;
                long rawBytesOut=0;
                long rawBytesIn=0;
                boolean isSuccessful=false;
                try {
                    // Get the connection to the daemon
                    long key=toServer.requestDaemonAccess(AOServDaemonProtocol.FAILOVER_FILE_REPLICATION, ffr.getPkey());
                    // Allow the failover connect address to be overridden
                    String ffrConnectAddress = ffr.getConnectAddress();
                    // Allow the address to be overridden
                    String daemonConnectAddress=toServer.getDaemonConnectAddress();
                    String connectAddress = ffrConnectAddress!=null ? ffrConnectAddress : daemonConnectAddress!=null ? daemonConnectAddress : toServer.getDaemonIPAddress().getIPAddress();
                    NetBind daemonBind = toServer.getDaemonConnectBind();
                    NetPort daemonBindPort = daemonBind.getPort();
                    Protocol daemonBindProtocol = daemonBind.getAppProtocol();

                    // First, the specific source address from ffr is used
                    String sourceIPAddress = ffr.getConnectFrom();
                    if(sourceIPAddress==null) {
                        sourceIPAddress = environment.getDefaultSourceIPAddress();
                    }
                    synchronized(this) {
                        if(currentThread!=thread) return;
                    }

                    AOServDaemonConnector daemonConnector=AOServDaemonConnector.getConnector(
                        toServer.getServer().getPkey(),
                        connectAddress,
                        sourceIPAddress,
                        daemonBindPort.getPort(),
                        daemonBindProtocol.getProtocol(),
                        null,
                        toServer.getPoolSize(),
                        AOPool.DEFAULT_MAX_CONNECTION_AGE,
                        SSLConnector.class,
                        SSLConnector.sslProviderLoaded,
                        AOServClientConfiguration.getSslTruststorePath(),
                        AOServClientConfiguration.getSslTruststorePassword(),
                        errorHandler
                    );
                    AOServDaemonConnection daemonConn=daemonConnector.getConnection();
                    try {
                        synchronized(this) {
                            if(currentThread!=thread) return;
                        }
                        // Start the replication
                        CompressedDataOutputStream rawOut=daemonConn.getOutputStream();

                        MD5 md5 = useCompression ? new MD5() : null;

                        rawOut.writeCompressedInt(AOServDaemonProtocol.FAILOVER_FILE_REPLICATION);
                        rawOut.writeLong(key);
                        rawOut.writeBoolean(useCompression);
                        rawOut.writeShort(retention);

                        // Determine the date on the from server
                        final int year = cal.get(Calendar.YEAR);
                        final int month = cal.get(Calendar.MONTH)+1;
                        final int day = cal.get(Calendar.DAY_OF_MONTH);
                        rawOut.writeShort(year);
                        rawOut.writeShort(month);
                        rawOut.writeShort(day);
                        if(retention==1) {
                            List<String> replicatedMySQLServers = environment.getReplicatedMySQLServers(ffr);
                            List<String> replicatedMySQLMinorVersions = environment.getReplicatedMySQLMinorVersions(ffr);
                            int len = replicatedMySQLServers.size();
                            rawOut.writeCompressedInt(len);
                            for(int c=0;c<len;c++) {
                                rawOut.writeUTF(replicatedMySQLServers.get(c));
                                rawOut.writeUTF(replicatedMySQLMinorVersions.get(c));
                            }
                        }
                        rawOut.flush();
                        synchronized(this) {
                            if(currentThread!=thread) return;
                        }

                        CompressedDataInputStream rawIn=daemonConn.getInputStream();
                        int result=rawIn.read();
                        synchronized(this) {
                            if(currentThread!=thread) return;
                        }
                        if(result==AOServDaemonProtocol.NEXT) {
                            // Only the output is limited because input should always be smaller than the output
                            ByteCountOutputStream rawBytesOutStream = new ByteCountOutputStream(new BitRateOutputStream(rawOut, new DynamicBitRateProvider(environment, ffr)));
                            CompressedDataOutputStream out = new CompressedDataOutputStream(
                                /*useCompression
                                ? new AutoFinishGZIPOutputStream(new DontCloseOutputStream(rawBytesOutStream), BufferManager.BUFFER_SIZE)
                                :*/ rawBytesOutStream
                            );

                            ByteCountInputStream rawBytesInStream = new ByteCountInputStream(rawIn);
                            CompressedDataInputStream in = new CompressedDataInputStream(rawBytesInStream);
                            try {
                                // Do requests in batches
                                String[] filenames = new String[failoverBatchSize];
                                int[] results = new int[failoverBatchSize];
                                long[][] md5His = useCompression ? new long[failoverBatchSize][] : null;
                                long[][] md5Los = useCompression ? new long[failoverBatchSize][] : null;
                                byte[] buff=BufferManager.getBytes();
                                byte[] chunkBuffer = new byte[AOServDaemonProtocol.FAILOVER_FILE_REPLICATION_CHUNK_SIZE];
                                try {
                                    final Iterator<String> filenameIterator = environment.getFilenameIterator(ffr);
                                    while(true) {
                                        synchronized(this) {
                                            if(currentThread!=thread) return;
                                        }
                                        int batchSize=getNextFilenames(filenameIterator, filenames, failoverBatchSize);
                                        if(batchSize==0) break;

                                        out.writeCompressedInt(batchSize);
                                        for(int d=0;d<batchSize;d++) {
                                            scanned++;
                                            String filename = filenames[d];
                                            try {
                                                long mode=environment.getStatMode(ffr, filename);
                                                if(!UnixFile.isSocket(mode)) {
                                                    // Get all the values first to avoid FileNotFoundException in middle of protocol
                                                    boolean isRegularFile = UnixFile.isRegularFile(mode);
                                                    long size = isRegularFile?environment.getLength(ffr, filename):-1;
                                                    int uid = environment.getUID(ffr, filename);
                                                    int gid = environment.getGID(ffr, filename);
                                                    boolean isSymLink = UnixFile.isSymLink(mode);
                                                    long modifyTime = isSymLink?-1:environment.getModifyTime(ffr, filename);
                                                    //if(modifyTime<1000 && !isSymLink && log.isWarnEnabled()) log.warn("Non-symlink modifyTime<1000: "+filename+": "+modifyTime);
                                                    String symLinkTarget;
                                                    if(isSymLink) {
                                                        try {
                                                            symLinkTarget = environment.readLink(ffr, filename);
                                                        } catch(SecurityException err) {
                                                            if(environment.isErrorEnabled()) environment.error(getClass(), "backupPass", "SecurityException trying to readlink: "+filename, err);
                                                            throw err;
                                                        } catch(IOException err) {
                                                            if(environment.isErrorEnabled()) environment.error(getClass(), "backupPass", "IOException trying to readlink: "+filename, err);
                                                            throw err;
                                                        }
                                                    } else {
                                                        symLinkTarget = null;
                                                    }
                                                    boolean isDevice = UnixFile.isBlockDevice(mode) || UnixFile.isCharacterDevice(mode);
                                                    long deviceID = isDevice?environment.getDeviceIdentifier(ffr, filename):-1;

                                                    out.writeBoolean(true);
                                                    // Adjust the filename to server formatting
                                                    String serverPath = environment.getServerPath(ffr, filename);
                                                    out.writeCompressedUTF(serverPath, 0);
                                                    out.writeLong(mode);
                                                    if(UnixFile.isRegularFile(mode)) out.writeLong(size);
                                                    if(uid<0 || uid>65535) {
                                                        if(environment.isWarnEnabled()) environment.warn(getClass(), "backupPass", null, new IOException("UID out of range, converted to 0, uid="+uid+" and path="+filename));
                                                        uid=0;
                                                    }
                                                    out.writeCompressedInt(uid);
                                                    if(gid<0 || gid>65535) {
                                                        if(environment.isWarnEnabled()) environment.warn(getClass(), "backupPass", null, new IOException("GID out of range, converted to 0, gid="+gid+" and path="+filename));
                                                        gid=0;
                                                    }
                                                    out.writeCompressedInt(gid);
                                                    if(!isSymLink) out.writeLong(modifyTime);
                                                    if(isSymLink) out.writeCompressedUTF(symLinkTarget, 1);
                                                    else if(isDevice) out.writeLong(deviceID);
                                                } else {
                                                    filenames[d]=null;
                                                    out.writeBoolean(false);
                                                }
                                            } catch(FileNotFoundException err) {
                                                // Normal because of a dynamic file system
                                                filenames[d]=null;
                                                out.writeBoolean(false);
                                            }
                                        }
                                        out.flush();
                                        // Recreate the compressed stream after flush because GZIPOutputStream is broken.
                                        /*if(useCompression) {
                                            out = new CompressedDataOutputStream(
                                                new AutoFinishGZIPOutputStream(new DontCloseOutputStream(rawBytesOutStream), BufferManager.BUFFER_SIZE)
                                            );
                                        }*/
                                        synchronized(this) {
                                            if(currentThread!=thread) return;
                                        }

                                        // Read the results
                                        result=in.read();
                                        synchronized(this) {
                                            if(currentThread!=thread) return;
                                        }
                                        boolean hasRequestData = false;
                                        if(result==AOServDaemonProtocol.NEXT) {
                                            for(int d=0;d<batchSize;d++) {
                                                if(filenames[d]!=null) {
                                                    result = in.read();
                                                    results[d]=result;
                                                    if(result==AOServDaemonProtocol.FAILOVER_FILE_REPLICATION_MODIFIED_REQUEST_DATA) {
                                                        hasRequestData = true;
                                                    } else if(result==AOServDaemonProtocol.FAILOVER_FILE_REPLICATION_MODIFIED_REQUEST_DATA_CHUNKED) {
                                                        hasRequestData = true;
                                                        int chunkCount = in.readCompressedInt();
                                                        long[] md5Hi = md5His[d] = new long[chunkCount];
                                                        long[] md5Lo = md5Los[d] = new long[chunkCount];
                                                        for(int e=0;e<chunkCount;e++) {
                                                            md5Hi[e]=in.readLong();
                                                            md5Lo[e]=in.readLong();
                                                        }
                                                    }
                                                }
                                            }
                                        } else {
                                            if (result == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
                                            else if (result == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
                                            else throw new IOException("Unknown result: " + result);
                                        }
                                        synchronized(this) {
                                            if(currentThread!=thread) return;
                                        }

                                        // Process the results
                                        //DeflaterOutputStream deflaterOut;
                                        DataOutputStream outgoing;

                                        if(hasRequestData) {
                                            //deflaterOut = null;
                                            outgoing = out;
                                        } else {
                                            //deflaterOut = null;
                                            outgoing = null;
                                        }
                                        for(int d=0;d<batchSize;d++) {
                                            synchronized(this) {
                                                if(currentThread!=thread) return;
                                            }
                                            String filename = filenames[d];
                                            if(filename!=null) {
                                                result=results[d];
                                                if(result==AOServDaemonProtocol.FAILOVER_FILE_REPLICATION_MODIFIED) {
                                                    if(environment.isDebugEnabled()) environment.debug(getClass(), "backupPass", (retention>1 ? "Backup: " : "Failover: ") + "File modified: "+filename, null);
                                                    updated++;
                                                } else if(result==AOServDaemonProtocol.FAILOVER_FILE_REPLICATION_MODIFIED_REQUEST_DATA) {
                                                    updated++;
                                                    try {
                                                        if(environment.isDebugEnabled()) environment.debug(getClass(), "backupPass", (retention>1 ? "Backup: " : "Failover: ") + "Sending file contents: "+filename, null);
                                                        // Shortcut for 0 length files (don't open for reading)
                                                        if(environment.getLength(ffr, filename)!=0) {
                                                            InputStream fileIn = environment.getInputStream(ffr, filename);
                                                            try {
                                                                int blockLen;
                                                                while((blockLen=fileIn.read(buff, 0, BufferManager.BUFFER_SIZE))!=-1) {
                                                                    synchronized(this) {
                                                                        if(currentThread!=thread) return;
                                                                    }
                                                                    if(blockLen>0) {
                                                                        outgoing.write(AOServDaemonProtocol.NEXT);
                                                                        outgoing.writeShort(blockLen);
                                                                        outgoing.write(buff, 0, blockLen);
                                                                    }
                                                                }
                                                            } finally {
                                                                fileIn.close();
                                                            }
                                                        }
                                                    } catch(FileNotFoundException err) {
                                                        // Normal when the file was deleted
                                                    } finally {
                                                        synchronized(this) {
                                                            if(currentThread!=thread) return;
                                                        }
                                                        outgoing.write(AOServDaemonProtocol.DONE);
                                                    }
                                                } else if(result==AOServDaemonProtocol.FAILOVER_FILE_REPLICATION_MODIFIED_REQUEST_DATA_CHUNKED) {
                                                    updated++;
                                                    try {
                                                        if(environment.isDebugEnabled()) environment.debug(getClass(), "backupPass", (retention>1 ? "Backup: " : "Failover: ") + "Chunking file contents: "+filename, null);
                                                        long[] md5Hi = md5His[d];
                                                        long[] md5Lo = md5Los[d];
                                                        InputStream fileIn=environment.getInputStream(ffr, filename);
                                                        try {
                                                            int chunkNumber = 0;
                                                            int sendChunkCount = 0;
                                                            while(true) {
                                                                synchronized(this) {
                                                                    if(currentThread!=thread) return;
                                                                }
                                                                // Read fully one chunk or to end of file
                                                                int pos=0;
                                                                while(pos<AOServDaemonProtocol.FAILOVER_FILE_REPLICATION_CHUNK_SIZE) {
                                                                    int ret = fileIn.read(chunkBuffer, pos, AOServDaemonProtocol.FAILOVER_FILE_REPLICATION_CHUNK_SIZE-pos);
                                                                    if(ret==-1) break;
                                                                    pos+=ret;
                                                                }
                                                                if(pos>0) {
                                                                    boolean sendData = true;
                                                                    if(pos==AOServDaemonProtocol.FAILOVER_FILE_REPLICATION_CHUNK_SIZE && chunkNumber < md5Hi.length) {
                                                                        // Calculate the MD5 hash
                                                                        md5.Init();
                                                                        md5.Update(chunkBuffer, 0, pos);
                                                                        byte[] md5Bytes = md5.Final();
                                                                        sendData = md5Hi[chunkNumber]!=MD5.getMD5Hi(md5Bytes) || md5Lo[chunkNumber]!=MD5.getMD5Lo(md5Bytes);
                                                                        if(sendData) sendChunkCount++;
                                                                    } else {
                                                                        // Either incomplete chunk or chunk past those sent by client
                                                                        sendData = true;
                                                                    }
                                                                    if(sendData) {
                                                                        outgoing.write(AOServDaemonProtocol.NEXT);
                                                                        outgoing.writeShort(pos);
                                                                        outgoing.write(chunkBuffer, 0, pos);
                                                                    } else {
                                                                        outgoing.write(AOServDaemonProtocol.NEXT_CHUNK);
                                                                    }
                                                                }
                                                                // At end of file when not complete chunk read
                                                                if(pos!=AOServDaemonProtocol.FAILOVER_FILE_REPLICATION_CHUNK_SIZE) break;

                                                                // Increment chunk number for next iteration
                                                                chunkNumber++;
                                                            }
                                                            if(environment.isDebugEnabled()) environment.debug(getClass(), "backupPass", (retention>1 ? "Backup: " : "Failover: ") + "Chunking file contents: "+filename+": Sent "+sendChunkCount+" out of "+chunkNumber+" chunks", null);
                                                        } finally {
                                                            fileIn.close();
                                                        }
                                                    } catch(FileNotFoundException err) {
                                                        // Normal when the file was deleted
                                                    } finally {
                                                        synchronized(this) {
                                                            if(currentThread!=thread) return;
                                                        }
                                                        outgoing.write(AOServDaemonProtocol.DONE);
                                                    }
                                                } else if(result!=AOServDaemonProtocol.FAILOVER_FILE_REPLICATION_NO_CHANGE) throw new IOException("Unknown result: "+result);
                                            }
                                        }

                                        // Flush any file data that was sent
                                        if(hasRequestData) {
                                            synchronized(this) {
                                                if(currentThread!=thread) return;
                                            }
                                            outgoing.flush();
                                        }
                                    }
                                } finally {
                                    BufferManager.release(buff);
                                }

                                // Tell the server we are finished
                                synchronized(this) {
                                    if(currentThread!=thread) return;
                                }
                                out.writeCompressedInt(-1);
                                out.flush();
                                // Recreate the compressed stream after flush because GZIPOutputStream is broken.
                                /*if(useCompression) {
                                    out = new CompressedDataOutputStream(
                                        new AutoFinishGZIPOutputStream(new DontCloseOutputStream(rawBytesOutStream), BufferManager.BUFFER_SIZE)
                                    );
                                }*/
                                synchronized(this) {
                                    if(currentThread!=thread) return;
                                }
                                result=in.read();
                                if(result!=AOServDaemonProtocol.DONE) {
                                    if (result == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
                                    else if (result == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
                                    else throw new IOException("Unknown result: " + result);
                                }
                            } finally {
                                // Store the bytes transferred
                                rawBytesOut=rawBytesOutStream.getCount();
                                rawBytesIn=rawBytesInStream.getCount();
                            }
                        } else {
                            if (result == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(rawIn.readUTF());
                            else if (result == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(rawIn.readUTF());
                            else throw new IOException("Unknown result: " + result);
                        }
                    } finally {
                        daemonConn.close(); // Always close to minimize connections on server and handle stop interruption causing interrupted protocol
                        daemonConnector.releaseConnection(daemonConn);
                    }
                    isSuccessful=true;
                } finally {
                    // Store the statistics
                    for(int c=0;c<10;c++) {
                        // Try in a loop with delay in case master happens to be restarting
                        try {
                            ffr.addFailoverFileLog(startTime, System.currentTimeMillis(), scanned, updated, rawBytesOut+rawBytesIn, isSuccessful);
                            break;
                        } catch(RuntimeException err) {
                            if(c>=10) {
                                environment.error(getClass(), "backupPass", "Error adding failover file log, giving up", err);
                            } else {
                                environment.error(getClass(), "backupPass", "Error adding failover file log, will retry in one minute", err);
                                try {
                                    Thread.sleep(60*1000);
                                } catch(InterruptedException err2) {
                                    environment.warn(getClass(), "backupPass", null, err2);
                                }
                            }
                        }
                    }
                }
            } finally {
                environment.cleanup(ffr);
            }
            synchronized(this) {
                if(currentThread!=thread) return;
            }
            environment.postBackup(ffr);
        }
    }

    /**
     * Runs the standalone <code>BackupDaemon</code> with the values
     * provided in <code>com/aoindustries/aoserv/backup/aoserv-backup.properties</code>.
     * This will typically be called by the init scripts of the dedicated machine.
     */
    public static void main(String[] args) {
        if(args.length!=1) {
            try {
                showUsage();
                System.exit(1);
            } catch(IOException err) {
                ErrorPrinter.printStackTraces(err);
                System.exit(5);
            }
            return;
        } else {
            // Load the environment class as provided on the command line
            BackupEnvironment environment;
            try {
                environment=(BackupEnvironment)Class.forName(args[0]).newInstance();
            } catch(ClassNotFoundException err) {
                ErrorPrinter.printStackTraces(err, new Object[] {"environment_classname="+args[0]});
                System.exit(2);
                return;
            } catch(InstantiationException err) {
                ErrorPrinter.printStackTraces(err, new Object[] {"environment_classname="+args[0]});
                System.exit(3);
                return;
            } catch(IllegalAccessException err) {
                ErrorPrinter.printStackTraces(err, new Object[] {"environment_classname="+args[0]});
                System.exit(4);
                return;
            }

            boolean done=false;
            while(!done) {
                try {
                    // Start up the daemon
                    BackupDaemon daemon = new BackupDaemon(environment);
                    daemon.start();
                    done=true;
                } catch (ThreadDeath TD) {
                    throw TD;
                } catch (Throwable T) {
                    environment.error(BackupDaemon.class, "main", null, T);
                    try {
                        Thread.sleep(60000);
                    } catch(InterruptedException err) {
                        environment.warn(BackupDaemon.class, "main", null, err);
                    }
                }
            }
        }
    }
    
    public static void showUsage() throws IOException {
        TerminalWriter out=new TerminalWriter(System.err);
        out.println();
        out.boldOn();
        out.print("SYNOPSIS");
        out.attributesOff();
        out.println();
        out.println("\t"+BackupDaemon.class.getName()+" {environment_classname}");
        out.println();
        out.boldOn();
        out.print("DESCRIPTION");
        out.attributesOff();
        out.println();
        out.println("\tLaunches the backup system daemon.  The process will continue indefinitely");
        out.println("\twhile backing-up files as needed.");
        out.println();
        out.println("\tAn environment_classname must be provided.  This must be the fully qualified");
        out.println("\tclass name of a "+BackupEnvironment.class.getName()+".  One instance");
        out.println("\tof this class will be created via the default constructor.");
        out.println();
        out.flush();
    }
}
