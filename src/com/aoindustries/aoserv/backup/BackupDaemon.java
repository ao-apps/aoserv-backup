/*
 * Copyright 2001-2013, 2015, 2016, 2017, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.backup;

import com.aoindustries.aoserv.client.AOServClientConfiguration;
import com.aoindustries.aoserv.client.AOServConnector;
import com.aoindustries.aoserv.client.backup.FileReplication;
import com.aoindustries.aoserv.client.backup.FileReplicationLog;
import com.aoindustries.aoserv.client.backup.FileReplicationSchedule;
import com.aoindustries.aoserv.client.linux.Server;
import com.aoindustries.aoserv.client.net.Host;
import com.aoindustries.aoserv.client.validator.MySQLServerName;
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
import com.aoindustries.math.SafeMath;
import com.aoindustries.md5.MD5;
import com.aoindustries.net.InetAddress;
import com.aoindustries.sql.SQLUtility;
import com.aoindustries.table.Table;
import com.aoindustries.table.TableListener;
import com.aoindustries.util.ErrorPrinter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The <code>FailoverFileReplicationDaemon</code> runs on every server that is backed-up.
 *
 * @author  AO Industries, Inc.
 */
final public class BackupDaemon {

	final private BackupEnvironment environment;

	private boolean isStarted = false;
	final private Map<FileReplication,BackupDaemonThread> threads = new HashMap<FileReplication,BackupDaemonThread>();

	public BackupDaemon(BackupEnvironment environment) {
		this.environment=environment;
	}

	final private TableListener tableListener = new TableListener() {
		@Override
		public void tableUpdated(Table<?> table) {
			try {
				verifyThreads();
			} catch(IOException err) {
				environment.getLogger().logp(Level.SEVERE, getClass().getName(), "tableUpdated", null, err);
			} catch(SQLException err) {
				environment.getLogger().logp(Level.SEVERE, getClass().getName(), "tableUpdated", null, err);
			}
		}
	};

	/**
	 * Starts the backup daemon (as one thread per FileReplication.
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
								environment.getLogger().logp(Level.SEVERE, getClass().getName(), "run", null, err);
								try {
									Thread.sleep(60000);
								} catch(InterruptedException err2) {
									environment.getLogger().logp(Level.WARNING, getClass().getName(), "run", null, err2);
								}
							} catch(IOException err) {
								environment.getLogger().logp(Level.SEVERE, getClass().getName(), "run", null, err);
								try {
									Thread.sleep(60000);
								} catch(InterruptedException err2) {
									environment.getLogger().logp(Level.WARNING, getClass().getName(), "run", null, err2);
								}
							} catch(SQLException err) {
								environment.getLogger().logp(Level.SEVERE, getClass().getName(), "run", null, err);
								try {
									Thread.sleep(60000);
								} catch(InterruptedException err2) {
									environment.getLogger().logp(Level.WARNING, getClass().getName(), "run", null, err2);
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
			Host thisServer = environment.getThisServer();
			Logger logger = environment.getLogger();
			boolean isDebug = logger.isLoggable(Level.FINE);
			//AOServConnector conn = environment.getConnector();
			List<FileReplication> removedList = new ArrayList<FileReplication>(threads.keySet());
			for(FileReplication ffr : thisServer.getFailoverFileReplications()) {
				removedList.remove(ffr);
				if(!threads.containsKey(ffr)) {
					if(isDebug) logger.logp(Level.FINE, getClass().getName(), "verifyThreads", "Starting BackupDaemonThread for "+ffr);
					BackupDaemonThread thread = new BackupDaemonThread(environment, ffr);
					threads.put(ffr, thread);
					thread.start();
				}
			}
			for(FileReplication ffr : removedList) {
				BackupDaemonThread thread = threads.get(ffr);
				if(isDebug) logger.logp(Level.FINE, getClass().getName(), "verifyThreads", "Stopping BackupDaemonThread for "+ffr);
				thread.stop();
			}
			for(FileReplication ffr : removedList) {
				BackupDaemonThread thread = threads.remove(ffr);
				if(isDebug) logger.logp(Level.FINE, getClass().getName(), "verifyThreads", "Joining BackupDaemonThread for "+ffr);
				try {
					thread.join();
				} catch(InterruptedException err) {
					environment.getLogger().logp(Level.WARNING, getClass().getName(), "verifyThreads", null, err);
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
			Logger logger = environment.getLogger();
			boolean isDebug = logger.isLoggable(Level.FINE);
			// Stop each thread
			for(Map.Entry<FileReplication,BackupDaemonThread> entry : threads.entrySet()) {
				if(isDebug) logger.logp(Level.FINE, getClass().getName(), "stop", "Stopping BackupDaemonThread for "+entry.getKey());
				entry.getValue().stop();
			}
			// Join each thread (wait for actual stop)
			for(Map.Entry<FileReplication,BackupDaemonThread> entry : threads.entrySet()) {
				if(isDebug) logger.logp(Level.FINE, getClass().getName(), "stop", "Joining BackupDaemonThread for "+entry.getKey());
				try {
					entry.getValue().join();
				} catch(InterruptedException err) {
					environment.getLogger().logp(Level.WARNING, getClass().getName(), "stop", null, err);
				}
			}
			threads.clear();
		}
	}

	synchronized public void runNow(FileReplication ffr) {
		BackupDaemonThread thread = threads.get(ffr);
		if(thread!=null) thread.runNow();
	}

	private static class DynamicBitRateProvider implements BitRateProvider {

		final private BackupEnvironment environment;
		final private FileReplication originalFfr;

		private DynamicBitRateProvider(BackupEnvironment environment, FileReplication ffr) {
			this.environment = environment;
			this.originalFfr = ffr;
		}

		@Override
		public Long getBitRate() {
			try {
				// Try to get the latest version of originalFfr
				FileReplication newFfr = originalFfr.getTable().get(originalFfr.getKey());
				if(newFfr!=null) return newFfr.getBitRate();
			} catch(IOException err) {
				environment.getLogger().logp(Level.SEVERE, DynamicBitRateProvider.class.getName(), "getBitRate", null, err);
			} catch(SQLException err) {
				environment.getLogger().logp(Level.SEVERE, DynamicBitRateProvider.class.getName(), "getBitRate", null, err);
			} catch(RuntimeException err) {
				environment.getLogger().logp(Level.SEVERE, DynamicBitRateProvider.class.getName(), "getBitRate", null, err);
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
		 * Gets the next filenames, up to batchSize, removing those found from required.
		 * @return the number of files in the array, zero (0) indicates iteration has completed
		 */
		private static int getNextFilenames(Set<String> remainingRequiredFilenames, Iterator<String> filenameIterator, String[] filenames, int batchSize) throws IOException {
			int c=0;
			while(c<batchSize) {
				if(!filenameIterator.hasNext()) break;
				String filename = filenameIterator.next();
				// Remove from required
				String requiredFilename = filename;
				if(requiredFilename.endsWith(File.separator)) {
					requiredFilename = requiredFilename.substring(0, requiredFilename.length()-1);
				}
				//System.err.println("DEBUG: BackupDaemon: filename="+filename);
				remainingRequiredFilenames.remove(requiredFilename);
				filenames[c++]=filename;
			}
			return c;
		}

		private final BackupEnvironment environment;
		private final FileReplication ffr;
		volatile private boolean runNow;
		private Thread thread;
		private Thread lastThread;

		private BackupDaemonThread(BackupEnvironment environment, FileReplication ffr) {
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
					if(currentThread != thread) return;
				}
				Logger logger = environment.getLogger();
				boolean isDebug = logger.isLoggable(Level.FINE);
				try {
					Random random = environment.getRandom();
					short retention = ffr.getRetention().getDays();

					// Get the last start time and success flag from the database (will be cached locally unless an error occurs
					Timestamp lastStartTime = null;
					boolean lastPassSuccessful = false;
					List<FileReplicationLog> ffls = ffr.getFailoverFileLogs(1);
					if(!ffls.isEmpty()) {
						FileReplicationLog lastLog = ffls.get(0);
						if(isDebug) logger.logp(Level.FINE, getClass().getName(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "lastLog="+lastLog);
						lastStartTime = lastLog.getStartTime();
						if(isDebug) logger.logp(Level.FINE, getClass().getName(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "lastStartTime="+SQLUtility.getDateTime(lastStartTime==null ? -1 : lastStartTime.getTime()));
						lastPassSuccessful = lastLog.isSuccessful();
						if(isDebug) logger.logp(Level.FINE, getClass().getName(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "lastPassSuccessful="+lastPassSuccessful);
					}
					// Single calendar instance is used
					Calendar cal = Calendar.getInstance();
					long lastCheckTime = -1;
					int lastCheckHour = -1; // The last hour that the schedule was checked
					int lastCheckMinute = -1; // The last minute that was checked
					while(true) {
						synchronized(this) {
							if(currentThread != thread) return;
						}
						// Sleep then look for the next (possibly missed) schedule
						if(!runNow) {
							try {
								// Sleep some before checking again, this is randomized so schedules don't start exactly as scheduled
								// But they should start within 5 minutes of the schedule.  This is because many people
								// may schedule for certain times (like 6:00 am exactly)
								//long sleepyTime = 60*1000 + random.nextInt(4*60*1000);
								long sleepyTime = 55*1000;
								if(isDebug) logger.logp(Level.FINE, getClass().getName(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "Sleeping for "+sleepyTime+" milliseconds before checking if backup pass needed.");
								Thread.sleep(sleepyTime);
							} catch(InterruptedException err) {
								// May be interrupted by stop call
							}
						}
						synchronized(this) {
							if(currentThread != thread) return;
						}

						// Get the latest ffr object (if cache was invalidated) to adhere to changes in enabled flag
						FileReplication newFFR = environment.getConnector().getFailoverFileReplications().get(ffr.getPkey());
						synchronized(this) {
							if(currentThread != thread) return;
						}

						if(newFFR==null) {
							// Don't try to run removed ffr
							if(isDebug) logger.logp(Level.FINE, getClass().getName(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "Replication removed");
						} else if(!newFFR.getEnabled()) {
							// Don't try to run disabled ffr
							if(isDebug) logger.logp(Level.FINE, getClass().getName(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "Replication not enabled");
						} else {
							long currentTime = System.currentTimeMillis();
							cal.setTimeInMillis(currentTime);
							int currentHour = cal.get(Calendar.HOUR_OF_DAY);
							int currentMinute = cal.get(Calendar.MINUTE);

							if(isDebug) logger.logp(Level.FINE, getClass().getName(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "newFFR="+newFFR);
							Host thisServer=environment.getThisServer();
							if(isDebug) logger.logp(Level.FINE, getClass().getName(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "thisServer="+thisServer);
							Server thisAOServer = thisServer.getAOServer();
							Server failoverServer = thisAOServer==null ? null : thisAOServer.getFailoverServer();
							if(isDebug) logger.logp(Level.FINE, getClass().getName(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "failoverServer="+failoverServer);
							Server toServer=newFFR.getBackupPartition().getAOServer();
							if(isDebug) logger.logp(Level.FINE, getClass().getName(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "toServer="+toServer);
							synchronized(this) {
								if(currentThread != thread) return;
							}

							// Should it run now?
							boolean shouldRun;
							if(
								// Will not replicate if the to server is our parent server in failover mode
								toServer.equals(failoverServer)
							) {
								shouldRun = false;
								if(isDebug) logger.logp(Level.FINE, getClass().getName(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "Refusing to replication to our failover parent: "+failoverServer);
							} else if(runNow) {
								shouldRun = true;
								runNow = false;
								if(isDebug) logger.logp(Level.FINE, getClass().getName(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "runNow causing immediate run of backup");
							} else if(
								// Never ran before
								lastStartTime==null
							) {
								shouldRun = true;
								if(isDebug) logger.logp(Level.FINE, getClass().getName(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "Never ran this mirror");
							} else if(
								// If the last attempt failed, run now
								!lastPassSuccessful
							) {
								shouldRun = true;
								if(isDebug) logger.logp(Level.FINE, getClass().getName(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "The last attempt at this mirror failed");
							} else if(
								// Last pass in the future (time reset)
								lastStartTime.getTime() > currentTime
							) {
								shouldRun = false;
								if(isDebug) logger.logp(Level.FINE, getClass().getName(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "Last pass in the future (time reset)");
							} else if(
								// Last pass more than 24 hours ago (this handles replications without schedules)
								(currentTime - lastStartTime.getTime())>=(24*60*60*1000)
							) {
								shouldRun = true;
								if(isDebug) logger.logp(Level.FINE, getClass().getName(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "Last pass more than 24 hours ago");
							} else if(
								// The system time was set back
								lastCheckTime!=-1 && lastCheckTime>currentTime
							) {
								shouldRun = false;
								if(isDebug) logger.logp(Level.FINE, getClass().getName(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "Last check time in the future (time reset)");
							} else {
								// If there is currently no schedule, this will not flag shouldRun and the check for 24-hours passed (above) will force the backup
								List<FileReplicationSchedule> schedules = newFFR.getFailoverFileSchedules();
								synchronized(this) {
									if(currentThread != thread) return;
								}
								shouldRun = false;
								for(FileReplicationSchedule schedule : schedules) {
									if(schedule.isEnabled()) {
										int scheduleHour = schedule.getHour();
										int scheduleMinute = schedule.getMinute();
										if(
											// Look for exact time match
											currentHour==scheduleHour
											&& currentMinute==scheduleMinute
										) {
											shouldRun = true;
											if(isDebug) logger.logp(Level.FINE, getClass().getName(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "It is the scheduled time: scheduleHour="+scheduleHour+" and scheduleMinute="+scheduleMinute);
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
										for(FileReplicationSchedule schedule : schedules) {
											if(schedule.isEnabled()) {
												int scheduleHour = schedule.getHour();
												int scheduleMinute = schedule.getMinute();
												if(lastCheckHour==scheduleHour && lastCheckMinute==scheduleMinute) {
													shouldRun = true;
													if(isDebug) logger.logp(Level.FINE, getClass().getName(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "Missed a scheduled time: scheduleHour="+scheduleHour+" and scheduleMinute="+scheduleMinute);
													break CHECK_LOOP;
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
									if(currentThread != thread) return;
								}
								try {
									lastStartTime = new Timestamp(currentTime);
									lastPassSuccessful = false;
									try {
										backupPass(newFFR);
									} finally {
										runNow = false;
									}
									lastPassSuccessful = true;
								} catch(RuntimeException T) {
									environment.getLogger().logp(Level.SEVERE, getClass().getName(), "run", null, T);
									synchronized(this) {
										if(currentThread != thread) return;
									}
									//Randomized sleep interval to reduce master load on startup (5-15 minutes)
									int sleepyTime = 5*60*1000 + random.nextInt(10*60*1000);
									if(isDebug) logger.logp(Level.FINE, getClass().getName(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "Sleeping for "+sleepyTime+" milliseconds after an error");
									try {
										Thread.sleep(sleepyTime); 
									} catch(InterruptedException err) {
										// May be interrupted by stop call
									}
								} catch(IOException T) {
									environment.getLogger().logp(Level.SEVERE, getClass().getName(), "run", null, T);
									synchronized(this) {
										if(currentThread != thread) return;
									}
									//Randomized sleep interval to reduce master load on startup (5-15 minutes)
									int sleepyTime = 5*60*1000 + random.nextInt(10*60*1000);
									if(isDebug) logger.logp(Level.FINE, getClass().getName(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "Sleeping for "+sleepyTime+" milliseconds after an error");
									try {
										Thread.sleep(sleepyTime); 
									} catch(InterruptedException err) {
										// May be interrupted by stop call
									}
								} catch(SQLException T) {
									environment.getLogger().logp(Level.SEVERE, getClass().getName(), "run", null, T);
									synchronized(this) {
										if(currentThread != thread) return;
									}
									//Randomized sleep interval to reduce master load on startup (5-15 minutes)
									int sleepyTime = 5*60*1000 + random.nextInt(10*60*1000);
									if(isDebug) logger.logp(Level.FINE, getClass().getName(), "run", (retention!=1 ? "Backup: " : "Failover: ") + "Sleeping for "+sleepyTime+" milliseconds after an error");
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
					environment.getLogger().logp(Level.SEVERE, getClass().getName(), "run", null, T);
					synchronized(this) {
						if(currentThread != thread) return;
					}
					//Randomized sleep interval to reduce master load on startup (5-15 minutes)
					int sleepyTime = 5*60*1000 + (int)(Math.random()*(10*60*1000));
					if(isDebug) logger.logp(Level.FINE, getClass().getName(), "run", "Sleeping for "+sleepyTime+" milliseconds after an error");
					try {
						Thread.sleep(sleepyTime);
					} catch(InterruptedException err) {
						// May be interrupted by stop call
					}
				}
			}
		}

		private void backupPass(FileReplication ffr) throws IOException, SQLException {
			final Thread currentThread = Thread.currentThread();

			environment.preBackup(ffr);
			synchronized(this) {
				if(currentThread != thread) return;
			}
			environment.init(ffr);
			try {
				Logger logger = environment.getLogger();
				boolean isDebug = logger.isLoggable(Level.FINE);
				synchronized(this) {
					if(currentThread != thread) return;
				}
				final Host thisServer = environment.getThisServer();
				final int failoverBatchSize = environment.getFailoverBatchSize(ffr);
				final Server toServer=ffr.getBackupPartition().getAOServer();
				final boolean useCompression = ffr.getUseCompression();
				final short retention = ffr.getRetention().getDays();
				synchronized(this) {
					if(currentThread != thread) return;
				}

				if(isDebug) logger.logp(Level.FINE, getClass().getName(), "backupPass", (retention>1 ? "Backup: " : "Failover: ") + "Running failover from "+thisServer+" to "+toServer);

				Calendar cal = Calendar.getInstance();
				final long startTime=cal.getTimeInMillis();

				if(isDebug) logger.logp(Level.FINE, getClass().getName(), "backupPass", (retention>1 ? "Backup: " : "Failover: ") + "useCompression="+useCompression);
				if(isDebug) logger.logp(Level.FINE, getClass().getName(), "backupPass", (retention>1 ? "Backup: " : "Failover: ") + "retention="+retention);

				// Keep statistics during the replication
				int scanned = 0;
				int updated = 0;
				long rawBytesOut = 0;
				long rawBytesIn = 0;
				boolean isSuccessful = false;
				try {
					// Get the connection to the daemon
					Server.DaemonAccess daemonAccess = ffr.requestReplicationDaemonAccess();

					// First, the specific source address from ffr is used
					InetAddress sourceIPAddress = ffr.getConnectFrom();
					if(sourceIPAddress==null) {
						sourceIPAddress = environment.getDefaultSourceIPAddress();
					}
					synchronized(this) {
						if(currentThread != thread) return;
					}
					AOServDaemonConnector daemonConnector = AOServDaemonConnector.getConnector(
						daemonAccess.getHost(),
						sourceIPAddress,
						daemonAccess.getPort(),
						daemonAccess.getProtocol(),
						null,
						toServer.getPoolSize(),
						AOPool.DEFAULT_MAX_CONNECTION_AGE,
						AOServClientConfiguration.getSslTruststorePath(),
						AOServClientConfiguration.getSslTruststorePassword(),
						environment.getLogger()
					);
					AOServDaemonConnection daemonConn = daemonConnector.getConnection();
					try {
						synchronized(this) {
							if(currentThread != thread) return;
						}
						// Start the replication
						CompressedDataOutputStream rawOut = daemonConn.getRequestOut(AOServDaemonProtocol.FAILOVER_FILE_REPLICATION);

						MD5 md5 = useCompression ? new MD5() : null;

						rawOut.writeLong(daemonAccess.getKey());
						rawOut.writeBoolean(useCompression);
						rawOut.writeShort(retention);

						// Determine the date on the from server
						final int year = cal.get(Calendar.YEAR);
						final int month = cal.get(Calendar.MONTH)+1;
						final int day = cal.get(Calendar.DAY_OF_MONTH);
						rawOut.writeShort(year);
						rawOut.writeShort(month);
						rawOut.writeShort(day);
						if(retention == 1) {
							List<MySQLServerName> replicatedMySQLServers = environment.getReplicatedMySQLServers(ffr);
							List<String> replicatedMySQLMinorVersions = environment.getReplicatedMySQLMinorVersions(ffr);
							int len = replicatedMySQLServers.size();
							rawOut.writeCompressedInt(len);
							for(int c = 0; c < len; c++) {
								rawOut.writeUTF(replicatedMySQLServers.get(c).toString());
								rawOut.writeUTF(replicatedMySQLMinorVersions.get(c));
							}
						}
						rawOut.flush();
						synchronized(this) {
							if(currentThread != thread) return;
						}

						CompressedDataInputStream rawIn=daemonConn.getResponseIn();
						int result=rawIn.read();
						synchronized(this) {
							if(currentThread != thread) return;
						}
						if(result == AOServDaemonProtocol.NEXT) {
							// Only the output is limited because input should always be smaller than the output
							final ByteCountOutputStream rawBytesOutStream = new ByteCountOutputStream(
								new BitRateOutputStream(
									rawOut,
									new DynamicBitRateProvider(environment, ffr)
								)
							);
							final CompressedDataOutputStream out = new CompressedDataOutputStream(
								/*useCompression
								? new AutoFinishGZIPOutputStream(new DontCloseOutputStream(rawBytesOutStream), BufferManager.BUFFER_SIZE)
								:*/ rawBytesOutStream
							);

							final ByteCountInputStream rawBytesInStream = new ByteCountInputStream(rawIn);
							final CompressedDataInputStream in = new CompressedDataInputStream(rawBytesInStream);
							try {
								// Do requests in batches
								final String[] filenames = new String[failoverBatchSize];
								final int[] results = new int[failoverBatchSize];
								final long[] chunkingSizes = useCompression ? new long[failoverBatchSize] : null;
								final long[][] md5His = useCompression ? new long[failoverBatchSize][] : null;
								final long[][] md5Los = useCompression ? new long[failoverBatchSize][] : null;
								final Set<String> remainingRequiredFilenames = new LinkedHashSet<String>(environment.getRequiredFilenames(ffr));
								final byte[] chunkBuffer = new byte[AOServDaemonProtocol.FAILOVER_FILE_REPLICATION_CHUNK_SIZE];
								final Iterator<String> filenameIterator = environment.getFilenameIterator(ffr);
								while(true) {
									synchronized(this) {
										if(currentThread != thread) return;
									}
									int batchSize = getNextFilenames(remainingRequiredFilenames, filenameIterator, filenames, failoverBatchSize);
									if(batchSize == 0) break;

									out.writeCompressedInt(batchSize);
									for(int d = 0; d < batchSize; d++) {
										scanned++;
										String filename = filenames[d];
										try {
											long mode = environment.getStatMode(ffr, filename);
											if(!UnixFile.isSocket(mode)) {
												// Get all the values first to avoid FileNotFoundException in middle of protocol
												boolean isRegularFile = UnixFile.isRegularFile(mode);
												long size = isRegularFile ? environment.getLength(ffr, filename) : -1;
												int uid = environment.getUid(ffr, filename);
												int gid = environment.getGid(ffr, filename);
												boolean isSymLink = UnixFile.isSymLink(mode);
												long modifyTime = isSymLink ? -1 : environment.getModifyTime(ffr, filename);
												//if(modifyTime<1000 && !isSymLink && log.isWarnEnabled()) log.warn("Non-symlink modifyTime<1000: "+filename+": "+modifyTime);
												String symLinkTarget;
												if(isSymLink) {
													try {
														symLinkTarget = environment.readLink(ffr, filename);
													} catch(SecurityException err) {
														environment.getLogger().logp(Level.SEVERE, getClass().getName(), "backupPass", "SecurityException trying to readlink: "+filename, err);
														throw err;
													} catch(IOException err) {
														environment.getLogger().logp(Level.SEVERE, getClass().getName(), "backupPass", "IOException trying to readlink: "+filename, err);
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
													environment.getLogger().logp(Level.WARNING, getClass().getName(), "backupPass", null, new IOException("UID out of range, converted to 0, uid="+uid+" and path="+filename));
													uid = 0;
												}
												out.writeCompressedInt(uid);
												if(gid<0 || gid>65535) {
													environment.getLogger().logp(Level.WARNING, getClass().getName(), "backupPass", null, new IOException("GID out of range, converted to 0, gid="+gid+" and path="+filename));
													gid = 0;
												}
												out.writeCompressedInt(gid);
												// TODO: Once glibc >= 2.6 and kernel >= 2.6.22, can use lutimes call for symbolic links
												if(!isSymLink) out.writeLong(modifyTime);
												if(isSymLink) out.writeCompressedUTF(symLinkTarget, 1);
												else if(isDevice) out.writeLong(deviceID);
											} else {
												filenames[d] = null;
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
										if(currentThread != thread) return;
									}

									// Read the results
									result = in.read();
									synchronized(this) {
										if(currentThread != thread) return;
									}
									boolean hasRequestData = false;
									if(result == AOServDaemonProtocol.NEXT) {
										for(int d = 0; d < batchSize; d++) {
											if(filenames[d] != null) {
												synchronized(this) {
													if(currentThread != thread) return;
												}
												result = in.read();
												results[d] = result;
												if(result == AOServDaemonProtocol.FAILOVER_FILE_REPLICATION_MODIFIED_REQUEST_DATA) {
													hasRequestData = true;
												} else if(result == AOServDaemonProtocol.FAILOVER_FILE_REPLICATION_MODIFIED_REQUEST_DATA_CHUNKED) {
													hasRequestData = true;
													long chunkingSize = in.readLong();
													int numChunks;
													{
														long numChunksL = chunkingSize >> AOServDaemonProtocol.FAILOVER_FILE_REPLICATION_CHUNK_SIZE_BITS;
														if((chunkingSize & (AOServDaemonProtocol.FAILOVER_FILE_REPLICATION_CHUNK_SIZE-1)) != 0) numChunksL++;
														numChunks = SafeMath.castInt(numChunksL);
													}
													long[] md5Hi = new long[numChunks];
													long[] md5Lo = new long[numChunks];
													for(int e = 0; e < numChunks; e++) {
														md5Hi[e] = in.readLong();
														md5Lo[e] = in.readLong();
													}
													chunkingSizes[d] = chunkingSize;
													md5His[d] = md5Hi;
													md5Los[d] = md5Lo;
												}
											}
										}
									} else {
										if (result == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
										else if (result == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
										else throw new IOException("Unknown result: " + result);
									}
									synchronized(this) {
										if(currentThread != thread) return;
									}

									// Process the results
									//DeflaterOutputStream deflaterOut;
									final CompressedDataOutputStream outgoing;

									if(hasRequestData) {
										//deflaterOut = null;
										outgoing = out;
									} else {
										//deflaterOut = null;
										outgoing = null;
									}
									for(int d = 0; d < batchSize; d++) {
										synchronized(this) {
											if(currentThread != thread) return;
										}
										String filename = filenames[d];
										if(filename != null) {
											result = results[d];
											if(result == AOServDaemonProtocol.FAILOVER_FILE_REPLICATION_MODIFIED) {
												if(isDebug) logger.logp(Level.FINE, getClass().getName(), "backupPass", (retention>1 ? "Backup: " : "Failover: ") + "File modified: "+filename);
												updated++;
											} else if(result == AOServDaemonProtocol.FAILOVER_FILE_REPLICATION_MODIFIED_REQUEST_DATA) {
												assert outgoing != null;
												updated++;
												try {
													if(isDebug) logger.logp(Level.FINE, getClass().getName(), "backupPass", (retention>1 ? "Backup: " : "Failover: ") + "Sending file contents: "+filename);
													// Shortcut for 0 length files (don't open for reading)
													if(environment.getLength(ffr, filename) != 0) {
														InputStream fileIn = environment.getInputStream(ffr, filename);
														try {
															// Read in full chunk size until end of file
															// Only the last chunk may be less than a full chunk size
															while(true) {
																synchronized(this) {
																	if(currentThread != thread) return;
																}
																int pos = 0;
																do {
																	int ret = fileIn.read(chunkBuffer, pos, AOServDaemonProtocol.FAILOVER_FILE_REPLICATION_CHUNK_SIZE - pos);
																	if(ret == -1) break;
																	pos += ret;
																} while(pos < AOServDaemonProtocol.FAILOVER_FILE_REPLICATION_CHUNK_SIZE);
																synchronized(this) {
																	if(currentThread != thread) return;
																}
																if(pos > 0) {
																	outgoing.write(AOServDaemonProtocol.NEXT);
																	outgoing.writeCompressedInt(pos);
																	outgoing.write(chunkBuffer, 0, pos);
																}
																// Check end of file
																if(pos < AOServDaemonProtocol.FAILOVER_FILE_REPLICATION_CHUNK_SIZE) {
																	break;
																}
															}
														} finally {
															fileIn.close();
														}
													}
												} catch(FileNotFoundException err) {
													// Normal when the file was deleted
												} catch(IOException e) {
													IOException ioExc = new IOException("filename="+ filename);
													ioExc.initCause(e);
													throw ioExc;
												} finally {
													synchronized(this) {
														if(currentThread != thread) return;
													}
													outgoing.write(AOServDaemonProtocol.DONE);
												}
											} else if(result==AOServDaemonProtocol.FAILOVER_FILE_REPLICATION_MODIFIED_REQUEST_DATA_CHUNKED) {
												assert outgoing != null;
												assert md5 != null;
												updated++;
												try {
													if(isDebug) logger.logp(Level.FINE, getClass().getName(), "backupPass", (retention>1 ? "Backup: " : "Failover: ") + "Chunking file contents: "+filename);
													final long[] md5Hi = md5His[d];
													final long[] md5Lo = md5Los[d];
													assert md5Lo.length == md5Hi.length;
													final int numChunks = md5Hi.length;
													InputStream fileIn = environment.getInputStream(ffr, filename);
													try {
														int chunkNumber = 0;
														int sendChunkCount = 0;
														while(true) {
															synchronized(this) {
																if(currentThread != thread) return;
															}
															// Read fully one chunk or to end of file
															int pos = 0;
															do {
																int ret = fileIn.read(chunkBuffer, pos, AOServDaemonProtocol.FAILOVER_FILE_REPLICATION_CHUNK_SIZE - pos);
																if(ret == -1) break;
																pos += ret;
															} while(pos < AOServDaemonProtocol.FAILOVER_FILE_REPLICATION_CHUNK_SIZE);
															synchronized(this) {
																if(currentThread != thread) return;
															}
															if(pos > 0) {
																if(chunkNumber < numChunks) {
																	final int chunkSize;
																	if(chunkNumber < (numChunks - 1)) {
																		// All but last chunk must be full-sized
																		chunkSize = AOServDaemonProtocol.FAILOVER_FILE_REPLICATION_CHUNK_SIZE;
																	} else {
																		assert chunkNumber == (numChunks - 1);
																		// Last chunk may be partial
																		chunkSize = SafeMath.castInt(chunkingSizes[d] - (((long)chunkNumber) << AOServDaemonProtocol.FAILOVER_FILE_REPLICATION_CHUNK_SIZE_BITS));
																		assert chunkSize > 0 && chunkSize <= AOServDaemonProtocol.FAILOVER_FILE_REPLICATION_CHUNK_SIZE;
																	}
																	if(pos < chunkSize) {
																		// Last chunk not fully read, just send data
																		sendChunkCount++;
																		outgoing.write(AOServDaemonProtocol.NEXT);
																		outgoing.writeCompressedInt(pos);
																		outgoing.write(chunkBuffer, 0, pos);
																	} else {
																		// Calculate the MD5 hash
																		md5.Init();
																		md5.Update(chunkBuffer, 0, chunkSize);
																		byte[] md5Bytes = md5.Final();
																		if(
																			md5Hi[chunkNumber] != MD5.getMD5Hi(md5Bytes)
																			|| md5Lo[chunkNumber] != MD5.getMD5Lo(md5Bytes)
																		) {
																			// MD5 mismatch, just send data
																			sendChunkCount++;
																			outgoing.write(AOServDaemonProtocol.NEXT);
																			outgoing.writeCompressedInt(pos);
																			outgoing.write(chunkBuffer, 0, pos);
																		} else {
																			outgoing.write(AOServDaemonProtocol.NEXT_CHUNK);
																			// Send any beyond the last chunk (file has grown)
																			if(pos > chunkSize) {
																				outgoing.write(AOServDaemonProtocol.NEXT);
																				int bytesBeyond = pos - chunkSize;
																				outgoing.writeCompressedInt(bytesBeyond);
																				outgoing.write(chunkBuffer, chunkSize, bytesBeyond);
																			}
																		}
																	}
																} else {
																	// Chunk past those sent from server
																	outgoing.write(AOServDaemonProtocol.NEXT);
																	outgoing.writeCompressedInt(pos);
																	outgoing.write(chunkBuffer, 0, pos);
																}
																// Increment chunk number for next iteration
																chunkNumber++;
															}
															// Check end of file
															if(pos < AOServDaemonProtocol.FAILOVER_FILE_REPLICATION_CHUNK_SIZE) {
																break;
															}
														}
														if(isDebug) logger.logp(Level.FINE, getClass().getName(), "backupPass", (retention>1 ? "Backup: " : "Failover: ") + "Chunking file contents: "+filename+": Sent "+sendChunkCount+" out of "+chunkNumber+" chunks");
													} finally {
														fileIn.close();
													}
												} catch(FileNotFoundException err) {
													// Normal when the file was deleted
												} catch(IOException e) {
													IOException ioExc = new IOException("filename="+ filename);
													ioExc.initCause(e);
													throw ioExc;
												} finally {
													synchronized(this) {
														if(currentThread != thread) return;
													}
													outgoing.write(AOServDaemonProtocol.DONE);
												}
											} else if(result != AOServDaemonProtocol.FAILOVER_FILE_REPLICATION_NO_CHANGE) throw new IOException("Unknown result: "+result);
										}
									}

									// Flush any file data that was sent
									if(hasRequestData) {
										assert outgoing != null;
										synchronized(this) {
											if(currentThread != thread) return;
										}
										outgoing.flush();
									}
								}

								// Error now if not all required have been found
								if(!remainingRequiredFilenames.isEmpty()) {
									StringBuilder message = new StringBuilder("Required files not found.  Successfully sent all found, but not flagging the backup as successful because the following files were missing:");
									for(String filename : remainingRequiredFilenames) {
										message.append('\n').append(filename);
									}
									throw new IOException(message.toString());
								}
								// Tell the server we are finished
								synchronized(this) {
									if(currentThread != thread) return;
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
									if(currentThread != thread) return;
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
					isSuccessful = true;
				} finally {
					// Store the statistics
					for(int c = 0; c < 10; c++) {
						// Try in a loop with delay in case master happens to be restarting
						try {
							ffr.addFailoverFileLog(startTime, System.currentTimeMillis(), scanned, updated, rawBytesOut+rawBytesIn, isSuccessful);
							break;
						} catch(RuntimeException err) {
							if(c >= 10) {
								environment.getLogger().logp(Level.SEVERE, getClass().getName(), "backupPass", "Error adding failover file log, giving up", err);
							} else {
								environment.getLogger().logp(Level.SEVERE, getClass().getName(), "backupPass", "Error adding failover file log, will retry in one minute", err);
								try {
									Thread.sleep(60*1000);
								} catch(InterruptedException err2) {
									environment.getLogger().logp(Level.WARNING, getClass().getName(), "backupPass", null, err2);
								}
							}
						}
					}
				}
			} finally {
				environment.cleanup(ffr);
			}
			synchronized(this) {
				if(currentThread != thread) return;
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
		if(args.length != 1) {
			try {
				showUsage();
				System.exit(1);
			} catch(IOException err) {
				ErrorPrinter.printStackTraces(err);
				System.exit(5);
			}
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
					Logger logger = environment.getLogger();
					logger.logp(Level.SEVERE, BackupDaemon.class.getName(), "main", null, T);
					try {
						Thread.sleep(60000);
					} catch(InterruptedException err) {
						logger.logp(Level.WARNING, BackupDaemon.class.getName(), "main", null, err);
					}
				}
			}
		}
	}

	public static void showUsage() throws IOException {
		TerminalWriter out=new TerminalWriter(new OutputStreamWriter(System.err));
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
