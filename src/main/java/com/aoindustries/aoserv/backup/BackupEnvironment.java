/*
 * aoserv-backup - Backup client for the AOServ Platform.
 * Copyright (C) 2003-2013, 2017, 2018, 2019, 2021, 2022, 2024  AO Industries, Inc.
 *     support@aoindustries.com
 *     7262 Bull Pen Cir
 *     Mobile, AL 36695
 *
 * This file is part of aoserv-backup.
 *
 * aoserv-backup is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * aoserv-backup is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with aoserv-backup.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.aoindustries.aoserv.backup;

import com.aoapps.net.InetAddress;
import com.aoindustries.aoserv.client.AoservConnector;
import com.aoindustries.aoserv.client.backup.FileReplication;
import com.aoindustries.aoserv.client.mysql.Server;
import com.aoindustries.aoserv.client.net.Host;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.logging.Logger;

/**
 * A <code>BackupEnvironment</code> tells the <code>BackupDaemon</code> how to run.
 *
 * <p>All paths are represented as a <code>String</code> to allow anything to be backed-up, not just local files.</p>
 *
 * @author  AO Industries, Inc.
 */
public interface BackupEnvironment {

  /**
   * Gets the stat mode (or a generated and equivalent one) for a file.
   */
  long getStatMode(FileReplication ffr, String filename) throws IOException;

  /**
   * Gets the listing for a directory.
   */
  String[] getDirectoryList(FileReplication ffr, String filename) throws IOException;

  /**
   * Gets the user ID.
   */
  int getUid(FileReplication ffr, String filename) throws IOException;

  /**
   * Gets the group ID.
   */
  int getGid(FileReplication ffr, String filename) throws IOException;

  /**
   * Gets the modified time.
   */
  long getModifyTime(FileReplication ffr, String filename) throws IOException;

  /**
   * Gets the length of the file.
   */
  long getLength(FileReplication ffr, String filename) throws IOException;

  /**
   * Reads a symbolic link.
   */
  String readLink(FileReplication ffr, String filename) throws IOException;

  /**
   * Gets the device file major and minor.
   */
  long getDeviceIdentifier(FileReplication ffr, String filename) throws IOException;

  /**
   * Gets a stream reading the file.
   */
  InputStream getInputStream(FileReplication ffr, String filename) throws IOException;

  /**
   * Gets the name of a file (the part after the last slash).
   */
  String getNameOfFile(FileReplication ffr, String filename);

  /**
   * Gets the connection to the master server.
   */
  AoservConnector getConnector() throws IOException, SQLException;

  /**
   * Gets the host this process is running on.
   */
  Host getThisHost() throws IOException, SQLException;

  /**
   * Gets the number of items per batch.  A higher value will consume
   * more RAM but better hide latency.
   */
  int getFailoverBatchSize(FileReplication ffr) throws IOException, SQLException;

  /**
   * Called right before a backup pass begins.
   * Implementations should call super.preBackup first.
   *
   * <p>The process is:</p>
   *
   * <ol>
   *   <li>preBackup</li>
   *   <li>init</li>
   *   <li>cleanup (always)</li>
   *   <li>postBackup (only when backup pass successful)</li>
   * </ol>
   */
  void preBackup(FileReplication ffr) throws IOException, SQLException;

  /**
   * Called before any backup-pass data is retrieved from the environment.
   * <code>cleanup()</code> will also be called in a finally block.
   * Implementations should call super.init first.
   *
   * @see  #cleanup(FileReplication)
   * @see  #preBackup(FileReplication)
   */
  void init(FileReplication ffr) throws IOException, SQLException;

  /**
   * Called in a finally block after any backup-pass completes, no data
   * will be obtained from the environment after this is called.
   * Implementations should call super.cleanup in a finally block.
   *
   * @see  #init(FileReplication)
   * @see  #preBackup(FileReplication)
   */
  void cleanup(FileReplication ffr) throws IOException, SQLException;

  /**
   * Called right after a backup pass ends.
   * Implementations should call super.postBackup last.
   *
   * @see  #preBackup(FileReplication)
   */
  void postBackup(FileReplication ffr) throws IOException, SQLException;

  /**
   * Gets the set of paths that must be found in the backup set.  These paths
   * must not include any trailing separators.
   */
  Set<String> getRequiredFilenames(FileReplication ffr) throws IOException, SQLException;

  /**
   * Gets the <code>Iterator</code> of filenames or <code>null</code> if completed.
   */
  Iterator<String> getFilenameIterator(FileReplication ffr) throws IOException, SQLException;

  /**
   * Gets the default source IP address or <code>InetAddress.UNSPECIFIED</code> to use the system default.
   */
  InetAddress getDefaultSourceIpAddress() throws IOException, SQLException;

  /**
   * Gets the list of MySQL server instance names that are being replicated.
   * This is only used for failover mode (retention == 1), and should only
   * be used for a replication that includes MySQL replication (Server only)
   *
   * @see  #getReplicatedMysqlMinorVersions(FileReplication)
   */
  List<Server.Name> getReplicatedMysqlServers(FileReplication ffr) throws IOException, SQLException;

  /**
   * Gets the list of MySQL server versions (in the same order as the list returned by <code>getReplicatedMysqlServers</code>.
   * This is only used for failover mode (retention == 1), and should only
   * be used for a replication that includes MySQL replication (Server only)
   *
   * @see  #getReplicatedMysqlServers(FileReplication)
   */
  List<String> getReplicatedMysqlMinorVersions(FileReplication ffr) throws IOException, SQLException;

  /**
   * A fast pseudo-random number generator for non-cryptographic purposes.
   *
   * <p>Gets a Random source for the backup daemon.  This does not need to be cryptographically strong
   * because it is only used to randomize some sleep times to distribute load on the server
   * processes.</p>
   */
  Random getFastRandom();

  /**
   * Gets the logger for this environment.
   */
  Logger getLogger();

  /**
   * Converts an environment-specific filename into a server path.  The server
   * path must begin with /, may not contain /../, and must use / as the path
   * separator.
   */
  String getServerPath(FileReplication ffr, String filename);
}
