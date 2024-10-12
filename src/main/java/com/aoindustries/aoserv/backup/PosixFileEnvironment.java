/*
 * aoserv-backup - Backup client for the AOServ Platform.
 * Copyright (C) 2003-2009, 2018, 2019, 2020, 2021, 2022, 2024  AO Industries, Inc.
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

import com.aoapps.io.posix.PosixFile;
import com.aoapps.io.posix.Stat;
import com.aoindustries.aoserv.client.backup.FileReplication;
import com.aoindustries.aoserv.client.net.Host;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * A <code>PosixEnvironment</code> controls the backup system on
 * a standalone Posix <code>Host</code>.
 *
 * @see  Host
 *
 * @author  AO Industries, Inc.
 */
public abstract class PosixFileEnvironment extends FileEnvironment {

  private final Object unixFileCacheLock = new Object();
  private final Map<FileReplication, File> lastFiles = new HashMap<>();
  private final Map<FileReplication, PosixFile> lastPosixFiles = new HashMap<>();
  private final Map<FileReplication, Stat> lastStats = new HashMap<>();

  /**
   * Gets the POSIX file for the given path.
   */
  protected PosixFile getPosixFile(FileReplication ffr, String filename) throws IOException {
    File file = getFile(ffr, filename);
    synchronized (unixFileCacheLock) {
      PosixFile lastPosixFile;
      if (file != lastFiles.get(ffr)) {
        lastPosixFile = new PosixFile(file);
        lastPosixFiles.put(ffr, lastPosixFile);
        lastStats.put(ffr, lastPosixFile.getStat());
        lastFiles.put(ffr, file);
      } else {
        lastPosixFile = lastPosixFiles.get(ffr);
      }
      return lastPosixFile;
    }
  }

  /**
   * Stats the given path.
   */
  protected Stat getStat(FileReplication ffr, String filename) throws IOException {
    if (filename == null) {
      throw new AssertionError("filename is null");
    }
    File file = getFile(ffr, filename);
    if (file == null) {
      throw new AssertionError("file is null");
    }
    synchronized (unixFileCacheLock) {
      Stat lastStat;
      if (file != lastFiles.get(ffr)) {
        PosixFile lastPosixFile = new PosixFile(file);
        lastPosixFiles.put(ffr, lastPosixFile);
        lastStat = lastPosixFile.getStat();
        lastStats.put(ffr, lastStat);
        lastFiles.put(ffr, file);
      } else {
        lastStat = lastStats.get(ffr);
      }
      return lastStat;
    }
  }

  @Override
  public long getStatMode(FileReplication ffr, String filename) throws IOException {
    return getStat(ffr, filename).getRawMode();
  }

  @Override
  public int getUid(FileReplication ffr, String filename) throws IOException {
    return getStat(ffr, filename).getUid();
  }

  @Override
  public int getGid(FileReplication ffr, String filename) throws IOException {
    return getStat(ffr, filename).getGid();
  }

  @Override
  public long getModifyTime(FileReplication ffr, String filename) throws IOException {
    return getStat(ffr, filename).getModifyTime();
  }

  @Override
  public long getLength(FileReplication ffr, String filename) throws IOException {
    return getStat(ffr, filename).getSize();
  }

  @Override
  public String readLink(FileReplication ffr, String filename) throws IOException {
    return getPosixFile(ffr, filename).readLink();
  }

  @Override
  public long getDeviceIdentifier(FileReplication ffr, String filename) throws IOException {
    return getStat(ffr, filename).getDeviceIdentifier();
  }

  @Override
  public void cleanup(FileReplication ffr) throws IOException, SQLException {
    try {
      synchronized (unixFileCacheLock) {
        lastFiles.remove(ffr);
        lastPosixFiles.remove(ffr);
        lastStats.remove(ffr);
      }
    } finally {
      super.cleanup(ffr);
    }
  }
}
