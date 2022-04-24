/*
 * aoserv-backup - Backup client for the AOServ Platform.
 * Copyright (C) 2003-2009, 2018, 2019, 2020, 2022  AO Industries, Inc.
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

import com.aoindustries.aoserv.client.backup.FileReplicationSetting;

/**
 * Stores the settings for one file that is being backed-up by <code>BackupDaemon</code>.
 *
 * @see  FileReplicationSetting
 *
 * @author  AO Industries, Inc.
 */
public class BackupFileSetting {

  private String filename;
  private boolean backupEnabled;

  public void clear() {
    filename = null;
    backupEnabled = false;
  }

  public String getFilename() {
    return filename;
  }

  public boolean getBackupEnabled() {
    return backupEnabled;
  }

  public void setSettings(
      boolean backupEnabled
  ) {
    this.backupEnabled = backupEnabled;
  }

  public void setFilename(
      String filename
  ) {
    this.filename = filename;
  }
}
