/*
 * aoserv-backup - Backup client for the AOServ Platform.
 * Copyright (C) 2008, 2009, 2018, 2019, 2021, 2022  AO Industries, Inc.
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

import com.aoapps.hodgepodge.io.FilesystemIteratorRule;
import com.aoindustries.aoserv.client.backup.FileReplication;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Windows environment.
 *
 * @author  AO Industries, Inc.
 */
public abstract class WindowsEnvironment extends FileEnvironment {

  @Override
  protected Map<String, FilesystemIteratorRule> getFilesystemIteratorRules(FileReplication ffr) throws IOException, SQLException {
    Map<String, FilesystemIteratorRule> filesystemRules = new HashMap<>();
    return filesystemRules;
  }

  @Override
  protected Map<String, FilesystemIteratorRule> getFilesystemIteratorPrefixRules(FileReplication ffr) throws IOException, SQLException {
    Map<String, FilesystemIteratorRule> filesystemPrefixRules = new HashMap<>();
    return filesystemPrefixRules;
  }

  /**
   * Because some of the backup servers use ext2/ext3 filesystems, which have only one-second
   * time accuracy, timestamps are rounded down to the nearest second.  (Truncates the milliseconds component)
   * When all servers support more granular file timestamps, or if we store the more
   * accurate timestamps in extended attributes, then we can remove this for potentially greater
   * accuracy in detecting file changes.
   */
  @Override
  public long getModifyTime(FileReplication ffr, String filename) throws IOException {
    return (super.getModifyTime(ffr, filename) / 1000) * 1000;
  }
}
