package com.aoindustries.aoserv.backup;

/*
 * Copyright 2001-2007 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.Package;
import com.aoindustries.aoserv.client.*;
import com.aoindustries.email.*;
import com.aoindustries.io.*;
import com.aoindustries.io.unix.*;
import com.aoindustries.profiler.*;
import com.aoindustries.md5.*;
import com.aoindustries.sql.*;
import com.aoindustries.util.*;
import com.aoindustries.util.sort.*;
import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.zip.*;

/**
 * The <code>BackupDaemon</code> runs on every server that is backed-up and
 * sends its backup info to the master as scheduled in the <code>servers</code>
 * table.
 *
 * @author  AO Industries, Inc.
 */
final public class BackupDaemon implements Runnable {

    final private BackupEnvironment environment;

    private BackupDaemon(BackupEnvironment environment) {
        this.environment=environment;
    }

    private final Object fileSourceLock=new Object();
    private Stack<String> currentDirectories;
    private Stack<String[]> currentLists;
    private Stack<Integer> currentIndexes;
    private boolean filesDone=false;

    /**
     * Resets the source of filenames used during the backup process.
     */
    private void resetFilenamePointer() {
        Profiler.startProfile(Profiler.FAST, BackupDaemon.class, "resetFilenamePointer()", null);
        try {
            synchronized(fileSourceLock) {
                currentDirectories=null;
                currentLists=null;
                currentIndexes=null;
                filesDone=false;
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    /**
     * Gets the next filename in the directory structure.  The <code>BackupFileSetting</code> contains <code>null</code>
     * when all files have been traversed.
     */
    private void getNextFilename(BackupFileSetting fileSetting, FileBackupSettingTable fileBackupSettingTable) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, BackupDaemon.class, "getNextFilename(BackupFileSetting,FileBackupSettingTable)", null);
        try {
            synchronized(fileSourceLock) {
                // Loop trying to get the file because some errors may normally occur due to dynamic filesystems
                while(true) {
                    if(filesDone) {
                        fileSetting.clear();
                        return;
                    }

                    // Initialize the stacks, if needed
                    if(currentDirectories==null) {
                        (currentDirectories=new Stack<String>()).push("");
                        (currentLists=new Stack<String[]>()).push(environment.getFilesystemRoots());
                        (currentIndexes=new Stack<Integer>()).push(Integer.valueOf(0));
                    }
                    String currentDirectory=null;
                    String[] currentList=null;
                    int currentIndex=-1;
                    try {
                        currentDirectory=currentDirectories.peek();
                        currentList=currentLists.peek();
                        currentIndex=(currentIndexes.peek()).intValue();

                        // Undo the stack as far as needed
                        while(currentDirectory!=null && currentIndex>=currentList.length) {
                            currentDirectories.pop();
                            currentDirectory=currentDirectories.peek();
                            currentLists.pop();
                            currentList=currentLists.peek();
                            currentIndexes.pop();
                            currentIndex=currentIndexes.peek();
                        }
                    } catch(EmptyStackException err) {
                        currentDirectory=null;
                    }
                    if(currentDirectory==null) {
                        filesDone=true;
                        fileSetting.clear();
                        return;
                    } else {
                        // Get the current filename
                        String filename;
                        if(currentDirectory.length()==0) filename=currentList[currentIndex++];
                        else if(environment.isFilesystemRoot(currentDirectory)) filename=currentDirectory+currentList[currentIndex++];
                        else filename=currentDirectory+'/'+currentList[currentIndex++];

                        // Set to the next file
                        currentIndexes.pop();
                        currentIndexes.push(currentIndex);
                        try {
                            fileSetting.setFilename(filename);
                            environment.getBackupFileSetting(fileSetting);
                            short backup_level=fileSetting.getBackupLevel().getLevel();
                            boolean recurse=fileSetting.isRecursible();
                            // IMPORTANT: Should still recurse if there are any file backup settings
                            // that require recursion to reach the paths
                            if(!recurse) {
                                List<FileBackupSetting> fbss=fileBackupSettingTable.getRows();
                                for(int c=0;c<fbss.size();c++) {
                                    FileBackupSetting fbs=fbss.get(c);
                                    if(
                                        (
                                            fbs.getBackupLevel().getLevel()>BackupLevel.DO_NOT_BACKUP
                                            || fbs.isRecursible()
                                        ) && fbs.getPath().startsWith(filename)
                                    ) {
                                        recurse=true;
                                        break;
                                    }
                                }
                            }
                            if(backup_level>BackupLevel.DO_NOT_BACKUP || recurse) {
                                if(recurse) {
                                    // Recurse for directories
                                    long statMode=environment.getStatMode(filename);
                                    if(
                                        !UnixFile.isSymLink(statMode)
                                        && UnixFile.isDirectory(statMode)
                                    ) {
                                        // Push on stacks for next level
                                        String[] list=environment.getDirectoryList(filename);
                                        if(list!=null && list.length>0) {
                                            AutoSort.sortStatic(list);
                                            currentDirectories.push(filename);
                                            currentLists.push(list);
                                            currentIndexes.push(0);
                                        }
                                    }
                                }
                                if(backup_level>BackupLevel.DO_NOT_BACKUP) return;
                            }
                        } catch(FileNotFoundException err) {
                            environment.getErrorHandler().reportWarning(err, null);
                        }
                    }
                }
            }
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    private void backupFileSystem() throws IOException, SQLException {
        Profiler.startProfile(Profiler.SLOW, BackupDaemon.class, "backupFileSystem()", null);
        try {
            Random random=environment.getRandom();
            ProcessTimer timer=new ProcessTimer(
                random,
                environment.getWarningSmtpServer(),
                environment.getWarningEmailFrom(),
                environment.getWarningEmailTo(),
                "File backup taking too long",
                "File Backup",
                environment.getWarningTime(),
                environment.getWarningReminderInterval()
            );
            try {
                timer.start();

                // Various statistics are maintained throughout the backup process
                BackupStat backupStat=new BackupStat(environment);
                try {
                    environment.init();
                    try {
                        Server thisServer=environment.getThisServer();
                        AOServConnector conn=environment.getConnector();
                        FileBackupTable fileBackupTable=conn.fileBackups;
                        FileBackupSettingTable fileBackupSettingTable=conn.fileBackupSettings;

                        // Get a list of all the file backup pkeys currently indicated as existing on this server
                        SortedIntArrayList latestFileBackupSet=thisServer.getLatestFileBackupSet();
                        boolean[] latestFileBackupSetCompleteds=new boolean[latestFileBackupSet.size()];

                        final int BATCH_SIZE=environment.getFileBatchSize();

                        // The processing values are stored here
                        String[] filenames=new String[BATCH_SIZE];
                        String[] paths=new String[BATCH_SIZE];
                        FileBackupDevice[] devices=new FileBackupDevice[BATCH_SIZE];
                        long[] inodes=new long[BATCH_SIZE];
                        int[] packageNums=new int[BATCH_SIZE];
                        long[] modes=new long[BATCH_SIZE];
                        int[] uids=new int[BATCH_SIZE];
                        int[] gids=new int[BATCH_SIZE];
                        long[] modify_times=new long[BATCH_SIZE];
                        long[] lengths=new long[BATCH_SIZE];
                        short[] backup_levels=new short[BATCH_SIZE];
                        short[] backup_retentions=new short[BATCH_SIZE];
                        String[] symlink_targets=new String[BATCH_SIZE];
                        long[] device_ids=new long[BATCH_SIZE];
                        String[] prunedPaths=new String[BATCH_SIZE];
                        BackupFileSetting fileSetting=new BackupFileSetting();
                        boolean[] md5Faileds=new boolean[BATCH_SIZE];
                        LongList prunedMD5His=new LongArrayList(BATCH_SIZE);
                        LongList prunedMD5Los=new LongArrayList(BATCH_SIZE);
                        LongList prunedLengths=new LongArrayList(BATCH_SIZE);

                        // Recursively verify each file in the filesystem, adding or modifying
                        resetFilenamePointer();
                        while(true) {
                            long batchStartTime=System.currentTimeMillis();
                            // Get the list of files while building up the attributes
                            int batchSize=0;
                            while(batchSize<BATCH_SIZE) {
                                getNextFilename(fileSetting, fileBackupSettingTable);
                                String filename=filenames[batchSize]=fileSetting.getFilename();
                                if(filename==null) break;
                                backupStat.scanned++;
                                short backup_level=backup_levels[batchSize]=fileSetting.getBackupLevel().getLevel();
                                if(backup_level>0) {
                                    try {
                                        FileBackupDevice dev=devices[batchSize]=environment.getFileBackupDevice(filename);
                                        long mode=environment.getStatMode(filename);
                                        if(dev==null || dev.canBackup() || UnixFile.isDirectory(mode)) {
                                            paths[batchSize]=filename;
                                            inodes[batchSize]=environment.getInode(filename);
                                            packageNums[batchSize]=fileSetting.getPackage().getPKey();
                                            modes[batchSize]=mode;
                                            uids[batchSize]=environment.getUID(filename);
                                            gids[batchSize]=environment.getGID(filename);
                                            modify_times[batchSize]=UnixFile.isRegularFile(mode)?environment.getModifyTime(filename):-1;
                                            lengths[batchSize]=UnixFile.isRegularFile(mode)?environment.getFileLength(filename):-1;
                                            backup_retentions[batchSize]=fileSetting.getBackupRetention().getDays();
                                            symlink_targets[batchSize]=UnixFile.isSymLink(mode)?environment.readLink(filename):null;
                                            device_ids[batchSize]=UnixFile.isBlockDevice(mode)||UnixFile.isCharacterDevice(mode)?environment.getDeviceIdentifier(filename):-1;
                                            batchSize++;
                                        }
                                    } catch(IOException err) {
                                        environment.getErrorHandler().reportError(err, new Object[] {"filename="+filename});
                                        try {
                                            Thread.sleep(1000);
                                        } catch(InterruptedException err2) {
                                            environment.getErrorHandler().reportWarning(err2, null);
                                        }
                                    }
                                }
                            }
                            if(batchSize==0) break;

                            // Look for attribute matches
                            Object[] OA=thisServer.findLatestFileBackupSetAttributeMatches(
                                batchSize,
                                paths,
                                devices,
                                inodes,
                                packageNums,
                                modes,
                                uids,
                                gids,
                                modify_times,
                                backup_levels,
                                backup_retentions,
                                lengths,
                                symlink_targets,
                                device_ids
                            );
                            int[] fileBackups=(int[])OA[0];
                            int[] backupDatas=(int[])OA[1];
                            long[] md5_his=(long[])OA[2];
                            long[] md5_los=(long[])OA[3];
                            boolean[] hasDatas=(boolean[])OA[4];
                            // Count the matches and calculate MD5s for regular files not matched
                            for(int c=0;c<batchSize;c++) {
                                int pkey=fileBackups[c];
                                if(pkey!=-1) {
                                    backupStat.file_backup_attribute_matches++;
                                    int index=latestFileBackupSet.indexOf(pkey);
                                    if(index>=0) latestFileBackupSetCompleteds[index]=true;
                                } else {
                                    // Calculate the MD5 hash and file lengths if it is a regular file
                                    if(UnixFile.isRegularFile(modes[c])) {
                                        try {
                                            MD5InputStream md5In=new MD5InputStream(
                                                new BufferedInputStream(
                                                    environment.getInputStream(filenames[c])
                                                )
                                            );
					    long readLength=0;
					    long statLength=lengths[c];
					    try {
						backupStat.not_matched_md5_files++;
						// Will at most read up to the number of bytes returned by the previous stat.  This
						// is good for files that are continually growing, such as log files.
						byte[] buff=BufferManager.getBytes();
						try {
						    while(readLength<statLength) {
							long bytesLeft=statLength-readLength;
							if(bytesLeft>BufferManager.BUFFER_SIZE) bytesLeft=BufferManager.BUFFER_SIZE;
							int ret=md5In.read(buff, 0, (int)bytesLeft);
							if(ret==-1) break;
							readLength+=ret;
						    }
						} finally {
						    BufferManager.release(buff);
						}
					    } finally {
						md5In.close();
					    }
                                            // If the file is smaller than the statLength, then it was modified and needs to be copied later
                                            // a null md5 indicates that a copy is required later
                                            if(readLength==statLength) {
                                                md5Faileds[c]=false;
                                                byte[] md5=md5In.hash();
                                                md5_his[c]=MD5.getMD5Hi(md5);
                                                md5_los[c]=MD5.getMD5Lo(md5);
                                            } else {
                                                md5Faileds[c]=true;
                                                backupStat.not_matched_md5_failures++;
                                            }
                                        } catch(FileNotFoundException err) {
                                            // Normal when the file is deleted during the backup
                                            paths[c]=null;
                                        }
                                    }
                                }
                            }

                            // Find or add backup_data
                            prunedMD5His.clear();
                            prunedMD5Los.clear();
                            prunedLengths.clear();
                            for(int c=0;c<batchSize;c++) {
                                if(
                                    paths[c]!=null
                                    && fileBackups[c]==-1
                                    && UnixFile.isRegularFile(modes[c])
                                    && !md5Faileds[c]
                                ) {
                                    prunedMD5His.add(md5_his[c]);
                                    prunedMD5Los.add(md5_los[c]);
                                    prunedLengths.add(lengths[c]);
                                }
                            }
                            OA=thisServer.findOrAddBackupDatas(
                                prunedMD5His.size(),
                                prunedLengths.toArrayLong(),
                                prunedMD5His.toArrayLong(),
                                prunedMD5Los.toArrayLong()
                            );
                            int[] moreBackupDatas=(int[])OA[0];
                            boolean[] moreHasDatas=(boolean[])OA[1];
                            int pos=0;
                            for(int c=0;c<batchSize;c++) {
                                if(
                                    paths[c]!=null
                                    && fileBackups[c]==-1
                                    && UnixFile.isRegularFile(modes[c])
                                    && !md5Faileds[c]
                                ) {
                                    backupDatas[c]=moreBackupDatas[pos];
                                    hasDatas[c]=moreHasDatas[pos++];
                                }
                            }

                            // Send missing backup data
                        Loop:
                            for(int c=0;c<batchSize;c++) {
                                int backupData=backupDatas[c];
                                if(
                                    paths[c]!=null
                                    && backupData!=-1
                                    && !hasDatas[c]
                                ) {
                                    // Look for already added in this batch
                                    boolean alreadyAdded=false;
                                    for(int d=0;d<c;d++) {
                                        if(
                                            backupDatas[d]==backupData
                                        ) {
                                            alreadyAdded=true;
                                            hasDatas[c]=hasDatas[d];
                                            break;
                                        }
                                    }
                                    if(!alreadyAdded) {
                                        String tempFilename=environment.getNameOfFile(filenames[c]);
                                        int retries=0;
                                        boolean committed=false;
                                        while(retries<environment.getRetryCount()) {
                                            retries++;
                                            try {
                                                InputStream in=environment.getInputStream(filenames[c]);
						try {
						    committed=environment.sendData(
                                                        conn,
                                                        backupDatas[c],
                                                        in,
                                                        tempFilename,
                                                        lengths[c],
                                                        md5_his[c],
                                                        md5_los[c],
                                                        false,
                                                        !FileBackup.isCompressedExtension(UnixFile.getExtension(tempFilename))
                                                    );
						} finally {
						    in.close();
						}
                                                break;
                                            } catch(FileNotFoundException err) {
                                                // Normal if the file was removed during the backup process
                                                paths[c]=null;
                                                continue Loop;
                                            } catch(IOException err) {
                                                if(retries<environment.getRetryCount()) {
                                                    environment.getErrorHandler().reportWarning(
                                                        err,
                                                        new Object[] {"retries="+retries, "environment.getRetryCount()="+environment.getRetryCount()}
                                                    );
                                                } else throw err;
                                            } catch(SQLException err) {
                                                if(retries<environment.getRetryCount()) {
                                                    environment.getErrorHandler().reportWarning(
                                                        err,
                                                        new Object[] {"retries="+retries, "environment.getRetryCount()="+environment.getRetryCount()}
                                                    );
                                                } else throw err;
                                            }
                                            try {
                                                Thread.sleep(environment.getRetryDelay());
                                            } catch(InterruptedException err) {
                                                environment.getErrorHandler().reportWarning(err, null);
                                            }
                                        }
                                        backupStat.send_missing_backup_data_files++;
                                        if(committed) hasDatas[c]=true;
                                        else backupStat.send_missing_backup_data_failures++;
                                    }
                                }
                            }

                            // Delete now unused backup data
                            for(int c=0;c<batchSize;c++) {
                                if(
                                    paths[c]!=null
                                    && backupDatas[c]!=-1
                                    && !hasDatas[c]
                                ) {
                                    backupDatas[c]=-1;
                                }
                            }

                            // Make a copy of the file recalculating the length and md5 and resend the data
                            for(int c=0;c<batchSize;c++) {
                                if(
                                    paths[c]!=null
                                    && UnixFile.isRegularFile(modes[c])
                                    && !hasDatas[c]
                                ) {
                                    backupStat.temp_files++;
                                    String filename=environment.getNameOfFile(filenames[c]);
                                    String extension=UnixFile.getExtension(filename);
                                    boolean shouldCompress=!FileBackup.isCompressedExtension(extension);
                                    File tempFile=FileList.getTempFile(filename, shouldCompress?"gz":null);
                                    try {
					long lengthCopied=0;
                                        MD5InputStream md5In=new MD5InputStream(
                                            new BufferedInputStream(
                                                environment.getInputStream(filenames[c])
                                            )
                                        );
					try {
					    OutputStream out=
						shouldCompress?(OutputStream)new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(tempFile)))
						    :new BufferedOutputStream(new FileOutputStream(tempFile))
							;
					    try {
						byte[] buff=BufferManager.getBytes();
						try {
						    int ret;
						    while((ret=md5In.read(buff, 0, BufferManager.BUFFER_SIZE))!=-1) {
							out.write(buff, 0, ret);
							lengthCopied+=ret;
						    }
						} finally {
						    BufferManager.release(buff);
						}
					    } finally {
						out.flush();
						out.close();
					    }
					} finally {
					    md5In.close();
					}

                                        byte[] md5=md5In.hash();
                                        long md5_hi=md5_his[c]=MD5.getMD5Hi(md5);
                                        long md5_lo=md5_los[c]=MD5.getMD5Lo(md5);
                                        lengths[c]=lengthCopied;

                                        // Find existing backup data that matches our snapshot copy of the file
                                        OA=thisServer.findOrAddBackupData(
                                            lengthCopied,
                                            md5_hi,
                                            md5_lo
                                        );
                                        backupDatas[c]=((Integer)OA[0]).intValue();
                                        hasDatas[c]=((Boolean)OA[1]).booleanValue();

                                        if(!hasDatas[c]) {
                                            int retries=0;
                                            boolean committed=false;
                                            while(retries<environment.getRetryCount()) {
                                                retries++;
                                                try {
                                                    InputStream in=new FileInputStream(tempFile);
						    try {
							committed=environment.sendData(
                                                            conn,
                                                            backupDatas[c],
                                                            in,
                                                            filename,
                                                            lengthCopied,
                                                            md5_hi,
                                                            md5_lo,
                                                            shouldCompress,
                                                            shouldCompress
                                                        );
						    } finally {
							in.close();
						    }
                                                    break;
                                                } catch(IOException err) {
                                                    if(retries<environment.getRetryCount()) {
                                                        environment.getErrorHandler().reportWarning(
                                                            err,
                                                            new Object[] {"retries="+retries, "environment.getRetryCount()="+environment.getRetryCount()}
                                                        );
                                                    } else throw err;
                                                } catch(SQLException err) {
                                                    if(retries<environment.getRetryCount()) {
                                                        environment.getErrorHandler().reportWarning(
                                                            err,
                                                            new Object[] {"retries="+retries, "environment.getRetryCount()="+environment.getRetryCount()}
                                                        );
                                                    } else throw err;
                                                }
                                                try {
                                                    Thread.sleep(environment.getRetryDelay());
                                                } catch(InterruptedException err) {
                                                    environment.getErrorHandler().reportWarning(err, null);
                                                }
                                            }
                                            backupStat.temp_send_backup_data_files++;

                                            if(committed) hasDatas[c]=true;
                                            else {
                                                backupStat.temp_failures++;
                                                paths[c]=null;
                                                throw new IOException("The temp copy used to backup a changing file was changed, unable to backup.  Source file: "+filenames[c]+", temp file: "+tempFile.getPath());
                                            }
                                        }
                                    } catch(FileNotFoundException err) {
                                        // Normal if file is removed during the backup process
                                        paths[c]=null;
                                    } finally {
                                        if(!tempFile.delete()) throw new IOException("Unable to delete temp file: "+tempFile.getPath());
                                    }
                                }
                            }

                            // Delete now unused backup data
                            for(int c=0;c<batchSize;c++) {
                                if(
                                    backupDatas[c]!=-1
                                    && !hasDatas[c]
                                ) {
                                    backupDatas[c]=-1;
                                }
                            }

                            // Add all of the new file backups in one transaction
                            for(int c=0;c<batchSize;c++) {
                                if(paths[c]!=null && fileBackups[c]==-1) {
                                    backupStat.added++;
                                    prunedPaths[c]=paths[c];
                                } else prunedPaths[c]=null;
                            }
                            int[] moreFileBackups=thisServer.addFileBackups(
                                batchSize,
                                prunedPaths,
                                devices,
                                inodes,
                                packageNums,
                                modes,
                                uids,
                                gids,
                                backupDatas,
                                md5_his,
                                md5_los,
                                modify_times,
                                backup_levels,
                                backup_retentions,
                                symlink_targets,
                                device_ids
                            );
                            for(int c=0;c<batchSize;c++) {
                                if(paths[c]!=null && fileBackups[c]==-1) {
                                    fileBackups[c]=moreFileBackups[c];
                                }
                            }

                            // Sleep for the amount of time it took to process the batch of files
                            /*
                            long sleepyTime=(long)((System.currentTimeMillis()-batchStartTime)/(1.5f+random.nextFloat()));
                            long maxTime=environment.getMaximumBatchSleepTime();
                            if(sleepyTime<0 || sleepyTime>maxTime) sleepyTime=maxTime;
                            if(sleepyTime>0) {
                                try {
                                    Thread.sleep(sleepyTime);
                                } catch(InterruptedException err) {
                                    environment.getErrorHandler().reportWarning(err, null);
                                }
                            }*/
                        }

                        // Flag those not found as deleted
                        int size=latestFileBackupSet.size();
                        final int DELETE_BATCH_SIZE=environment.getDeleteBatchSize();
                        int[] pkeys=new int[DELETE_BATCH_SIZE];
                        int pos=0;
                        for(int c=0;c<size;c++) {
                            if(!latestFileBackupSetCompleteds[c]) {
                                pkeys[pos++]=latestFileBackupSet.getInt(c);
                                if(pos>=DELETE_BATCH_SIZE) {
                                    fileBackupTable.flagFileBackupsAsDeleted(pos, pkeys);
                                    backupStat.deleted+=pos;
                                    pos=0;
                                }
                            }
                        }
                        if(pos>0) {
                            fileBackupTable.flagFileBackupsAsDeleted(pos, pkeys);
                            backupStat.deleted+=pos;
                        }

                        backupStat.is_successful=true;
                    } finally {
                        environment.cleanup();
                    }
                } catch(RuntimeException err) {
                    environment.getErrorHandler().reportError(err, new Object[] {"Caught exception, commiting backup stats."});
                    throw err;
                } catch(IOException err) {
                    environment.getErrorHandler().reportError(err, new Object[] {"Caught exception, commiting backup stats."});
                    throw err;
                } catch(SQLException err) {
                    environment.getErrorHandler().reportError(err, new Object[] {"Caught exception, commiting backup stats."});
                    throw err;
                } finally {
                    backupStat.commit();
                }
            } finally {
                timer.stop();
            }
        } finally {
            Profiler.endProfile(Profiler.SLOW);
        }
    }

    public void removeExpiredBackupFiles() throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, BackupDaemon.class, "removeExpiredBackupFiles()", null);
        try {
            environment.getThisServer().removeExpiredFileBackups();
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void run() {
        Profiler.startProfile(Profiler.UNKNOWN, BackupDaemon.class, "run()", null);
        try {
            while(true) {
                try {
                    while(true) {
                        Server thisServer=environment.getThisServer();
                        long lastBackupTime=thisServer.getLastBackupTime();
                        if(lastBackupTime!=-1) {
                            try {
                                Thread.sleep(environment.getInitialSleepTime());
                            } catch(InterruptedException err) {
                                environment.getErrorHandler().reportWarning(err, null);
                            }
                            thisServer=environment.getThisServer();
                            lastBackupTime=thisServer.getLastBackupTime();
                        }
                        
                        // It is time to run if it is the backup hour and the backup has not been run for at least 2 hours
                        long backupStartTime=System.currentTimeMillis();
                        if(lastBackupTime==-1 || lastBackupTime>backupStartTime || (backupStartTime-lastBackupTime)>=2L*60*60*1000) {
                            if(lastBackupTime==-1) runNow();
                            else {
                                int backupHour=thisServer.getBackupHour();
                                Calendar cal=Calendar.getInstance();
                                cal.setTimeInMillis(backupStartTime);
                                if(cal.get(Calendar.HOUR_OF_DAY)==backupHour) runNow();
                            }
                        }
                    }
                } catch(ThreadDeath TD) {
                    throw TD;
                } catch(Throwable T) {
                    environment.getErrorHandler().reportError(T, null);
                    try {
                        Thread.sleep(environment.getMaximumBatchSleepTime());
                    } catch(InterruptedException err) {
                        environment.getErrorHandler().reportWarning(err, null);
                    }
                }
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public void runNow() throws IOException, SQLException {
        environment.getThisServer().setLastBackupTime(System.currentTimeMillis());

        environment.preBackup();

        backupFileSystem();
        removeExpiredBackupFiles();

        environment.postBackup();
    }

    private static boolean started=false;

    public static void start(BackupEnvironment environment) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupDaemon.class, "start(BackupEnvironment)", null);
        try {
            if(!started) {
                synchronized(System.out) {
                    if(!started) {
                        System.out.print("Starting BackupDaemon: ");
                        new Thread(new BackupDaemon(environment)).start();
                        System.out.println("Done");
                        started=true;
                    }
                }
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Runs the standalone <code>BackupDaemon</code> with the values
     * provided in <code>com/aoindustries/aoserv/backuo/aoserv-backup.properties</code>.
     * This will typically be called by the init scripts of the dedicated machine.
     */
    public static void main(String[] args) {
        // Not profiled because the profiler is not enabled here
        if(args.length<1 || args.length>2) {
            try {
                showUsage();
                System.exit(1);
            } catch(IOException err) {
                ErrorPrinter.printStackTraces(err);
                System.exit(5);
            }
            return;
        } else {
            if(args[0].equals("daemon") || args[0].equals("run")) {
                // Load the environment class as provided on the command line
                BackupEnvironment environment;
                if(args.length>1) {
                    try {
                        environment=(BackupEnvironment)Class.forName(args[1]).newInstance();
                    } catch(ClassNotFoundException err) {
                        ErrorPrinter.printStackTraces(err, new Object[] {"environment="+args[1]});
                        System.exit(2);
                        return;
                    } catch(InstantiationException err) {
                        ErrorPrinter.printStackTraces(err, new Object[] {"environment_classname="+args[1]});
                        System.exit(3);
                        return;
                    } catch(IllegalAccessException err) {
                        ErrorPrinter.printStackTraces(err, new Object[] {"environment_classname="+args[1]});
                        System.exit(4);
                        return;
                    }
                } else {
                    // Try to pick the correct class based on the System properties
                    if(System.getProperty("os.name").equalsIgnoreCase("Linux")) {
                        environment=new UnixEnvironment();
                    } else {
                        environment=new JavaEnvironment();
                    }
                }

                if(args[0].equals("daemon")) {
                    boolean done=false;
                    while(!done) {
                        try {
                            // Start up the daemon
                            BackupDaemon.start(environment);
                            done=true;
                        } catch (ThreadDeath TD) {
                            throw TD;
                        } catch (Throwable T) {
                            environment.getErrorHandler().reportError(T, null);
                            try {
                                Thread.sleep(60000);
                            } catch(InterruptedException err) {
                                environment.getErrorHandler().reportWarning(err, null);
                            }
                        }
                    }
                } else if(args[0].equals("run")) {
                    try {
                        new BackupDaemon(environment).runNow();
                    } catch(IOException err) {
                        environment.getErrorHandler().reportError(err, null);
                        System.exit(5);
                        return;
                    } catch(SQLException err) {
                        environment.getErrorHandler().reportError(err, null);
                        System.exit(6);
                        return;
                    }
                } else throw new RuntimeException("Unknown value for args[0]: "+args[0]);
            } else {
                try {
                    showUsage();
                    System.exit(1);
                } catch(IOException err) {
                    ErrorPrinter.printStackTraces(err);
                    System.exit(5);
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
        out.println("\t"+BackupDaemon.class.getName()+" {daemon|run} [environment_classname]");
        out.println();
        out.boldOn();
        out.print("DESCRIPTION");
        out.attributesOff();
        out.println();
        out.println("\tLaunches the backup system in either \"run\" mode or \"daemon\" mode.  In \"run\" mode the");
        out.println("\tbackup will begin immediately and the process will exit when the backup completes.");
        out.println("\tIn \"daemon\" mode, the process will continue indefinitely while backing-up files once");
        out.println("\tper day at the time configured in the servers table.");
        out.println();
        out.println("\tAn environment_classname may be provided.  If provided, this must be the fully");
        out.println("\tqualified class name of a "+BackupEnvironment.class.getName()+".");
        out.println("\tOne instance of this class will be created via the default constructor.  If no");
        out.println("\tenvironment_classname is provided, an attempt is made to automatically select");
        out.println("\tthe appropriate class.");
        out.println();
        out.flush();
    }
}
