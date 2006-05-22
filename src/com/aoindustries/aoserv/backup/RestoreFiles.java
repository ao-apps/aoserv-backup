package com.aoindustries.aoserv.backup;

/*
 * Copyright 2001-2006 by AO Industries, Inc.,
 * 2200 Dogwood Ct N, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.*;
import com.aoindustries.io.*;
import com.aoindustries.io.unix.*;
import com.aoindustries.profiler.*;
import com.aoindustries.sql.*;
import com.aoindustries.util.*;
import java.io.*;
import java.sql.*;
import java.util.*;

public class RestoreFiles {

    public static final int BATCH_SIZE=1000;

    public static void main(String[] args) {
        Profiler.startProfile(Profiler.UNKNOWN, RestoreFiles.class, "main(String[])", null);
        try {
            TerminalWriter out=new TerminalWriter(System.out);
            TerminalWriter err=new TerminalWriter(System.err);
            if(args.length==3 || args.length==4) {
                try {
                    String username=AOSH.getConfigUsername(System.in, out);
                    String password=AOSH.getConfigPassword(System.in, out);
                    AOServConnector conn=AOServConnector.getConnector(username, password, new StandardErrorHandler());
                    String server=args[0];
                    String path=args[1];
                    String destinationDirectory=args[2];
                    long time;
                    if(args.length==4) time=SQLUtility.getDate(args[3]).getTime();
                    else time=-1;

                    restoreFiles(out, err, conn, server, path, destinationDirectory, time);
                    System.exit(0);
                } catch(IOException exc) {
                    ErrorPrinter.printStackTraces(exc, err);
                    err.flush();
                    System.exit(2);
                } catch(SQLException exc) {
                    ErrorPrinter.printStackTraces(exc, err);
                    err.flush();
                    System.exit(3);
                }
            } else {
                err.println("usage: "+RestoreFiles.class.getName()+" <server> <path> <destination_root> [<backup_date>]");
                err.flush();
                System.exit(1);
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    
    public static void restoreFiles(TerminalWriter out, TerminalWriter err, AOServConnector conn, String server, String restorePath, String destinationDirectory, long time) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, RestoreFiles.class, "restoreFiles(TerminalWriter,TerminalWriter,AOServConnector,String,String,String,long)", null);
        try {
            // Get a few objects first
            BackupDataTable backupDataTable=conn.backupDatas;
            FileBackupTable fileBackupTable=conn.fileBackups;

            // Locate the server
            Server se=conn.servers.get(server);
            if(se==null) throw new SQLException("Unable to find Server: "+server);

            // Get a list of the files
            IntsAndBooleans pkeysAndIsDirectories=se.getFileBackupSet(restorePath, time);

            // Restore the files
            int[] pkeys=new int[BATCH_SIZE];
            List<FileBackup> fileBackups=new ArrayList<FileBackup>(BATCH_SIZE);
            BackupData[] backupDatas=new BackupData[BATCH_SIZE];
            int len=pkeysAndIsDirectories.size();
            for(int c=0;c<len;c+=BATCH_SIZE) {
                int batchSize=0;
                for(;batchSize<BATCH_SIZE;batchSize++) {
                    int index=c+batchSize;
                    if(index<len) pkeys[batchSize]=pkeysAndIsDirectories.getInt(index);
                    else break;
                }
                
                // Get the FileBackup for each pkey
                fileBackups.clear();
                fileBackupTable.getFileBackups(batchSize, pkeys, fileBackups);

                // Get the BackupDatas for each pkey
                backupDataTable.getBackupData(batchSize, fileBackups, backupDatas);
                
                // Restore each of the files and their contents
                for(int d=0;d<batchSize;d++) {
                    FileBackup fb=fileBackups.get(d);
                    if(fb!=null) {
                        BackupData bd=backupDatas[d];

                        String path=fb.getPath();
                        out.print(path);
                        try {
                            if(bd!=null) {
                                out.print(' ');
                                out.print(bd.getDataSize());
                                long compressedSize=bd.getCompressedSize();
                                if(compressedSize!=-1) {
                                    out.print(" (");
                                    out.print(compressedSize);
                                    out.print(')');
                                }
                            }
                            out.println();
                            out.flush();

                            String destPath=destinationDirectory+path;
                            UnixFile uf=new UnixFile(destPath);
                            if(!uf.exists()) {
                                long mode=fb.getMode();
                                if(UnixFile.isRegularFile(mode)) {
                                    if(bd!=null) {
                                        OutputStream fileOut=new FileOutputStream(destPath);
                                        try {
                                            try {
                                                chown(fb, uf);
                                                uf.setMode(mode);
                                                bd.getData(fileOut, bd.getCompressedSize()!=-1, 0, null);
                                            } finally {
						fileOut.flush();
                                                fileOut.close();
                                            }
                                        } catch(IOException exc) {
                                            uf.delete();
                                            throw exc;
                                        } catch(SQLException exc) {
                                            uf.delete();
                                            throw exc;
                                        }
                                        uf.setModifyTime(fb.getModifyTime());
                                    } else {
                                        err.println(path+" - BackupData not found");
                                        err.flush();
                                    }
                                } else if(UnixFile.isDirectory(mode)) {
                                    uf.mkdir(true, 0755);
                                    chown(fb, uf);
                                    uf.setMode(mode);
                                } else if(UnixFile.isSymLink(mode)) {
                                    uf.symLink(fb.getSymLinkTarget());
                                    chown(fb, uf);
                                } else if(UnixFile.isBlockDevice(mode) || UnixFile.isCharacterDevice(mode)) {
                                    uf.mknod(mode, fb.getDeviceID());
                                    chown(fb, uf);
                                } else if(UnixFile.isFIFO(mode)) {
                                    uf.mkfifo(mode);
                                    chown(fb, uf);
                                } else if(UnixFile.isSocket(mode)) {
                                    // Ignore, created by the processes as they start
                                } else {
                                    err.println(path+" - Unknown mode: "+mode);
                                    err.flush();
                                }
                            } else {
                                err.println(path+" - Already exists");
                                err.flush();
                            }
                        } catch(IOException exc) {
                            err.println(path+" - IOException: "+exc.toString());
                            err.flush();
                        } catch(SQLException exc) {
                            err.println(path+" - SQLException: "+exc.toString());
                            err.flush();
                        }
                    } else {
                        err.println(pkeys[d]+" - FileBackup not found");
                        err.flush();
                    }
                }
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    
    private static void chown(FileBackup fb, UnixFile uf) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, RestoreFiles.class, "chown(FileBackup,UnixFile)", null);
        try {
            uf.chown(
                fb.getUID().getID(),
                fb.getGID().getID()
            );
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }
}
