/**
 * Copyright (C) 2008 Happy Fish / YuQing
 * <p>
 * FastDFS Java Client may be copied only under the terms of the GNU Lesser
 * General Public License (LGPL).
 * Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
 */

package org.csource.fastdfs;

import org.csource.common.Base64;
import org.csource.common.FastDFSClientException;
import org.csource.common.NameValuePair;

import java.io.*;
import java.net.Socket;
import java.util.Arrays;

/**
 * Storage client for 2 fields file id: group name and filename
 *
 * @author Happy Fish / YuQing
 * @version Version 1.24
 */
public class StorageClient {
    /**
     * Upload file by file buff
     *
     * @author Happy Fish / YuQing
     * @version Version 1.12
     */
    public static class UploadBuff implements UploadCallback {
        private byte[] fileBuff;
        private int offset;
        private int length;

        /**
         * constructor
         *
         * @param fileBuff the file buff for uploading
         */
        public UploadBuff(byte[] fileBuff, int offset, int length) {
            super();
            this.fileBuff = fileBuff;
            this.offset = offset;
            this.length = length;
        }

        /**
         * send file content callback function, be called only once when the file uploaded
         *
         * @param out output stream for writing file content
         * @return 0 success, return none zero(errno) if fail
         */
        public int send(OutputStream out) throws IOException {
            out.write(this.fileBuff, this.offset, this.length);

            return 0;
        }
    }

    public final static Base64 base64 = new Base64('-', '_', '.', 0);
    protected TrackerServer trackerServer;
    protected StorageServer storageServer;
    protected byte errno;

    /**
     * constructor using global settings in class ClientGlobal
     */
    public StorageClient() {
        this.trackerServer = null;
        this.storageServer = null;
    }

    /**
     * constructor with tracker server and storage server
     *
     * @param trackerServer the tracker server, can be null
     */
    public StorageClient(TrackerServer trackerServer) {
        this(trackerServer, null);
    }

    /**
     * constructor with tracker server and storage server
     *
     * @param trackerServer the tracker server, can be null
     * @param storageServer the storage server, can be null
     */
    public StorageClient(TrackerServer trackerServer, StorageServer storageServer) {
        this.trackerServer = trackerServer;
        this.storageServer = storageServer;
    }

    /**
     * get the error code of last call
     *
     * @return the error code of last call
     */
    public byte getErrorCode() {
        return this.errno;
    }

    /**
     * upload file to storage server (by file name)
     *
     * @param localFilename local filename to upload
     * @param fileExtName   file ext name, do not include dot(.), null to extract ext name from the local filename
     * @param metaList      meta info array
     * @return 2 elements string array if success:<br>
     * <ul><li>results[0]: the group name to store the file </li></ul>
     * <ul><li>results[1]: the new created filename</li></ul>
     * return null if fail
     */
    public String[] upload_file(String localFilename, String fileExtName, NameValuePair[] metaList) throws IOException, FastDFSClientException {
        return upload_file(null, localFilename, fileExtName, metaList);
    }

    /**
     * upload file to storage server (by file name)
     *
     * @param groupName     the group name to upload file to, can be empty
     * @param localFilename local filename to upload
     * @param fileExtName   file ext name, do not include dot(.), null to extract ext name from the local filename
     * @param metaList      meta info array
     * @return 2 elements string array if success:<br>
     * <ul><li>results[0]: the group name to store the file </li></ul>
     * <ul><li>results[1]: the new created filename</li></ul>
     * return null if fail
     */
    protected String[] upload_file(String groupName, String localFilename, String fileExtName,
                                   NameValuePair[] metaList) throws IOException, FastDFSClientException {
        return upload_file(ProtoCommon.STORAGE_PROTO_CMD_UPLOAD_FILE, groupName, localFilename, fileExtName, metaList);
    }

    /**
     * upload file to storage server (by file name)
     *
     * @param cmd           the command
     * @param groupName     the group name to upload file to, can be empty
     * @param localFilename local filename to upload
     * @param fileExtName   file ext name, do not include dot(.), null to extract ext name from the local filename
     * @param metaList      meta info array
     * @return 2 elements string array if success:<br>
     * <ul><li>results[0]: the group name to store the file </li></ul>
     * <ul><li>results[1]: the new created filename</li></ul>
     * return null if fail
     */
    protected String[] upload_file(byte cmd, String groupName, String localFilename, String fileExtName, NameValuePair[] metaList) throws IOException, FastDFSClientException {
        if (fileExtName == null) {
            fileExtName = getExtension(localFilename);
        }
        File file = new File(localFilename);
        long length = file.length();
        FileInputStream fileInputStream = new FileInputStream(file);
        UploadStream uploadStream = new UploadStream(fileInputStream, length);
        try {
            return do_upload_file(cmd, groupName, null, null, fileExtName, length, uploadStream, metaList);
        } finally {
            fileInputStream.close();
        }
    }

    /**
     * upload file to storage server (by file buff)
     *
     * @param fileBuff    file content/buff
     * @param offset      start offset of the buff
     * @param length      the length of buff to upload
     * @param fileExtName file ext name, do not include dot(.)
     * @param metaList    meta info array
     * @return 2 elements string array if success:<br>
     * <ul><li>results[0]: the group name to store the file</li></ul>
     * <ul><li>results[1]: the new created filename</li></ul>
     * return null if fail
     */
    public String[] upload_file(byte[] fileBuff, int offset, int length, String fileExtName,
                                NameValuePair[] metaList) throws IOException, FastDFSClientException {
        return upload_file(null, fileBuff, offset, length, fileExtName, metaList);
    }

    /**
     * upload file to storage server (by file buff)
     *
     * @param groupName   the group name to upload file to, can be empty
     * @param fileBuff    file content/buff
     * @param offset      start offset of the buff
     * @param length      the length of buff to upload
     * @param fileExtName file ext name, do not include dot(.)
     * @param metaList    meta info array
     * @return 2 elements string array if success:<br>
     * <ul><li>results[0]: the group name to store the file</li></ul>
     * <ul><li>results[1]: the new created filename</li></ul>
     * return null if fail
     */
    public String[] upload_file(String groupName, byte[] fileBuff, int offset, int length,
                                String fileExtName, NameValuePair[] metaList) throws IOException, FastDFSClientException {
        UploadBuff uploadBuff = new UploadBuff(fileBuff, offset, length);
        return do_upload_file(ProtoCommon.STORAGE_PROTO_CMD_UPLOAD_FILE, groupName, null, null, fileExtName,
                length, uploadBuff, metaList);
    }

    /**
     * upload file to storage server (by file buff)
     *
     * @param fileBuff    file content/buff
     * @param fileExtName file ext name, do not include dot(.)
     * @param metaList    meta info array
     * @return 2 elements string array if success:<br>
     * <ul><li>results[0]: the group name to store the file</li></ul>
     * <ul><li>results[1]: the new created filename</li></ul>
     * return null if fail
     */
    public String[] upload_file(byte[] fileBuff, String fileExtName, NameValuePair[] metaList) throws IOException, FastDFSClientException {
        return upload_file(null, fileBuff, 0, fileBuff.length, fileExtName, metaList);
    }

    /**
     * upload file to storage server (by file buff)
     *
     * @param groupName   the group name to upload file to, can be empty
     * @param fileBuff    file content/buff
     * @param fileExtName file ext name, do not include dot(.)
     * @param metaList    meta info array
     * @return 2 elements string array if success:<br>
     * <ul><li>results[0]: the group name to store the file</li></ul>
     * <ul><li>results[1]: the new created filename</li></ul>
     * return null if fail
     */
    public String[] upload_file(String groupName, byte[] fileBuff,
                                String fileExtName, NameValuePair[] metaList) throws IOException, FastDFSClientException {
        int length = fileBuff.length;
        UploadBuff uploadBuff = new UploadBuff(fileBuff, 0, length);
        return do_upload_file(ProtoCommon.STORAGE_PROTO_CMD_UPLOAD_FILE, groupName, null, null, fileExtName, length, uploadBuff, metaList);
    }


    /**
     * upload file to storage server (by callback)
     *
     * @param groupName   the group name to upload file to, can be empty
     * @param fileSize    the file size
     * @param callback    the write data callback object
     * @param fileExtName file ext name, do not include dot(.)
     * @param metaList    meta info array
     * @return 2 elements string array if success:<br>
     * <ul><li>results[0]: the group name to store the file</li></ul>
     * <ul><li>results[1]: the new created filename</li></ul>
     * return null if fail
     */
    public String[] upload_file(String groupName, long fileSize, UploadCallback callback,
                                String fileExtName, NameValuePair[] metaList) throws IOException, FastDFSClientException {
        return do_upload_file(ProtoCommon.STORAGE_PROTO_CMD_UPLOAD_FILE, groupName, null, null, fileExtName, fileSize, callback, metaList);
    }

    /**
     * upload file to storage server (by file name, slave file mode)
     *
     * @param groupName      the group name of master file
     * @param masterFilename the master file name to generate the slave file
     * @param prefixName     the prefix name to generate the slave file
     * @param localFileName  local filename to upload
     * @param fileExtName    file ext name, do not include dot(.), null to extract ext name from the local filename
     * @param metaList       meta info array
     * @return 2 elements string array if success:<br>
     * <ul><li>results[0]: the group name to store the file </li></ul>
     * <ul><li>results[1]: the new created filename</li></ul>
     * return null if fail
     */
    public String[] upload_file(String groupName, String masterFilename, String prefixName,
                                String localFileName, String fileExtName, NameValuePair[] metaList) throws IOException, FastDFSClientException {
        if ((groupName == null || groupName.length() == 0) ||
                (masterFilename == null || masterFilename.length() == 0) ||
                (prefixName == null)) {
            throw new FastDFSClientException("invalid argument");
        }
        if (fileExtName == null) {
            fileExtName = getExtension(localFileName);
        }
        File file = new File(localFileName);
        long length = file.length();
        FileInputStream fileInputStream = new FileInputStream(file);
        UploadStream uploadStream = new UploadStream(fileInputStream, length);
        try {
            return do_upload_file(ProtoCommon.STORAGE_PROTO_CMD_UPLOAD_SLAVE_FILE, groupName, masterFilename, prefixName,
                    fileExtName, length, uploadStream, metaList);
        } finally {
            fileInputStream.close();
        }
    }

    private String getExtension(String fileName) {
        int nPos = fileName.lastIndexOf('.');
        if (nPos > 0 && fileName.length() - nPos <= ProtoCommon.FDFS_FILE_EXT_NAME_MAX_LEN + 1) {
            return fileName.substring(nPos + 1);
        }
        return null;
    }

    /**
     * upload file to storage server (by file buff, slave file mode)
     *
     * @param groupName      the group name of master file
     * @param masterFilename the master file name to generate the slave file
     * @param prefixName     the prefix name to generate the slave file
     * @param fileBuff       file content/buff
     * @param fileExtName    file ext name, do not include dot(.)
     * @param metaList       meta info array
     * @return 2 elements string array if success:<br>
     * <ul><li>results[0]: the group name to store the file</li></ul>
     * <ul><li>results[1]: the new created filename</li></ul>
     * return null if fail
     */
    public String[] upload_file(String groupName, String masterFilename, String prefixName,
                                byte[] fileBuff, String fileExtName, NameValuePair[] metaList) throws IOException, FastDFSClientException {
        if ((groupName == null || groupName.length() == 0) ||
                (masterFilename == null || masterFilename.length() == 0) || (prefixName == null)) {
            throw new FastDFSClientException("invalid argument");
        }
        return do_upload_file(ProtoCommon.STORAGE_PROTO_CMD_UPLOAD_SLAVE_FILE, groupName, masterFilename, prefixName,
                fileExtName, fileBuff.length, new UploadBuff(fileBuff, 0, fileBuff.length), metaList);
    }

    /**
     * upload file to storage server (by file buff, slave file mode)
     *
     * @param groupName      the group name of master file
     * @param masterFilename the master file name to generate the slave file
     * @param prefixName     the prefix name to generate the slave file
     * @param fileBuff       file content/buff
     * @param offset         start offset of the buff
     * @param length         the length of buff to upload
     * @param fileExtName    file ext name, do not include dot(.)
     * @param metaList       meta info array
     * @return 2 elements string array if success:<br>
     * <ul><li>results[0]: the group name to store the file</li></ul>
     * <ul><li>results[1]: the new created filename</li></ul>
     * return null if fail
     */
    public String[] upload_file(String groupName, String masterFilename, String prefixName,
                                byte[] fileBuff, int offset, int length, String fileExtName,
                                NameValuePair[] metaList) throws IOException, FastDFSClientException {
        if ((groupName == null || groupName.length() == 0) ||
                (masterFilename == null || masterFilename.length() == 0) || (prefixName == null)) {
            throw new FastDFSClientException("invalid argument");
        }

        return do_upload_file(ProtoCommon.STORAGE_PROTO_CMD_UPLOAD_SLAVE_FILE, groupName, masterFilename, prefixName,
                fileExtName, length, new UploadBuff(fileBuff, offset, length), metaList);
    }

    /**
     * upload file to storage server (by callback, slave file mode)
     *
     * @param groupName      the group name to upload file to, can be empty
     * @param masterFilename the master file name to generate the slave file
     * @param prefixName     the prefix name to generate the slave file
     * @param fileSize       the file size
     * @param callback       the write data callback object
     * @param fileExtName    file ext name, do not include dot(.)
     * @param metaList       meta info array
     * @return 2 elements string array if success:<br>
     * <ul><li>results[0]: the group name to store the file</li></ul>
     * <ul><li>results[1]: the new created filename</li></ul>
     * return null if fail
     */
    public String[] upload_file(String groupName, String masterFilename,
                                String prefixName, long fileSize, UploadCallback callback,
                                String fileExtName, NameValuePair[] metaList) throws IOException, FastDFSClientException {
        return do_upload_file(ProtoCommon.STORAGE_PROTO_CMD_UPLOAD_SLAVE_FILE, groupName, masterFilename, prefixName,
                fileExtName, fileSize, callback, metaList);
    }

    /**
     * upload appender file to storage server (by file name)
     *
     * @param localFilename local filename to upload
     * @param fileExtName   file ext name, do not include dot(.), null to extract ext name from the local filename
     * @param metaList      meta info array
     * @return 2 elements string array if success:<br>
     * <ul><li>results[0]: the group name to store the file </li></ul>
     * <ul><li>results[1]: the new created filename</li></ul>
     * return null if fail
     */
    public String[] upload_appender_file(String localFilename, String fileExtName, NameValuePair[] metaList) throws IOException, FastDFSClientException {
        return upload_appender_file(null, localFilename, fileExtName, metaList);
    }

    /**
     * upload appender file to storage server (by file name)
     *
     * @param groupName     the group name to upload file to, can be empty
     * @param localFilename local filename to upload
     * @param fileExtName   file ext name, do not include dot(.), null to extract ext name from the local filename
     * @param metaList      meta info array
     * @return 2 elements string array if success:<br>
     * <ul><li>results[0]: the group name to store the file </li></ul>
     * <ul><li>results[1]: the new created filename</li></ul>
     * return null if fail
     */
    protected String[] upload_appender_file(String groupName, String localFilename, String fileExtName,
                                            NameValuePair[] metaList) throws IOException, FastDFSClientException {
        return upload_file(ProtoCommon.STORAGE_PROTO_CMD_UPLOAD_APPENDER_FILE, groupName, localFilename, fileExtName, metaList);
    }

    /**
     * upload appender file to storage server (by file buff)
     *
     * @param fileBuff    file content/buff
     * @param offset      start offset of the buff
     * @param length      the length of buff to upload
     * @param fileExtName file ext name, do not include dot(.)
     * @param metaList    meta info array
     * @return 2 elements string array if success:<br>
     * <ul><li>results[0]: the group name to store the file</li></ul>
     * <ul><li>results[1]: the new created filename</li></ul>
     * return null if fail
     */
    public String[] upload_appender_file(byte[] fileBuff, int offset, int length, String fileExtName,
                                         NameValuePair[] metaList) throws IOException, FastDFSClientException {
        return upload_appender_file(null, fileBuff, offset, length, fileExtName, metaList);
    }

    /**
     * upload appender file to storage server (by file buff)
     *
     * @param groupName   the group name to upload file to, can be empty
     * @param fileBuff    file content/buff
     * @param offset      start offset of the buff
     * @param length      the length of buff to upload
     * @param fileExtName file ext name, do not include dot(.)
     * @param metaList    meta info array
     * @return 2 elements string array if success:<br>
     * <ul><li>results[0]: the group name to store the file</li></ul>
     * <ul><li>results[1]: the new created filename</li></ul>
     * return null if fail
     */
    public String[] upload_appender_file(String groupName, byte[] fileBuff, int offset, int length,
                                         String fileExtName, NameValuePair[] metaList) throws IOException, FastDFSClientException {
        UploadBuff uploadBuff = new UploadBuff(fileBuff, offset, length);
        return do_upload_file(ProtoCommon.STORAGE_PROTO_CMD_UPLOAD_APPENDER_FILE, groupName, null, null, fileExtName, length, uploadBuff, metaList);
    }

    /**
     * upload appender file to storage server (by file buff)
     *
     * @param fileBuff    file content/buff
     * @param fileExtName file ext name, do not include dot(.)
     * @param metaList    meta info array
     * @return 2 elements string array if success:<br>
     * <ul><li>results[0]: the group name to store the file</li></ul>
     * <ul><li>results[1]: the new created filename</li></ul>
     * return null if fail
     */
    public String[] upload_appender_file(byte[] fileBuff, String fileExtName,
                                         NameValuePair[] metaList) throws IOException, FastDFSClientException {
        return upload_appender_file(null, fileBuff, 0, fileBuff.length, fileExtName, metaList);
    }

    /**
     * upload appender file to storage server (by file buff)
     *
     * @param groupName   the group name to upload file to, can be empty
     * @param fileBuff    file content/buff
     * @param fileExtName file ext name, do not include dot(.)
     * @param metaList    meta info array
     * @return 2 elements string array if success:<br>
     * <ul><li>results[0]: the group name to store the file</li></ul>
     * <ul><li>results[1]: the new created filename</li></ul>
     * return null if fail
     */
    public String[] upload_appender_file(String groupName, byte[] fileBuff,
                                         String fileExtName, NameValuePair[] metaList) throws IOException, FastDFSClientException {
        UploadBuff uploadBuff = new UploadBuff(fileBuff, 0, fileBuff.length);
        return do_upload_file(ProtoCommon.STORAGE_PROTO_CMD_UPLOAD_APPENDER_FILE, groupName, null, null, fileExtName, fileBuff.length, uploadBuff, metaList);
    }

    /**
     * upload appender file to storage server (by callback)
     *
     * @param groupName   the group name to upload file to, can be empty
     * @param fileSize    the file size
     * @param callback    the write data callback object
     * @param fileExtName file ext name, do not include dot(.)
     * @param metaList    meta info array
     * @return 2 elements string array if success:<br>
     * <ul><li>results[0]: the group name to store the file</li></ul>
     * <ul><li>results[1]: the new created filename</li></ul>
     * return null if fail
     */
    public String[] upload_appender_file(String groupName, long fileSize, UploadCallback callback,
                                         String fileExtName, NameValuePair[] metaList) throws IOException, FastDFSClientException {
        return do_upload_file(ProtoCommon.STORAGE_PROTO_CMD_UPLOAD_APPENDER_FILE, groupName, null, null,
                fileExtName, fileSize, callback, metaList);
    }

    /**
     * append file to storage server (by file name)
     *
     * @param groupName        the group name of appender file
     * @param appenderFilename the appender filename
     * @param localFileName    local filename to append
     * @return 0 for success, != 0 for error (error no)
     */
    public int append_file(String groupName, String appenderFilename, String localFileName) throws IOException, FastDFSClientException {
        File file = new File(localFileName);
        FileInputStream fileInputStream = new FileInputStream(file);
        try {
            long length = file.length();
            UploadStream uploadStream = new UploadStream(fileInputStream, length);
            return do_append_file(groupName, appenderFilename, length, uploadStream);
        } finally {
            fileInputStream.close();
        }
    }

    /**
     * append file to storage server (by file buff)
     *
     * @param groupName        the group name of appender file
     * @param appenderFilename the appender filename
     * @param fileBuff         file content/buff
     * @return 0 for success, != 0 for error (error no)
     */
    public int append_file(String groupName, String appenderFilename, byte[] fileBuff) throws IOException, FastDFSClientException {
        int length = fileBuff.length;
        UploadBuff uploadBuff = new UploadBuff(fileBuff, 0, length);
        return do_append_file(groupName, appenderFilename, length, uploadBuff);
    }

    /**
     * append file to storage server (by file buff)
     *
     * @param groupName        the group name of appender file
     * @param appenderFilename the appender filename
     * @param fileBuff         file content/buff
     * @param offset           start offset of the buff
     * @param length           the length of buff to append
     * @return 0 for success, != 0 for error (error no)
     */
    public int append_file(String groupName, String appenderFilename,
                           byte[] fileBuff, int offset, int length) throws IOException, FastDFSClientException {
        return do_append_file(groupName, appenderFilename, length, new UploadBuff(fileBuff, offset, length));
    }

    /**
     * append file to storage server (by callback)
     *
     * @param groupName        the group name to append file to
     * @param appenderFilename the appender filename
     * @param fileSize         the file size
     * @param callback         the write data callback object
     * @return 0 for success, != 0 for error (error no)
     */
    public int append_file(String groupName, String appenderFilename,
                           long fileSize, UploadCallback callback) throws IOException, FastDFSClientException {
        return this.do_append_file(groupName, appenderFilename, fileSize, callback);
    }

    /**
     * modify appender file to storage server (by file name)
     *
     * @param groupName        the group name of appender file
     * @param appenderFileName the appender filename
     * @param fileOffset       the offset of appender file
     * @param localFileName    local filename to append
     * @return 0 for success, != 0 for error (error no)
     */
    public int modify_file(String groupName, String appenderFileName,
                           long fileOffset, String localFileName) throws IOException, FastDFSClientException {
        File file = new File(localFileName);
        FileInputStream fis = new FileInputStream(file);
        try {
            return do_modify_file(groupName, appenderFileName, fileOffset, file.length(), new UploadStream(fis, file.length()));
        } finally {
            fis.close();
        }
    }

    /**
     * modify appender file to storage server (by file buff)
     *
     * @param groupName        the group name of appender file
     * @param appenderFileName the appender filename
     * @param fileOffset       the offset of appender file
     * @param fileBuff         file content/buff
     * @return 0 for success, != 0 for error (error no)
     */
    public int modify_file(String groupName, String appenderFileName,
                           long fileOffset, byte[] fileBuff) throws IOException, FastDFSClientException {
        return this.do_modify_file(groupName, appenderFileName, fileOffset,
                fileBuff.length, new UploadBuff(fileBuff, 0, fileBuff.length));
    }

    /**
     * modify appender file to storage server (by file buff)
     *
     * @param groupName        the group name of appender file
     * @param appenderFileName the appender filename
     * @param fileOffset       the offset of appender file
     * @param fileBuff         file content/buff
     * @param bufferOffset     start offset of the buff
     * @param bufferLength     the length of buff to modify
     * @return 0 for success, != 0 for error (error no)
     */
    public int modify_file(String groupName, String appenderFileName,
                           long fileOffset, byte[] fileBuff, int bufferOffset, int bufferLength) throws IOException, FastDFSClientException {
        return this.do_modify_file(groupName, appenderFileName, fileOffset,
                bufferLength, new UploadBuff(fileBuff, bufferOffset, bufferLength));
    }

    /**
     * modify appender file to storage server (by callback)
     *
     * @param groupName        the group name to modify file to
     * @param appenderFileName the appender filename
     * @param fileOffset       the offset of appender file
     * @param modifySize       the modify size
     * @param callback         the write data callback object
     * @return 0 for success, != 0 for error (error no)
     */
    public int modify_file(String groupName, String appenderFileName,
                           long fileOffset, long modifySize, UploadCallback callback) throws IOException, FastDFSClientException {
        return this.do_modify_file(groupName, appenderFileName, fileOffset,
                modifySize, callback);
    }

    /**
     * upload file to storage server
     *
     * @param cmd            the command code
     * @param groupName      the group name to upload file to, can be empty
     * @param masterFileName the master file name to generate the slave file
     * @param prefixName     the prefix name to generate the slave file
     * @param fileExtName    file ext name, do not include dot(.)
     * @param file_size      the file size
     * @param callback       the write data callback object
     * @param metaList       meta info array
     * @return 2 elements string array if success:<br>
     * <ul><li> results[0]: the group name to store the file</li></ul>
     * <ul><li> results[1]: the new created filename</li></ul>
     * return null if fail
     */
    protected String[] do_upload_file(byte cmd, String groupName, String masterFileName,
                                      String prefixName, String fileExtName, long file_size, UploadCallback callback,
                                      NameValuePair[] metaList) throws IOException, FastDFSClientException {
        byte[] header;
        byte[] extNameBytes = new byte[ProtoCommon.FDFS_FILE_EXT_NAME_MAX_LEN];
        Arrays.fill(extNameBytes, (byte) 0);
        String newGroupName;
        String remoteFilename;
        boolean bNewConnection;
        Socket storageSocket;
        byte[] sizeBytes;
        byte[] hexLenBytes;
        byte[] masterFilenameBytes;
        boolean bUploadSlave;
        int offset;
        long bodyLen;

        bUploadSlave = ((groupName != null && groupName.length() > 0) && (masterFileName != null && masterFileName.length() > 0) && (prefixName != null));
        if (bUploadSlave) {
            bNewConnection = newUpdatableStorageConnection(groupName, masterFileName);
        } else {
            bNewConnection = newWritableStorageConnection(groupName);
        }

        try {
            storageSocket = storageServer.getSocket();
            if (fileExtName != null && fileExtName.length() > 0) {
                byte[] fileExtNameBytes = fileExtName.getBytes(ClientGlobal.G_CHARSET);
                int extNameLen = fileExtNameBytes.length;
                if (extNameLen > ProtoCommon.FDFS_FILE_EXT_NAME_MAX_LEN) {
                    extNameLen = ProtoCommon.FDFS_FILE_EXT_NAME_MAX_LEN;
                }
                System.arraycopy(fileExtNameBytes, 0, extNameBytes, 0, extNameLen);
            }

            if (bUploadSlave) {
                masterFilenameBytes = masterFileName.getBytes(ClientGlobal.G_CHARSET);

                sizeBytes = new byte[2 * ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE];
                bodyLen = sizeBytes.length + ProtoCommon.FDFS_FILE_PREFIX_MAX_LEN + ProtoCommon.FDFS_FILE_EXT_NAME_MAX_LEN
                        + masterFilenameBytes.length + file_size;

                hexLenBytes = ProtoCommon.long2buff(masterFileName.length());
                System.arraycopy(hexLenBytes, 0, sizeBytes, 0, hexLenBytes.length);
                offset = hexLenBytes.length;
            } else {
                masterFilenameBytes = null;
                sizeBytes = new byte[1 + 1 * ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE];
                bodyLen = sizeBytes.length + ProtoCommon.FDFS_FILE_EXT_NAME_MAX_LEN + file_size;

                sizeBytes[0] = (byte) this.storageServer.getStorePathIndex();
                offset = 1;
            }

            hexLenBytes = ProtoCommon.long2buff(file_size);
            System.arraycopy(hexLenBytes, 0, sizeBytes, offset, hexLenBytes.length);

            OutputStream out = storageSocket.getOutputStream();
            header = ProtoCommon.packHeader(cmd, bodyLen, (byte) 0);
            byte[] wholePkg = new byte[(int) (header.length + bodyLen - file_size)];
            System.arraycopy(header, 0, wholePkg, 0, header.length);
            System.arraycopy(sizeBytes, 0, wholePkg, header.length, sizeBytes.length);
            offset = header.length + sizeBytes.length;
            if (bUploadSlave) {
                byte[] prefixName_bs = new byte[ProtoCommon.FDFS_FILE_PREFIX_MAX_LEN];
                byte[] bs = prefixName.getBytes(ClientGlobal.G_CHARSET);
                int prefixName_len = bs.length;
                Arrays.fill(prefixName_bs, (byte) 0);
                if (prefixName_len > ProtoCommon.FDFS_FILE_PREFIX_MAX_LEN) {
                    prefixName_len = ProtoCommon.FDFS_FILE_PREFIX_MAX_LEN;
                }
                if (prefixName_len > 0) {
                    System.arraycopy(bs, 0, prefixName_bs, 0, prefixName_len);
                }

                System.arraycopy(prefixName_bs, 0, wholePkg, offset, prefixName_bs.length);
                offset += prefixName_bs.length;
            }

            System.arraycopy(extNameBytes, 0, wholePkg, offset, extNameBytes.length);
            offset += extNameBytes.length;

            if (bUploadSlave) {
                System.arraycopy(masterFilenameBytes, 0, wholePkg, offset, masterFilenameBytes.length);
                offset += masterFilenameBytes.length;
            }

            out.write(wholePkg);

            if ((this.errno = (byte) callback.send(out)) != 0) {
                return null;
            }

            ProtoCommon.RecvPackageInfo pkgInfo = ProtoCommon.recvPackage(storageSocket.getInputStream(),
                    ProtoCommon.STORAGE_PROTO_CMD_RESP, -1);
            this.errno = pkgInfo.errno;
            if (pkgInfo.errno != 0) {
                return null;
            }

            if (pkgInfo.body.length <= ProtoCommon.FDFS_GROUP_NAME_MAX_LEN) {
                throw new FastDFSClientException("body length: " + pkgInfo.body.length + " <= " + ProtoCommon.FDFS_GROUP_NAME_MAX_LEN);
            }

            newGroupName = new String(pkgInfo.body, 0, ProtoCommon.FDFS_GROUP_NAME_MAX_LEN).trim();
            remoteFilename = new String(pkgInfo.body, ProtoCommon.FDFS_GROUP_NAME_MAX_LEN, pkgInfo.body.length - ProtoCommon.FDFS_GROUP_NAME_MAX_LEN);
            String[] results = new String[2];
            results[0] = newGroupName;
            results[1] = remoteFilename;

            if (metaList == null || metaList.length == 0) {
                return results;
            }

            int result = 0;
            try {
                result = set_metadata(newGroupName, remoteFilename, metaList, ProtoCommon.STORAGE_SET_METADATA_FLAG_OVERWRITE);
            } catch (IOException ex) {
                result = 5;
                throw ex;
            } finally {
                if (result != 0) {
                    this.errno = (byte) result;
                    this.delete_file(newGroupName, remoteFilename);
                    return null;
                }
            }

            return results;
        } catch (IOException ex) {
            throw ex;
        } finally {
            closeStorageServer(bNewConnection);
        }
    }

    /**
     * append file to storage server
     *
     * @param groupName        the group name of appender file
     * @param appenderFileName the appender filename
     * @param file_size        the file size
     * @param callback         the write data callback object
     * @return return true for success, false for fail
     */
    protected int do_append_file(String groupName, String appenderFileName,
                                 long file_size, UploadCallback callback) throws IOException, FastDFSClientException {
        byte[] header;
        boolean bNewConnection;
        Socket storageSocket;
        byte[] hexLenBytes;
        byte[] appenderFilenameBytes;
        int offset;
        long bodyLen;

        if ((groupName == null || groupName.length() == 0) ||
                (appenderFileName == null || appenderFileName.length() == 0)) {
            this.errno = ProtoCommon.ERR_NO_EINVAL;
            return this.errno;
        }

        bNewConnection = this.newUpdatableStorageConnection(groupName, appenderFileName);

        try {
            storageSocket = this.storageServer.getSocket();

            appenderFilenameBytes = appenderFileName.getBytes(ClientGlobal.G_CHARSET);
            bodyLen = 2 * ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE + appenderFilenameBytes.length + file_size;

            header = ProtoCommon.packHeader(ProtoCommon.STORAGE_PROTO_CMD_APPEND_FILE, bodyLen, (byte) 0);
            byte[] wholePkg = new byte[(int) (header.length + bodyLen - file_size)];
            System.arraycopy(header, 0, wholePkg, 0, header.length);
            offset = header.length;

            hexLenBytes = ProtoCommon.long2buff(appenderFileName.length());
            System.arraycopy(hexLenBytes, 0, wholePkg, offset, hexLenBytes.length);
            offset += hexLenBytes.length;

            hexLenBytes = ProtoCommon.long2buff(file_size);
            System.arraycopy(hexLenBytes, 0, wholePkg, offset, hexLenBytes.length);
            offset += hexLenBytes.length;

            OutputStream out = storageSocket.getOutputStream();

            System.arraycopy(appenderFilenameBytes, 0, wholePkg, offset, appenderFilenameBytes.length);
            offset += appenderFilenameBytes.length;

            out.write(wholePkg);
            if ((this.errno = (byte) callback.send(out)) != 0) {
                return this.errno;
            }

            ProtoCommon.RecvPackageInfo pkgInfo = ProtoCommon.recvPackage(storageSocket.getInputStream(),
                    ProtoCommon.STORAGE_PROTO_CMD_RESP, 0);
            this.errno = pkgInfo.errno;
            if (pkgInfo.errno != 0) {
                return this.errno;
            }

            return 0;
        } catch (IOException ex) {
            throw ex;
        } finally {
            closeStorageServer(bNewConnection);
        }
    }

    /**
     * modify appender file to storage server
     *
     * @param groupName        the group name of appender file
     * @param appenderFileName the appender filename
     * @param fileOffset       the offset of appender file
     * @param modify_size      the modify size
     * @param callback         the write data callback object
     * @return return true for success, false for fail
     */
    protected int do_modify_file(String groupName, String appenderFileName,
                                 long fileOffset, long modify_size, UploadCallback callback) throws IOException, FastDFSClientException {
        byte[] header;
        boolean bNewConnection;
        Socket storageSocket;
        byte[] hexLenBytes;
        byte[] appenderFilenameBytes;
        int offset;
        long bodyLen;

        if ((groupName == null || groupName.length() == 0) ||
                (appenderFileName == null || appenderFileName.length() == 0)) {
            this.errno = ProtoCommon.ERR_NO_EINVAL;
            return this.errno;
        }

        bNewConnection = this.newUpdatableStorageConnection(groupName, appenderFileName);

        try {
            storageSocket = this.storageServer.getSocket();

            appenderFilenameBytes = appenderFileName.getBytes(ClientGlobal.G_CHARSET);
            bodyLen = 3 * ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE + appenderFilenameBytes.length + modify_size;

            header = ProtoCommon.packHeader(ProtoCommon.STORAGE_PROTO_CMD_MODIFY_FILE, bodyLen, (byte) 0);
            byte[] wholePkg = new byte[(int) (header.length + bodyLen - modify_size)];
            System.arraycopy(header, 0, wholePkg, 0, header.length);
            offset = header.length;

            hexLenBytes = ProtoCommon.long2buff(appenderFileName.length());
            System.arraycopy(hexLenBytes, 0, wholePkg, offset, hexLenBytes.length);
            offset += hexLenBytes.length;

            hexLenBytes = ProtoCommon.long2buff(fileOffset);
            System.arraycopy(hexLenBytes, 0, wholePkg, offset, hexLenBytes.length);
            offset += hexLenBytes.length;

            hexLenBytes = ProtoCommon.long2buff(modify_size);
            System.arraycopy(hexLenBytes, 0, wholePkg, offset, hexLenBytes.length);
            offset += hexLenBytes.length;

            OutputStream out = storageSocket.getOutputStream();

            System.arraycopy(appenderFilenameBytes, 0, wholePkg, offset, appenderFilenameBytes.length);
            offset += appenderFilenameBytes.length;

            out.write(wholePkg);
            if ((this.errno = (byte) callback.send(out)) != 0) {
                return this.errno;
            }

            ProtoCommon.RecvPackageInfo pkgInfo = ProtoCommon.recvPackage(storageSocket.getInputStream(),
                    ProtoCommon.STORAGE_PROTO_CMD_RESP, 0);
            this.errno = pkgInfo.errno;
            if (pkgInfo.errno != 0) {
                return this.errno;
            }

            return 0;
        } catch (IOException ex) {
            throw ex;
        } finally {
            closeStorageServer(bNewConnection);
        }
    }

    /**
     * delete file from storage server
     *
     * @param groupName      the group name of storage server
     * @param remoteFileName filename on storage server
     * @return 0 for success, none zero for fail (error code)
     */
    public int delete_file(String groupName, String remoteFileName) throws IOException, FastDFSClientException {
        boolean bNewConnection = this.newUpdatableStorageConnection(groupName, remoteFileName);
        Socket storageSocket = this.storageServer.getSocket();

        try {
            send_package(ProtoCommon.STORAGE_PROTO_CMD_DELETE_FILE, groupName, remoteFileName);
            ProtoCommon.RecvPackageInfo pkgInfo = ProtoCommon.recvPackage(storageSocket.getInputStream(),
                    ProtoCommon.STORAGE_PROTO_CMD_RESP, 0);

            this.errno = pkgInfo.errno;
            return pkgInfo.errno;
        } catch (IOException ex) {
            throw ex;
        } finally {
            closeStorageServer(bNewConnection);
        }
    }

    /**
     * truncate appender file to size 0 from storage server
     *
     * @param groupName        the group name of storage server
     * @param appenderFileName the appender filename
     * @return 0 for success, none zero for fail (error code)
     */
    public int truncate_file(String groupName, String appenderFileName) throws IOException, FastDFSClientException {
        return this.truncate_file(groupName, appenderFileName, 0);
    }

    /**
     * truncate appender file from storage server
     *
     * @param groupName         the group name of storage server
     * @param appenderFileName  the appender filename
     * @param truncatedFileSize truncated file size
     * @return 0 for success, none zero for fail (error code)
     */
    public int truncate_file(String groupName, String appenderFileName,
                             long truncatedFileSize) throws IOException, FastDFSClientException {
        byte[] header;
        boolean bNewConnection;
        Socket storageSocket;
        byte[] hexLenBytes;
        byte[] appenderFilenameBytes;
        int offset;
        int bodyLen;

        if ((groupName == null || groupName.length() == 0) ||
                (appenderFileName == null || appenderFileName.length() == 0)) {
            this.errno = ProtoCommon.ERR_NO_EINVAL;
            return this.errno;
        }

        bNewConnection = this.newUpdatableStorageConnection(groupName, appenderFileName);

        try {
            storageSocket = this.storageServer.getSocket();

            appenderFilenameBytes = appenderFileName.getBytes(ClientGlobal.G_CHARSET);
            bodyLen = 2 * ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE + appenderFilenameBytes.length;

            header = ProtoCommon.packHeader(ProtoCommon.STORAGE_PROTO_CMD_TRUNCATE_FILE, bodyLen, (byte) 0);
            byte[] wholePkg = new byte[header.length + bodyLen];
            System.arraycopy(header, 0, wholePkg, 0, header.length);
            offset = header.length;

            hexLenBytes = ProtoCommon.long2buff(appenderFileName.length());
            System.arraycopy(hexLenBytes, 0, wholePkg, offset, hexLenBytes.length);
            offset += hexLenBytes.length;

            hexLenBytes = ProtoCommon.long2buff(truncatedFileSize);
            System.arraycopy(hexLenBytes, 0, wholePkg, offset, hexLenBytes.length);
            offset += hexLenBytes.length;

            OutputStream out = storageSocket.getOutputStream();

            System.arraycopy(appenderFilenameBytes, 0, wholePkg, offset, appenderFilenameBytes.length);
            offset += appenderFilenameBytes.length;

            out.write(wholePkg);
            ProtoCommon.RecvPackageInfo pkgInfo = ProtoCommon.recvPackage(storageSocket.getInputStream(),
                    ProtoCommon.STORAGE_PROTO_CMD_RESP, 0);
            this.errno = pkgInfo.errno;
            return pkgInfo.errno;
        } catch (IOException ex) {
            throw ex;
        } finally {
            closeStorageServer(bNewConnection);
        }
    }

    private void closeStorageServer(boolean bNewConnection) {
        if (bNewConnection) {
            try {
                this.storageServer.close();
            } catch (IOException ex1) {
                ex1.printStackTrace();
            } finally {
                this.storageServer = null;
            }
        }
    }

    /**
     * download file from storage server
     *
     * @param groupName      the group name of storage server
     * @param remoteFileName filename on storage server
     * @return file content/buff, return null if fail
     */
    public byte[] download_file(String groupName, String remoteFileName) throws IOException, FastDFSClientException {
        return this.download_file(groupName, remoteFileName, 0, 0);
    }

    /**
     * download file from storage server
     *
     * @param groupName      the group name of storage server
     * @param remoteFileName filename on storage server
     * @param fileOffset     the start offset of the file
     * @param downloadBytes  download bytes, 0 for remain bytes from offset
     * @return file content/buff, return null if fail
     */
    public byte[] download_file(String groupName, String remoteFileName, long fileOffset, long downloadBytes) throws IOException, FastDFSClientException {
        boolean bNewConnection = this.newReadableStorageConnection(groupName, remoteFileName);
        Socket storageSocket = this.storageServer.getSocket();

        try {
            ProtoCommon.RecvPackageInfo pkgInfo;

            this.send_download_package(groupName, remoteFileName, fileOffset, downloadBytes);
            pkgInfo = ProtoCommon.recvPackage(storageSocket.getInputStream(),
                    ProtoCommon.STORAGE_PROTO_CMD_RESP, -1);

            this.errno = pkgInfo.errno;
            if (pkgInfo.errno != 0) {
                return null;
            }

            return pkgInfo.body;
        } catch (IOException ex) {
            throw ex;
        } finally {
            closeStorageServer(bNewConnection);
        }
    }

    /**
     * download file from storage server
     *
     * @param groupName      the group name of storage server
     * @param remoteFileName filename on storage server
     * @param localFileName  filename on local
     * @return 0 success, return none zero errno if fail
     */
    public int download_file(String groupName, String remoteFileName,
                             String localFileName) throws IOException, FastDFSClientException {
        return this.download_file(groupName, remoteFileName, 0, 0, localFileName);
    }

    /**
     * download file from storage server
     *
     * @param groupName      the group name of storage server
     * @param remoteFileName filename on storage server
     * @param fileOffset     the start offset of the file
     * @param downloadBytes  download bytes, 0 for remain bytes from offset
     * @param localFileName  filename on local
     * @return 0 success, return none zero errno if fail
     */
    public int download_file(String groupName, String remoteFileName, long fileOffset, long downloadBytes, String localFileName) throws IOException, FastDFSClientException {
        boolean bNewConnection = this.newReadableStorageConnection(groupName, remoteFileName);
        Socket storageSocket = this.storageServer.getSocket();
        try {
            ProtoCommon.RecvHeaderInfo header;
            FileOutputStream out = new FileOutputStream(localFileName);
            try {
                this.errno = 0;
                this.send_download_package(groupName, remoteFileName, fileOffset, downloadBytes);

                InputStream in = storageSocket.getInputStream();
                header = ProtoCommon.recvHeader(in, ProtoCommon.STORAGE_PROTO_CMD_RESP, -1);
                this.errno = header.errno;
                if (header.errno != 0) {
                    return header.errno;
                }

                byte[] buff = new byte[256 * 1024];
                long remainBytes = header.body_len;
                int bytes;

                while (remainBytes > 0) {
                    if ((bytes = in.read(buff, 0, remainBytes > buff.length ? buff.length : (int) remainBytes)) < 0) {
                        throw new IOException("recv package size " + (header.body_len - remainBytes) + " != " + header.body_len);
                    }
                    out.write(buff, 0, bytes);
                    remainBytes -= bytes;
                }
                return 0;
            } catch (IOException ex) {
                if (this.errno == 0) {
                    this.errno = ProtoCommon.ERR_NO_EIO;
                }
                throw ex;
            } finally {
                out.close();
                if (this.errno != 0) {
                    new File(localFileName).delete();
                }
            }
        } catch (IOException ex) {
            throw ex;
        } finally {
            closeStorageServer(bNewConnection);
        }
    }

    /**
     * download file from storage server
     *
     * @param groupName      the group name of storage server
     * @param remoteFileName filename on storage server
     * @param callback       call callback.recv() when data arrive
     * @return 0 success, return none zero errno if fail
     */
    public int download_file(String groupName, String remoteFileName,
                             DownloadCallback callback) throws IOException, FastDFSClientException {
        return this.download_file(groupName, remoteFileName, 0, 0, callback);
    }

    /**
     * download file from storage server
     *
     * @param groupName      the group name of storage server
     * @param remoteFileName filename on storage server
     * @param fileOffset     the start offset of the file
     * @param downloadBytes  download bytes, 0 for remain bytes from offset
     * @param callback       call callback.recv() when data arrive
     * @return 0 success, return none zero errno if fail
     */
    public int download_file(String groupName, String remoteFileName, long fileOffset, long downloadBytes, DownloadCallback callback) throws IOException, FastDFSClientException {
        int result;
        boolean bNewConnection = this.newReadableStorageConnection(groupName, remoteFileName);
        Socket storageSocket = this.storageServer.getSocket();

        try {
            ProtoCommon.RecvHeaderInfo header;
            this.send_download_package(groupName, remoteFileName, fileOffset, downloadBytes);

            InputStream in = storageSocket.getInputStream();
            header = ProtoCommon.recvHeader(in, ProtoCommon.STORAGE_PROTO_CMD_RESP, -1);
            this.errno = header.errno;
            if (header.errno != 0) {
                return header.errno;
            }

            byte[] buff = new byte[2 * 1024];
            long remainBytes = header.body_len;
            int bytes;

            while (remainBytes > 0) {
                if ((bytes = in.read(buff, 0, remainBytes > buff.length ? buff.length : (int) remainBytes)) < 0) {
                    throw new IOException("recv package size " + (header.body_len - remainBytes) + " != " + header.body_len);
                }
                if ((result = callback.recv(header.body_len, buff, bytes)) != 0) {
                    this.errno = (byte) result;
                    return result;
                }
                remainBytes -= bytes;
            }
            return 0;
        } catch (IOException ex) {
            throw ex;
        } finally {
            closeStorageServer(bNewConnection);
        }
    }

    /**
     * get all metadata items from storage server
     *
     * @param groupName      the group name of storage server
     * @param remoteFileName filename on storage server
     * @return meta info array, return null if fail
     */
    public NameValuePair[] get_metadata(String groupName, String remoteFileName) throws IOException, FastDFSClientException {
        boolean bNewConnection = this.newUpdatableStorageConnection(groupName, remoteFileName);
        Socket storageSocket = this.storageServer.getSocket();

        try {
            ProtoCommon.RecvPackageInfo pkgInfo;

            this.send_package(ProtoCommon.STORAGE_PROTO_CMD_GET_METADATA, groupName, remoteFileName);
            pkgInfo = ProtoCommon.recvPackage(storageSocket.getInputStream(),
                    ProtoCommon.STORAGE_PROTO_CMD_RESP, -1);

            this.errno = pkgInfo.errno;
            if (pkgInfo.errno != 0) {
                return null;
            }

            return ProtoCommon.split_metadata(new String(pkgInfo.body, ClientGlobal.G_CHARSET));
        } catch (IOException ex) {
            throw ex;
        } finally {
            closeStorageServer(bNewConnection);
        }
    }

    /**
     * set metadata items to storage server
     *
     * @param groupName      the group name of storage server
     * @param remoteFileName filename on storage server
     * @param metaList       meta item array
     * @param op_flag        flag, can be one of following values: <br>
     *                       <ul><li> ProtoCommon.STORAGE_SET_METADATA_FLAG_OVERWRITE: overwrite all old
     *                       metadata items</li></ul>
     *                       <ul><li> ProtoCommon.STORAGE_SET_METADATA_FLAG_MERGE: merge, insert when
     *                       the metadata item not exist, otherwise update it</li></ul>
     * @return 0 for success, !=0 fail (error code)
     */
    public int set_metadata(String groupName, String remoteFileName,
                            NameValuePair[] metaList, byte op_flag) throws IOException, FastDFSClientException {
        boolean bNewConnection = this.newUpdatableStorageConnection(groupName, remoteFileName);
        Socket storageSocket = this.storageServer.getSocket();

        try {
            byte[] header;
            byte[] groupBytes;
            byte[] filenameBytes;
            byte[] metaBuff;
            byte[] bs;
            int groupLen;
            byte[] sizeBytes;
            ProtoCommon.RecvPackageInfo pkgInfo;

            if (metaList == null) {
                metaBuff = new byte[0];
            } else {
                metaBuff = ProtoCommon.pack_metadata(metaList).getBytes(ClientGlobal.G_CHARSET);
            }

            filenameBytes = remoteFileName.getBytes(ClientGlobal.G_CHARSET);
            sizeBytes = new byte[2 * ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE];
            Arrays.fill(sizeBytes, (byte) 0);

            bs = ProtoCommon.long2buff(filenameBytes.length);
            System.arraycopy(bs, 0, sizeBytes, 0, bs.length);
            bs = ProtoCommon.long2buff(metaBuff.length);
            System.arraycopy(bs, 0, sizeBytes, ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE, bs.length);

            groupBytes = new byte[ProtoCommon.FDFS_GROUP_NAME_MAX_LEN];
            bs = groupName.getBytes(ClientGlobal.G_CHARSET);

            Arrays.fill(groupBytes, (byte) 0);
            if (bs.length <= groupBytes.length) {
                groupLen = bs.length;
            } else {
                groupLen = groupBytes.length;
            }
            System.arraycopy(bs, 0, groupBytes, 0, groupLen);

            header = ProtoCommon.packHeader(ProtoCommon.STORAGE_PROTO_CMD_SET_METADATA,
                    2 * ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE + 1 + groupBytes.length
                            + filenameBytes.length + metaBuff.length, (byte) 0);
            OutputStream out = storageSocket.getOutputStream();
            byte[] wholePkg = new byte[header.length + sizeBytes.length + 1 + groupBytes.length + filenameBytes.length];
            System.arraycopy(header, 0, wholePkg, 0, header.length);
            System.arraycopy(sizeBytes, 0, wholePkg, header.length, sizeBytes.length);
            wholePkg[header.length + sizeBytes.length] = op_flag;
            System.arraycopy(groupBytes, 0, wholePkg, header.length + sizeBytes.length + 1, groupBytes.length);
            System.arraycopy(filenameBytes, 0, wholePkg, header.length + sizeBytes.length + 1 + groupBytes.length, filenameBytes.length);
            out.write(wholePkg);
            if (metaBuff.length > 0) {
                out.write(metaBuff);
            }

            pkgInfo = ProtoCommon.recvPackage(storageSocket.getInputStream(), ProtoCommon.STORAGE_PROTO_CMD_RESP, 0);

            this.errno = pkgInfo.errno;
            return pkgInfo.errno;
        } catch (IOException ex) {
            throw ex;
        } finally {
            closeStorageServer(bNewConnection);
        }
    }

    /**
     * get file info decoded from the filename, fetch from the storage if necessary
     *
     * @param groupName      the group name
     * @param remoteFileName the filename
     * @return FileInfo object for success, return null for fail
     */
    public FileInfo get_file_info(String groupName, String remoteFileName) throws IOException, FastDFSClientException {
        if (remoteFileName.length() < ProtoCommon.FDFS_FILE_PATH_LEN + ProtoCommon.FDFS_FILENAME_BASE64_LENGTH
                + ProtoCommon.FDFS_FILE_EXT_NAME_MAX_LEN + 1) {
            this.errno = ProtoCommon.ERR_NO_EINVAL;
            return null;
        }

        byte[] buff = base64.decodeAuto(remoteFileName.substring(ProtoCommon.FDFS_FILE_PATH_LEN,
                ProtoCommon.FDFS_FILE_PATH_LEN + ProtoCommon.FDFS_FILENAME_BASE64_LENGTH));

        long file_size = ProtoCommon.buff2long(buff, 4 * 2);
        if (((remoteFileName.length() > ProtoCommon.TRUNK_LOGIC_FILENAME_LENGTH) ||
                ((remoteFileName.length() > ProtoCommon.NORMAL_LOGIC_FILENAME_LENGTH) && ((file_size & ProtoCommon.TRUNK_FILE_MARK_SIZE) == 0))) ||
                ((file_size & ProtoCommon.APPENDER_FILE_SIZE) != 0)) { //slave file or appender file
            FileInfo fi = this.query_file_info(groupName, remoteFileName);
            if (fi == null) {
                return null;
            }
            return fi;
        }

        FileInfo fileInfo = new FileInfo(file_size, 0, 0, ProtoCommon.getIpAddress(buff, 0));
        fileInfo.setCreateTimestamp(ProtoCommon.buff2int(buff, 4));
        if ((file_size >> 63) != 0) {
            file_size &= 0xFFFFFFFFL;  //low 32 bits is file size
            fileInfo.setFileSize(file_size);
        }
        fileInfo.setCrc32(ProtoCommon.buff2int(buff, 4 * 4));

        return fileInfo;
    }

    /**
     * get file info from storage server
     *
     * @param groupName      the group name of storage server
     * @param remoteFileName filename on storage server
     * @return FileInfo object for success, return null for fail
     */
    public FileInfo query_file_info(String groupName, String remoteFileName) throws IOException, FastDFSClientException {
        boolean bNewConnection = this.newUpdatableStorageConnection(groupName, remoteFileName);
        Socket storageSocket = this.storageServer.getSocket();

        try {
            byte[] header;
            byte[] groupBytes;
            byte[] filenameBytes;
            byte[] bs;
            int groupLen;
            ProtoCommon.RecvPackageInfo pkgInfo;

            filenameBytes = remoteFileName.getBytes(ClientGlobal.G_CHARSET);
            groupBytes = new byte[ProtoCommon.FDFS_GROUP_NAME_MAX_LEN];
            bs = groupName.getBytes(ClientGlobal.G_CHARSET);

            Arrays.fill(groupBytes, (byte) 0);
            if (bs.length <= groupBytes.length) {
                groupLen = bs.length;
            } else {
                groupLen = groupBytes.length;
            }
            System.arraycopy(bs, 0, groupBytes, 0, groupLen);

            header = ProtoCommon.packHeader(ProtoCommon.STORAGE_PROTO_CMD_QUERY_FILE_INFO,
                    +groupBytes.length + filenameBytes.length, (byte) 0);
            OutputStream out = storageSocket.getOutputStream();
            byte[] wholePkg = new byte[header.length + groupBytes.length + filenameBytes.length];
            System.arraycopy(header, 0, wholePkg, 0, header.length);
            System.arraycopy(groupBytes, 0, wholePkg, header.length, groupBytes.length);
            System.arraycopy(filenameBytes, 0, wholePkg, header.length + groupBytes.length, filenameBytes.length);
            out.write(wholePkg);

            pkgInfo = ProtoCommon.recvPackage(storageSocket.getInputStream(),
                    ProtoCommon.STORAGE_PROTO_CMD_RESP,
                    3 * ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE +
                            ProtoCommon.FDFS_IPADDR_SIZE);

            this.errno = pkgInfo.errno;
            if (pkgInfo.errno != 0) {
                return null;
            }

            long file_size = ProtoCommon.buff2long(pkgInfo.body, 0);
            int create_timestamp = (int) ProtoCommon.buff2long(pkgInfo.body, ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE);
            int crc32 = (int) ProtoCommon.buff2long(pkgInfo.body, 2 * ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE);
            String source_ip_addr = (new String(pkgInfo.body, 3 * ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE, ProtoCommon.FDFS_IPADDR_SIZE)).trim();
            return new FileInfo(file_size, create_timestamp, crc32, source_ip_addr);
        } catch (IOException ex) {
            throw ex;
        } finally {
            closeStorageServer(bNewConnection);
        }
    }

    /**
     * check storage socket, if null create a new connection
     *
     * @param groupName the group name to upload file to, can be empty
     * @return true if create a new connection
     */
    protected boolean newWritableStorageConnection(String groupName) throws IOException, FastDFSClientException {
        if (this.storageServer != null) {
            return false;
        } else {
            TrackerClient tracker = new TrackerClient();
            this.storageServer = tracker.getStorageServer(this.trackerServer, groupName);
            if (this.storageServer == null) {
                throw new FastDFSClientException("getStorageServer fail, errno code: " + tracker.getErrorCode());
            }
            return true;
        }
    }

    /**
     * check storage socket, if null create a new connection
     *
     * @param groupName      the group name of storage server
     * @param remoteFileName filename on storage server
     * @return true if create a new connection
     */
    protected boolean newReadableStorageConnection(String groupName, String remoteFileName) throws IOException, FastDFSClientException {
        if (this.storageServer != null) {
            return false;
        } else {
            TrackerClient tracker = new TrackerClient();
            this.storageServer = tracker.getFetchStorageServer(this.trackerServer, groupName, remoteFileName);
            if (this.storageServer == null) {
                throw new FastDFSClientException("getStorageServer fail, errno code: " + tracker.getErrorCode());
            }
            return true;
        }
    }

    /**
     * check storage socket, if null create a new connection
     *
     * @param groupName      the group name of storage server
     * @param remoteFileName filename on storage server
     * @return true if create a new connection
     */
    protected boolean newUpdatableStorageConnection(String groupName, String remoteFileName) throws IOException, FastDFSClientException {
        if (this.storageServer != null) {
            return false;
        } else {
            TrackerClient tracker = new TrackerClient();
            this.storageServer = tracker.getUpdateStorageServer(this.trackerServer, groupName, remoteFileName);
            if (this.storageServer == null) {
                throw new FastDFSClientException("getStorageServer fail, errno code: " + tracker.getErrorCode());
            }
            return true;
        }
    }

    /**
     * send package to storage server
     *
     * @param cmd            which command to send
     * @param groupName      the group name of storage server
     * @param remoteFileName filename on storage server
     */
    protected void send_package(byte cmd, String groupName, String remoteFileName) throws IOException {
        byte[] header;
        byte[] groupBytes;
        byte[] filenameBytes;
        byte[] bs;
        int groupLen;

        groupBytes = new byte[ProtoCommon.FDFS_GROUP_NAME_MAX_LEN];
        bs = groupName.getBytes(ClientGlobal.G_CHARSET);
        filenameBytes = remoteFileName.getBytes(ClientGlobal.G_CHARSET);

        Arrays.fill(groupBytes, (byte) 0);
        if (bs.length <= groupBytes.length) {
            groupLen = bs.length;
        } else {
            groupLen = groupBytes.length;
        }
        System.arraycopy(bs, 0, groupBytes, 0, groupLen);

        header = ProtoCommon.packHeader(cmd, groupBytes.length + filenameBytes.length, (byte) 0);
        byte[] wholePkg = new byte[header.length + groupBytes.length + filenameBytes.length];
        System.arraycopy(header, 0, wholePkg, 0, header.length);
        System.arraycopy(groupBytes, 0, wholePkg, header.length, groupBytes.length);
        System.arraycopy(filenameBytes, 0, wholePkg, header.length + groupBytes.length, filenameBytes.length);
        this.storageServer.getSocket().getOutputStream().write(wholePkg);
    }

    /**
     * send package to storage server
     *
     * @param groupName      the group name of storage server
     * @param remoteFileName filename on storage server
     * @param fileOffset     the start offset of the file
     * @param downloadBytes  download bytes
     */
    protected void send_download_package(String groupName, String remoteFileName, long fileOffset, long downloadBytes) throws IOException {
        byte[] header;
        byte[] bsOffset;
        byte[] bsDownBytes;
        byte[] groupBytes;
        byte[] filenameBytes;
        byte[] bs;
        int groupLen;

        bsOffset = ProtoCommon.long2buff(fileOffset);
        bsDownBytes = ProtoCommon.long2buff(downloadBytes);
        groupBytes = new byte[ProtoCommon.FDFS_GROUP_NAME_MAX_LEN];
        bs = groupName.getBytes(ClientGlobal.G_CHARSET);
        filenameBytes = remoteFileName.getBytes(ClientGlobal.G_CHARSET);

        Arrays.fill(groupBytes, (byte) 0);
        if (bs.length <= groupBytes.length) {
            groupLen = bs.length;
        } else {
            groupLen = groupBytes.length;
        }
        System.arraycopy(bs, 0, groupBytes, 0, groupLen);

        header = ProtoCommon.packHeader(ProtoCommon.STORAGE_PROTO_CMD_DOWNLOAD_FILE,
                bsOffset.length + bsDownBytes.length + groupBytes.length + filenameBytes.length, (byte) 0);
        byte[] wholePkg = new byte[header.length + bsOffset.length + bsDownBytes.length + groupBytes.length + filenameBytes.length];
        System.arraycopy(header, 0, wholePkg, 0, header.length);
        System.arraycopy(bsOffset, 0, wholePkg, header.length, bsOffset.length);
        System.arraycopy(bsDownBytes, 0, wholePkg, header.length + bsOffset.length, bsDownBytes.length);
        System.arraycopy(groupBytes, 0, wholePkg, header.length + bsOffset.length + bsDownBytes.length, groupBytes.length);
        System.arraycopy(filenameBytes, 0, wholePkg, header.length + bsOffset.length + bsDownBytes.length + groupBytes.length, filenameBytes.length);
        this.storageServer.getSocket().getOutputStream().write(wholePkg);
    }
}
