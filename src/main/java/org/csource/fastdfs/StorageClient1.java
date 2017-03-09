/**
 * Copyright (C) 2008 Happy Fish / YuQing
 * <p>
 * FastDFS Java Client may be copied only under the terms of the GNU Lesser
 * General Public License (LGPL).
 * Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
 */

package org.csource.fastdfs;

import org.csource.common.FastDFSClientException;
import org.csource.common.NameValuePair;

import java.io.IOException;

/**
 * Storage client for 1 field file id: combined group name and filename
 *
 * @author Happy Fish / YuQing
 * @version Version 1.21
 */
public class StorageClient1 extends StorageClient {
    public static final String SPLIT_GROUP_NAME_AND_FILENAME_SEPERATOR = "/";

    /**
     * constructor
     */
    public StorageClient1() {
        super();
    }

    /**
     * constructor
     *
     * @param trackerServer the tracker server, can be null
     * @param storageServer the storage server, can be null
     */
    public StorageClient1(TrackerServer trackerServer, StorageServer storageServer) {
        super(trackerServer, storageServer);
    }

    public static byte split_file_id(String file_id, String[] results) {
        int pos = file_id.indexOf(SPLIT_GROUP_NAME_AND_FILENAME_SEPERATOR);
        if ((pos <= 0) || (pos == file_id.length() - 1)) {
            return ProtoCommon.ERR_NO_EINVAL;
        }

        results[0] = file_id.substring(0, pos); //group name
        results[1] = file_id.substring(pos + 1); //file name
        return 0;
    }

    /**
     * upload file to storage server (by file name)
     *
     * @param localFileName local filename to upload
     * @param fileExtName   file ext name, do not include dot(.), null to extract ext name from the local filename
     * @param metaList      meta info array
     * @return file id(including group name and filename) if success, <br>
     * return null if fail
     */
    public String upload_file1(String localFileName, String fileExtName,
                               NameValuePair[] metaList) throws IOException, FastDFSClientException {
        String parts[] = this.upload_file(localFileName, fileExtName, metaList);
        if (parts != null) {
            return parts[0] + SPLIT_GROUP_NAME_AND_FILENAME_SEPERATOR + parts[1];
        } else {
            return null;
        }
    }

    /**
     * upload file to storage server (by file name)
     *
     * @param groupName     the group name to upload file to, can be empty
     * @param localFileName local filename to upload
     * @param fileExtName   file ext name, do not include dot(.), null to extract ext name from the local filename
     * @param metaList      meta info array
     * @return file id(including group name and filename) if success, <br>
     * return null if fail
     */
    public String upload_file1(String groupName, String localFileName, String fileExtName,
                               NameValuePair[] metaList) throws IOException, FastDFSClientException {
        String parts[] = this.upload_file(groupName, localFileName, fileExtName, metaList);
        if (parts != null) {
            return parts[0] + SPLIT_GROUP_NAME_AND_FILENAME_SEPERATOR + parts[1];
        } else {
            return null;
        }
    }

    /**
     * upload file to storage server (by file buff)
     *
     * @param fileBuff    file content/buff
     * @param fileExtName file ext name, do not include dot(.)
     * @param metaList    meta info array
     * @return file id(including group name and filename) if success, <br>
     * return null if fail
     */
    public String upload_file1(byte[] fileBuff, String fileExtName,
                               NameValuePair[] metaList) throws IOException, FastDFSClientException {
        String parts[] = this.upload_file(fileBuff, fileExtName, metaList);
        if (parts != null) {
            return parts[0] + SPLIT_GROUP_NAME_AND_FILENAME_SEPERATOR + parts[1];
        } else {
            return null;
        }
    }

    /**
     * upload file to storage server (by file buff)
     *
     * @param groupName   the group name to upload file to, can be empty
     * @param fileBuff    file content/buff
     * @param fileExtName file ext name, do not include dot(.)
     * @param metaList    meta info array
     * @return file id(including group name and filename) if success, <br>
     * return null if fail
     */
    public String upload_file1(String groupName, byte[] fileBuff, String fileExtName,
                               NameValuePair[] metaList) throws IOException, FastDFSClientException {
        String parts[] = this.upload_file(groupName, fileBuff, fileExtName, metaList);
        if (parts != null) {
            return parts[0] + SPLIT_GROUP_NAME_AND_FILENAME_SEPERATOR + parts[1];
        } else {
            return null;
        }
    }

    /**
     * upload file to storage server (by callback)
     *
     * @param groupName   the group name to upload file to, can be empty
     * @param file_size   the file size
     * @param callback    the write data callback object
     * @param fileExtName file ext name, do not include dot(.)
     * @param metaList    meta info array
     * @return file id(including group name and filename) if success, <br>
     * return null if fail
     */
    public String upload_file1(String groupName, long file_size,
                               UploadCallback callback, String fileExtName,
                               NameValuePair[] metaList) throws IOException, FastDFSClientException {
        String parts[] = this.upload_file(groupName, file_size, callback, fileExtName, metaList);
        if (parts != null) {
            return parts[0] + SPLIT_GROUP_NAME_AND_FILENAME_SEPERATOR + parts[1];
        } else {
            return null;
        }
    }

    /**
     * upload appender file to storage server (by file name)
     *
     * @param localFileName local filename to upload
     * @param fileExtName   file ext name, do not include dot(.), null to extract ext name from the local filename
     * @param metaList      meta info array
     * @return file id(including group name and filename) if success, <br>
     * return null if fail
     */
    public String upload_appender_file1(String localFileName, String fileExtName,
                                        NameValuePair[] metaList) throws IOException, FastDFSClientException {
        String parts[] = this.upload_appender_file(localFileName, fileExtName, metaList);
        if (parts != null) {
            return parts[0] + SPLIT_GROUP_NAME_AND_FILENAME_SEPERATOR + parts[1];
        } else {
            return null;
        }
    }

    /**
     * upload appender file to storage server (by file name)
     *
     * @param groupName     the group name to upload file to, can be empty
     * @param localFileName local filename to upload
     * @param fileExtName   file ext name, do not include dot(.), null to extract ext name from the local filename
     * @param metaList      meta info array
     * @return file id(including group name and filename) if success, <br>
     * return null if fail
     */
    public String upload_appender_file1(String groupName, String localFileName, String fileExtName,
                                        NameValuePair[] metaList) throws IOException, FastDFSClientException {
        String parts[] = this.upload_appender_file(groupName, localFileName, fileExtName, metaList);
        if (parts != null) {
            return parts[0] + SPLIT_GROUP_NAME_AND_FILENAME_SEPERATOR + parts[1];
        } else {
            return null;
        }
    }

    /**
     * upload appender file to storage server (by file buff)
     *
     * @param fileBuff    file content/buff
     * @param fileExtName file ext name, do not include dot(.)
     * @param metaList    meta info array
     * @return file id(including group name and filename) if success, <br>
     * return null if fail
     */
    public String upload_appender_file1(byte[] fileBuff, String fileExtName,
                                        NameValuePair[] metaList) throws IOException, FastDFSClientException {
        String parts[] = this.upload_appender_file(fileBuff, fileExtName, metaList);
        if (parts != null) {
            return parts[0] + SPLIT_GROUP_NAME_AND_FILENAME_SEPERATOR + parts[1];
        } else {
            return null;
        }
    }

    /**
     * upload appender file to storage server (by file buff)
     *
     * @param groupName   the group name to upload file to, can be empty
     * @param fileBuff    file content/buff
     * @param fileExtName file ext name, do not include dot(.)
     * @param metaList    meta info array
     * @return file id(including group name and filename) if success, <br>
     * return null if fail
     */
    public String upload_appender_file1(String groupName, byte[] fileBuff, String fileExtName,
                                        NameValuePair[] metaList) throws IOException, FastDFSClientException {
        String parts[] = this.upload_appender_file(groupName, fileBuff, fileExtName, metaList);
        if (parts != null) {
            return parts[0] + SPLIT_GROUP_NAME_AND_FILENAME_SEPERATOR + parts[1];
        } else {
            return null;
        }
    }

    /**
     * upload appender file to storage server (by callback)
     *
     * @param groupName   the group name to upload file to, can be empty
     * @param file_size   the file size
     * @param callback    the write data callback object
     * @param fileExtName file ext name, do not include dot(.)
     * @param metaList    meta info array
     * @return file id(including group name and filename) if success, <br>
     * return null if fail
     */
    public String upload_appender_file1(String groupName, long file_size,
                                        UploadCallback callback, String fileExtName,
                                        NameValuePair[] metaList) throws IOException, FastDFSClientException {
        String parts[] = this.upload_appender_file(groupName, file_size, callback, fileExtName, metaList);
        if (parts != null) {
            return parts[0] + SPLIT_GROUP_NAME_AND_FILENAME_SEPERATOR + parts[1];
        } else {
            return null;
        }
    }

    /**
     * upload file to storage server (by file name, slave file mode)
     *
     * @param master_file_id the master file id to generate the slave file
     * @param prefix_name    the prefix name to generate the slave file
     * @param localFileName  local filename to upload
     * @param fileExtName    file ext name, do not include dot(.), null to extract ext name from the local filename
     * @param metaList       meta info array
     * @return file id(including group name and filename) if success, <br>
     * return null if fail
     */
    public String upload_file1(String master_file_id, String prefix_name,
                               String localFileName, String fileExtName, NameValuePair[] metaList) throws IOException, FastDFSClientException {
        String[] parts = new String[2];
        this.errno = split_file_id(master_file_id, parts);
        if (this.errno != 0) {
            return null;
        }

        parts = this.upload_file(parts[0], parts[1], prefix_name,
                localFileName, fileExtName, metaList);
        if (parts != null) {
            return parts[0] + SPLIT_GROUP_NAME_AND_FILENAME_SEPERATOR + parts[1];
        } else {
            return null;
        }
    }

    /**
     * upload file to storage server (by file buff, slave file mode)
     *
     * @param master_file_id the master file id to generate the slave file
     * @param prefix_name    the prefix name to generate the slave file
     * @param fileBuff       file content/buff
     * @param fileExtName    file ext name, do not include dot(.)
     * @param metaList       meta info array
     * @return file id(including group name and filename) if success, <br>
     * return null if fail
     */
    public String upload_file1(String master_file_id, String prefix_name,
                               byte[] fileBuff, String fileExtName, NameValuePair[] metaList) throws IOException, FastDFSClientException {
        String[] parts = new String[2];
        this.errno = split_file_id(master_file_id, parts);
        if (this.errno != 0) {
            return null;
        }

        parts = this.upload_file(parts[0], parts[1], prefix_name, fileBuff, fileExtName, metaList);
        if (parts != null) {
            return parts[0] + SPLIT_GROUP_NAME_AND_FILENAME_SEPERATOR + parts[1];
        } else {
            return null;
        }
    }

    /**
     * upload file to storage server (by file buff, slave file mode)
     *
     * @param master_file_id the master file id to generate the slave file
     * @param prefix_name    the prefix name to generate the slave file
     * @param fileBuff       file content/buff
     * @param fileExtName    file ext name, do not include dot(.)
     * @param metaList       meta info array
     * @return file id(including group name and filename) if success, <br>
     * return null if fail
     */
    public String upload_file1(String master_file_id, String prefix_name,
                               byte[] fileBuff, int offset, int length, String fileExtName,
                               NameValuePair[] metaList) throws IOException, FastDFSClientException {
        String[] parts = new String[2];
        this.errno = split_file_id(master_file_id, parts);
        if (this.errno != 0) {
            return null;
        }

        parts = this.upload_file(parts[0], parts[1], prefix_name, fileBuff,
                offset, length, fileExtName, metaList);
        if (parts != null) {
            return parts[0] + SPLIT_GROUP_NAME_AND_FILENAME_SEPERATOR + parts[1];
        } else {
            return null;
        }
    }

    /**
     * upload file to storage server (by callback)
     *
     * @param master_file_id the master file id to generate the slave file
     * @param prefix_name    the prefix name to generate the slave file
     * @param file_size      the file size
     * @param callback       the write data callback object
     * @param fileExtName    file ext name, do not include dot(.)
     * @param metaList       meta info array
     * @return file id(including group name and filename) if success, <br>
     * return null if fail
     */
    public String upload_file1(String master_file_id, String prefix_name, long file_size,
                               UploadCallback callback, String fileExtName,
                               NameValuePair[] metaList) throws IOException, FastDFSClientException {
        String[] parts = new String[2];
        this.errno = split_file_id(master_file_id, parts);
        if (this.errno != 0) {
            return null;
        }

        parts = this.upload_file(parts[0], parts[1], prefix_name, file_size, callback, fileExtName, metaList);
        if (parts != null) {
            return parts[0] + SPLIT_GROUP_NAME_AND_FILENAME_SEPERATOR + parts[1];
        } else {
            return null;
        }
    }

    /**
     * append file to storage server (by file name)
     *
     * @param appender_file_id the appender file id
     * @param localFileName    local filename to append
     * @return 0 for success, != 0 for error (error no)
     */
    public int append_file1(String appender_file_id, String localFileName) throws IOException, FastDFSClientException {
        String[] parts = new String[2];
        this.errno = split_file_id(appender_file_id, parts);
        if (this.errno != 0) {
            return this.errno;
        }

        return this.append_file(parts[0], parts[1], localFileName);
    }

    /**
     * append file to storage server (by file buff)
     *
     * @param appender_file_id the appender file id
     * @param fileBuff         file content/buff
     * @return 0 for success, != 0 for error (error no)
     */
    public int append_file1(String appender_file_id, byte[] fileBuff) throws IOException, FastDFSClientException {
        String[] parts = new String[2];
        this.errno = split_file_id(appender_file_id, parts);
        if (this.errno != 0) {
            return this.errno;
        }

        return this.append_file(parts[0], parts[1], fileBuff);
    }

    /**
     * append file to storage server (by file buff)
     *
     * @param appender_file_id the appender file id
     * @param fileBuff         file content/buffer
     * @param offset           start offset of the buffer
     * @param length           the length of the buffer to append
     * @return 0 for success, != 0 for error (error no)
     */
    public int append_file1(String appender_file_id, byte[] fileBuff, int offset, int length) throws IOException, FastDFSClientException {
        String[] parts = new String[2];
        this.errno = split_file_id(appender_file_id, parts);
        if (this.errno != 0) {
            return this.errno;
        }

        return this.append_file(parts[0], parts[1], fileBuff, offset, length);
    }

    /**
     * append file to storage server (by callback)
     *
     * @param appender_file_id the appender file id
     * @param file_size        the file size
     * @param callback         the write data callback object
     * @return 0 for success, != 0 for error (error no)
     */
    public int append_file1(String appender_file_id, long file_size, UploadCallback callback) throws IOException, FastDFSClientException {
        String[] parts = new String[2];
        this.errno = split_file_id(appender_file_id, parts);
        if (this.errno != 0) {
            return this.errno;
        }

        return this.append_file(parts[0], parts[1], file_size, callback);
    }

    /**
     * modify appender file to storage server (by file name)
     *
     * @param appender_file_id the appender file id
     * @param file_offset      the offset of appender file
     * @param localFileName    local filename to append
     * @return 0 for success, != 0 for error (error no)
     */
    public int modify_file1(String appender_file_id,
                            long file_offset, String localFileName) throws IOException, FastDFSClientException {
        String[] parts = new String[2];
        this.errno = split_file_id(appender_file_id, parts);
        if (this.errno != 0) {
            return this.errno;
        }

        return this.modify_file(parts[0], parts[1], file_offset, localFileName);
    }

    /**
     * modify appender file to storage server (by file buff)
     *
     * @param appender_file_id the appender file id
     * @param file_offset      the offset of appender file
     * @param fileBuff         file content/buff
     * @return 0 for success, != 0 for error (error no)
     */
    public int modify_file1(String appender_file_id,
                            long file_offset, byte[] fileBuff) throws IOException, FastDFSClientException {
        String[] parts = new String[2];
        this.errno = split_file_id(appender_file_id, parts);
        if (this.errno != 0) {
            return this.errno;
        }

        return this.modify_file(parts[0], parts[1], file_offset, fileBuff);
    }

    /**
     * modify appender file to storage server (by file buff)
     *
     * @param appender_file_id the appender file id
     * @param file_offset      the offset of appender file
     * @param fileBuff         file content/buff
     * @param buffer_offset    start offset of the buff
     * @param buffer_length    the length of buff to modify
     * @return 0 for success, != 0 for error (error no)
     */
    public int modify_file1(String appender_file_id,
                            long file_offset, byte[] fileBuff, int buffer_offset, int buffer_length) throws IOException, FastDFSClientException {
        String[] parts = new String[2];
        this.errno = split_file_id(appender_file_id, parts);
        if (this.errno != 0) {
            return this.errno;
        }

        return this.modify_file(parts[0], parts[1], file_offset,
                fileBuff, buffer_offset, buffer_length);
    }

    /**
     * modify appender file to storage server (by callback)
     *
     * @param appender_file_id the appender file id
     * @param file_offset      the offset of appender file
     * @param modify_size      the modify size
     * @param callback         the write data callback object
     * @return 0 for success, != 0 for error (error no)
     */
    public int modify_file1(String appender_file_id,
                            long file_offset, long modify_size, UploadCallback callback) throws IOException, FastDFSClientException {
        String[] parts = new String[2];
        this.errno = split_file_id(appender_file_id, parts);
        if (this.errno != 0) {
            return this.errno;
        }

        return this.modify_file(parts[0], parts[1], file_offset, modify_size, callback);
    }

    /**
     * delete file from storage server
     *
     * @param file_id the file id(including group name and filename)
     * @return 0 for success, none zero for fail (error code)
     */
    public int delete_file1(String file_id) throws IOException, FastDFSClientException {
        String[] parts = new String[2];
        this.errno = split_file_id(file_id, parts);
        if (this.errno != 0) {
            return this.errno;
        }

        return this.delete_file(parts[0], parts[1]);
    }

    /**
     * truncate appender file to size 0 from storage server
     *
     * @param appender_file_id the appender file id
     * @return 0 for success, none zero for fail (error code)
     */
    public int truncate_file1(String appender_file_id) throws IOException, FastDFSClientException {
        String[] parts = new String[2];
        this.errno = split_file_id(appender_file_id, parts);
        if (this.errno != 0) {
            return this.errno;
        }

        return this.truncate_file(parts[0], parts[1]);
    }

    /**
     * truncate appender file from storage server
     *
     * @param appender_file_id    the appender file id
     * @param truncated_file_size truncated file size
     * @return 0 for success, none zero for fail (error code)
     */
    public int truncate_file1(String appender_file_id, long truncated_file_size) throws IOException, FastDFSClientException {
        String[] parts = new String[2];
        this.errno = split_file_id(appender_file_id, parts);
        if (this.errno != 0) {
            return this.errno;
        }

        return this.truncate_file(parts[0], parts[1], truncated_file_size);
    }

    /**
     * download file from storage server
     *
     * @param file_id the file id(including group name and filename)
     * @return file content/buffer, return null if fail
     */
    public byte[] download_file1(String file_id) throws IOException, FastDFSClientException {
        final long file_offset = 0;
        final long download_bytes = 0;

        return this.download_file1(file_id, file_offset, download_bytes);
    }

    /**
     * download file from storage server
     *
     * @param file_id        the file id(including group name and filename)
     * @param file_offset    the start offset of the file
     * @param download_bytes download bytes, 0 for remain bytes from offset
     * @return file content/buff, return null if fail
     */
    public byte[] download_file1(String file_id, long file_offset, long download_bytes) throws IOException, FastDFSClientException {
        String[] parts = new String[2];
        this.errno = split_file_id(file_id, parts);
        if (this.errno != 0) {
            return null;
        }

        return this.download_file(parts[0], parts[1], file_offset, download_bytes);
    }

    /**
     * download file from storage server
     *
     * @param file_id       the file id(including group name and filename)
     * @param localFileName the filename on local
     * @return 0 success, return none zero errno if fail
     */
    public int download_file1(String file_id, String localFileName) throws IOException, FastDFSClientException {
        final long file_offset = 0;
        final long download_bytes = 0;

        return this.download_file1(file_id, file_offset, download_bytes, localFileName);
    }

    /**
     * download file from storage server
     *
     * @param file_id        the file id(including group name and filename)
     * @param file_offset    the start offset of the file
     * @param download_bytes download bytes, 0 for remain bytes from offset
     * @param localFileName  the filename on local
     * @return 0 success, return none zero errno if fail
     */
    public int download_file1(String file_id, long file_offset, long download_bytes, String localFileName) throws IOException, FastDFSClientException {
        String[] parts = new String[2];
        this.errno = split_file_id(file_id, parts);
        if (this.errno != 0) {
            return this.errno;
        }

        return this.download_file(parts[0], parts[1], file_offset, download_bytes, localFileName);
    }

    /**
     * download file from storage server
     *
     * @param file_id  the file id(including group name and filename)
     * @param callback the callback object, will call callback.recv() when data arrive
     * @return 0 success, return none zero errno if fail
     */
    public int download_file1(String file_id, DownloadCallback callback) throws IOException, FastDFSClientException {
        final long file_offset = 0;
        final long download_bytes = 0;

        return this.download_file1(file_id, file_offset, download_bytes, callback);
    }

    /**
     * download file from storage server
     *
     * @param file_id        the file id(including group name and filename)
     * @param file_offset    the start offset of the file
     * @param download_bytes download bytes, 0 for remain bytes from offset
     * @param callback       the callback object, will call callback.recv() when data arrive
     * @return 0 success, return none zero errno if fail
     */
    public int download_file1(String file_id, long file_offset, long download_bytes, DownloadCallback callback) throws IOException, FastDFSClientException {
        String[] parts = new String[2];
        this.errno = split_file_id(file_id, parts);
        if (this.errno != 0) {
            return this.errno;
        }

        return this.download_file(parts[0], parts[1], file_offset, download_bytes, callback);
    }

    /**
     * get all metadata items from storage server
     *
     * @param file_id the file id(including group name and filename)
     * @return meta info array, return null if fail
     */
    public NameValuePair[] get_metadata1(String file_id) throws IOException, FastDFSClientException {
        String[] parts = new String[2];
        this.errno = split_file_id(file_id, parts);
        if (this.errno != 0) {
            return null;
        }

        return this.get_metadata(parts[0], parts[1]);
    }

    /**
     * set metadata items to storage server
     *
     * @param file_id  the file id(including group name and filename)
     * @param metaList meta item array
     * @param op_flag  flag, can be one of following values: <br>
     *                 <ul><li> ProtoCommon.STORAGE_SET_METADATA_FLAG_OVERWRITE: overwrite all old
     *                 metadata items</li></ul>
     *                 <ul><li> ProtoCommon.STORAGE_SET_METADATA_FLAG_MERGE: merge, insert when
     *                 the metadata item not exist, otherwise update it</li></ul>
     * @return 0 for success, !=0 fail (error code)
     */
    public int set_metadata1(String file_id, NameValuePair[] metaList, byte op_flag) throws IOException, FastDFSClientException {
        String[] parts = new String[2];
        this.errno = split_file_id(file_id, parts);
        if (this.errno != 0) {
            return this.errno;
        }

        return this.set_metadata(parts[0], parts[1], metaList, op_flag);
    }

    /**
     * get file info from storage server
     *
     * @param file_id the file id(including group name and filename)
     * @return FileInfo object for success, return null for fail
     */
    public FileInfo query_file_info1(String file_id) throws IOException, FastDFSClientException {
        String[] parts = new String[2];
        this.errno = split_file_id(file_id, parts);
        if (this.errno != 0) {
            return null;
        }

        return this.query_file_info(parts[0], parts[1]);
    }

    /**
     * get file info decoded from filename
     *
     * @param file_id the file id(including group name and filename)
     * @return FileInfo object for success, return null for fail
     */
    public FileInfo get_file_info1(String file_id) throws IOException, FastDFSClientException {
        String[] parts = new String[2];
        this.errno = split_file_id(file_id, parts);
        if (this.errno != 0) {
            return null;
        }

        return this.get_file_info(parts[0], parts[1]);
    }
}
