/**
 * Copyright (C) 2008 Happy Fish / YuQing
 * <p>
 * FastDFS Java Client may be copied only under the terms of the GNU Lesser
 * General Public License (LGPL).
 * Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
 */

package org.csource.fastdfs;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Server Info
 *
 * @author Happy Fish / YuQing
 * @version Version 1.23
 */
public class FileInfo {
    private String sourceIpAddr;
    private long fileSize;
    private Date createTimestamp;
    private int crc32;

    /**
     * Constructor
     *
     * @param fileSize        the file size
     * @param createTimestamp create timestamp in seconds
     * @param crc32           the crc32 signature
     * @param sourceIpAddr    the source storage ip address
     */
    public FileInfo(long fileSize, int createTimestamp, int crc32, String sourceIpAddr) {
        this.fileSize = fileSize;
        this.createTimestamp = new Date(createTimestamp * 1000L);
        this.crc32 = crc32;
        this.sourceIpAddr = sourceIpAddr;
    }

    /**
     * set the source ip address of the file uploaded to
     *
     * @param source_ip_addr the source ip address
     */
    public void setSourceIpAddr(String source_ip_addr) {
        this.sourceIpAddr = source_ip_addr;
    }

    /**
     * get the source ip address of the file uploaded to
     *
     * @return the source ip address of the file uploaded to
     */
    public String getSourceIpAddr() {
        return this.sourceIpAddr;
    }

    /**
     * set the file size
     *
     * @param file_size the file size
     */
    public void setFileSize(long file_size) {
        this.fileSize = file_size;
    }

    /**
     * get the file size
     *
     * @return the file size
     */
    public long getFileSize() {
        return this.fileSize;
    }

    /**
     * set the create timestamp of the file
     *
     * @param create_timestamp create timestamp in seconds
     */
    public void setCreateTimestamp(int create_timestamp) {
        this.createTimestamp = new Date(create_timestamp * 1000L);
    }

    /**
     * get the create timestamp of the file
     *
     * @return the create timestamp of the file
     */
    public Date getCreateTimestamp() {
        return this.createTimestamp;
    }

    /**
     * set the create timestamp of the file
     *
     * @param crc32 the crc32 signature
     */
    public void setCrc32(int crc32) {
        this.crc32 = crc32;
    }

    /**
     * get the file CRC32 signature
     *
     * @return the file CRC32 signature
     */
    public long getCrc32() {
        return this.crc32;
    }

    /**
     * to string
     *
     * @return string
     */
    public String toString() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return "sourceIpAddr = " + this.sourceIpAddr + ", " +
                "fileSize = " + this.fileSize + ", " +
                "createTimestamp = " + df.format(this.createTimestamp) + ", " +
                "crc32 = " + this.crc32;
    }
}
