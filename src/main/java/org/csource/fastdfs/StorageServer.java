/**
 * Copyright (C) 2008 Happy Fish / YuQing
 * <p>
 * FastDFS Java Client may be copied only under the terms of the GNU Lesser
 * General Public License (LGPL).
 * Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
 */

package org.csource.fastdfs;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Storage Server Info
 *
 * @author Happy Fish / YuQing
 * @version Version 1.11
 */
public class StorageServer extends TrackerServer {
    private int storePathIndex = 0;

    /**
     * Constructor
     *
     * @param ipAddr     the ip address of storage server
     * @param port       the port of storage server
     * @param store_path the store path index on the storage server
     */
    public StorageServer(String ipAddr, int port, int store_path) throws IOException {
        super(ClientGlobal.getSocket(ipAddr, port), new InetSocketAddress(ipAddr, port));
        this.storePathIndex = store_path;
    }

    /**
     * Constructor
     *
     * @param ipAddr     the ip address of storage server
     * @param port       the port of storage server
     * @param store_path the store path index on the storage server
     */
    public StorageServer(String ipAddr, int port, byte store_path) throws IOException {
        super(ClientGlobal.getSocket(ipAddr, port), new InetSocketAddress(ipAddr, port));
        if (store_path < 0) {
            this.storePathIndex = 256 + store_path;
        } else {
            this.storePathIndex = store_path;
        }
    }

    /**
     * @return the store path index on the storage server
     */
    public int getStorePathIndex() {
        return this.storePathIndex;
    }
}
