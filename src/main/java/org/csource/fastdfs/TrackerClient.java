/**
 * Copyright (C) 2008 Happy Fish / YuQing
 * <p>
 * FastDFS Java Client may be copied only under the terms of the GNU Lesser
 * General Public License (LGPL).
 * Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
 */

package org.csource.fastdfs;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;

/**
 * Tracker client
 *
 * @author Happy Fish / YuQing
 * @version Version 1.19
 */
public class TrackerClient {
    private TrackerGroup trackerGroup;
    private byte errno;

    /**
     * constructor with global tracker group
     */
    public TrackerClient() {
        this.trackerGroup = ClientGlobal.G_TRACKER_GROUP;
    }

    /**
     * constructor with specified tracker group
     *
     * @param tracker_group the tracker group object
     */
    public TrackerClient(TrackerGroup tracker_group) {
        this.trackerGroup = tracker_group;
    }

    /**
     * get the error code of last call
     *
     * @return the error code of last call
     */
    public byte getErrorCode() {
        return errno;
    }

    /**
     * get a connection to tracker server
     *
     * @return tracker server Socket object, return null if fail
     */
    public TrackerServer getTrackerServer() throws IOException {
        return trackerGroup.getTrackerServer();
    }

    /**
     * query storage server to upload file
     *
     * @param trackerServer the tracker server
     * @return storage server Socket object, return null if fail
     */
    public StorageServer getStorageServer(TrackerServer trackerServer) throws IOException {
        return getStorageServer(trackerServer, null);
    }

    /**
     * query storage server to upload file
     *
     * @param trackerServer the tracker server
     * @param groupName     the group name to upload file to, can be empty
     * @return storage server object, return null if fail
     */
    public StorageServer getStorageServer(TrackerServer trackerServer, String groupName) throws IOException {
        byte[] header;
        String ipAddr;
        int port;
        byte cmd;
        int outLen;
        boolean bNewConnection;
        byte storePath;
        Socket trackerSocket;

        if (trackerServer == null) {
            trackerServer = getTrackerServer();
            if (trackerServer == null) {
                return null;
            }
            bNewConnection = true;
        } else {
            bNewConnection = false;
        }

        trackerSocket = trackerServer.getSocket();
        OutputStream out = trackerSocket.getOutputStream();

        try {
            boolean empty = groupName == null || groupName.length() == 0;
            if (empty) {
                cmd = ProtoCommon.TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE;
                outLen = 0;
            } else {
                cmd = ProtoCommon.TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITH_GROUP_ONE;
                outLen = ProtoCommon.FDFS_GROUP_NAME_MAX_LEN;
            }
            header = ProtoCommon.packHeader(cmd, outLen, (byte) 0);
            out.write(header);

            if (!empty) {
                byte[] bGroupName;
                byte[] bs;
                int groupLen;

                bs = groupName.getBytes(ClientGlobal.G_CHARSET);
                bGroupName = new byte[ProtoCommon.FDFS_GROUP_NAME_MAX_LEN];

                if (bs.length <= ProtoCommon.FDFS_GROUP_NAME_MAX_LEN) {
                    groupLen = bs.length;
                } else {
                    groupLen = ProtoCommon.FDFS_GROUP_NAME_MAX_LEN;
                }
                Arrays.fill(bGroupName, (byte) 0);
                System.arraycopy(bs, 0, bGroupName, 0, groupLen);
                out.write(bGroupName);
            }

            ProtoCommon.RecvPackageInfo pkgInfo = ProtoCommon.recvPackage(trackerSocket.getInputStream(),
                    ProtoCommon.TRACKER_PROTO_CMD_RESP,
                    ProtoCommon.TRACKER_QUERY_STORAGE_STORE_BODY_LEN);
            errno = pkgInfo.errno;
            if (pkgInfo.errno != 0) {
                return null;
            }

            ipAddr = new String(pkgInfo.body, ProtoCommon.FDFS_GROUP_NAME_MAX_LEN, ProtoCommon.FDFS_IPADDR_SIZE - 1).trim();

            port = (int) ProtoCommon.buff2long(pkgInfo.body, ProtoCommon.FDFS_GROUP_NAME_MAX_LEN
                    + ProtoCommon.FDFS_IPADDR_SIZE - 1);
            storePath = pkgInfo.body[ProtoCommon.TRACKER_QUERY_STORAGE_STORE_BODY_LEN - 1];

            return new StorageServer(ipAddr, port, storePath);
        } catch (IOException ex) {
            if (!bNewConnection) {
                try {
                    trackerServer.close();
                } catch (IOException ex1) {
                    ex1.printStackTrace();
                }
            }

            throw ex;
        } finally {
            if (bNewConnection) {
                try {
                    trackerServer.close();
                } catch (IOException ex1) {
                    ex1.printStackTrace();
                }
            }
        }
    }

    /**
     * query storage servers to upload file
     *
     * @param trackerServer the tracker server
     * @param groupName     the group name to upload file to, can be empty
     * @return storage servers, return null if fail
     */
    public StorageServer[] getStorageServerList(TrackerServer trackerServer, String groupName) throws IOException {
        byte[] header;
        String ipAddr;
        int port;
        byte cmd;
        int outLen;
        boolean bNewConnection;
        Socket trackerSocket;

        if (trackerServer == null) {
            trackerServer = getTrackerServer();
            if (trackerServer == null) {
                return null;
            }
            bNewConnection = true;
        } else {
            bNewConnection = false;
        }

        trackerSocket = trackerServer.getSocket();
        OutputStream outputStream = trackerSocket.getOutputStream();

        try {
            boolean empty = groupName != null && groupName.length() > 0;
            if (!empty) {
                cmd = ProtoCommon.TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ALL;
                outLen = 0;
            } else {
                cmd = ProtoCommon.TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITH_GROUP_ALL;
                outLen = ProtoCommon.FDFS_GROUP_NAME_MAX_LEN;
            }
            header = ProtoCommon.packHeader(cmd, outLen, (byte) 0);
            outputStream.write(header);
            if (empty) {
                byte[] bGroupName;
                byte[] bs;
                int groupLen;

                bs = groupName.getBytes(ClientGlobal.G_CHARSET);
                bGroupName = new byte[ProtoCommon.FDFS_GROUP_NAME_MAX_LEN];

                if (bs.length <= ProtoCommon.FDFS_GROUP_NAME_MAX_LEN) {
                    groupLen = bs.length;
                } else {
                    groupLen = ProtoCommon.FDFS_GROUP_NAME_MAX_LEN;
                }
                Arrays.fill(bGroupName, (byte) 0);
                System.arraycopy(bs, 0, bGroupName, 0, groupLen);
                outputStream.write(bGroupName);
            }

            ProtoCommon.RecvPackageInfo pkgInfo = ProtoCommon.recvPackage(trackerSocket.getInputStream(),
                    ProtoCommon.TRACKER_PROTO_CMD_RESP, -1);
            errno = pkgInfo.errno;
            if (pkgInfo.errno != 0) {
                return null;
            }

            if (pkgInfo.body.length < ProtoCommon.TRACKER_QUERY_STORAGE_STORE_BODY_LEN) {
                errno = ProtoCommon.ERR_NO_EINVAL;
                return null;
            }

            int ipPortLen = pkgInfo.body.length - (ProtoCommon.FDFS_GROUP_NAME_MAX_LEN + 1);
            final int recordLength = ProtoCommon.FDFS_IPADDR_SIZE - 1 + ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE;

            if (ipPortLen % recordLength != 0) {
                errno = ProtoCommon.ERR_NO_EINVAL;
                return null;
            }

            int serverCount = ipPortLen / recordLength;
            if (serverCount > 16) {
                errno = ProtoCommon.ERR_NO_ENOSPC;
                return null;
            }

            StorageServer[] storageServers = new StorageServer[serverCount];
            byte storePath = pkgInfo.body[pkgInfo.body.length - 1];
            int offset = ProtoCommon.FDFS_GROUP_NAME_MAX_LEN;

            for (int i = 0; i < serverCount; i++) {
                ipAddr = new String(pkgInfo.body, offset, ProtoCommon.FDFS_IPADDR_SIZE - 1).trim();
                offset += ProtoCommon.FDFS_IPADDR_SIZE - 1;

                port = (int) ProtoCommon.buff2long(pkgInfo.body, offset);
                offset += ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE;

                storageServers[i] = new StorageServer(ipAddr, port, storePath);
            }

            return storageServers;
        } catch (IOException ex) {
            if (!bNewConnection) {
                try {
                    trackerServer.close();
                } catch (IOException ex1) {
                    ex1.printStackTrace();
                }
            }

            throw ex;
        } finally {
            if (bNewConnection) {
                try {
                    trackerServer.close();
                } catch (IOException ex1) {
                    ex1.printStackTrace();
                }
            }
        }
    }

    /**
     * query storage server to download file
     *
     * @param trackerServer the tracker server
     * @param groupName     the group name of storage server
     * @param filename      filename on storage server
     * @return storage server Socket object, return null if fail
     */
    public StorageServer getFetchStorageServer(TrackerServer trackerServer,
                                               String groupName, String filename) throws IOException {
        ServerInfo[] serverInfoList = getServerInfoList(trackerServer, ProtoCommon.TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE,
                groupName, filename);
        if (serverInfoList == null || serverInfoList.length == 0) {
            return null;
        }
        ServerInfo serverInfo = serverInfoList[0];
        return new StorageServer(serverInfo.getIpAddr(), serverInfo.getPort(), 0);
    }

    /**
     * query storage server to update file (delete file or set meta data)
     *
     * @param trackerServer the tracker server
     * @param groupName     the group name of storage server
     * @param filename      filename on storage server
     * @return storage server Socket object, return null if fail
     */
    public StorageServer getUpdateStorageServer(TrackerServer trackerServer,
                                                String groupName, String filename) throws IOException {
        ServerInfo[] serverInfoList = getServerInfoList(trackerServer, ProtoCommon.TRACKER_PROTO_CMD_SERVICE_QUERY_UPDATE,
                groupName, filename);
        if (serverInfoList == null || serverInfoList.length == 0) {
            return null;
        }
        ServerInfo serverInfo = serverInfoList[0];
        return new StorageServer(serverInfo.getIpAddr(), serverInfo.getPort(), 0);
    }

    /**
     * get storage servers to download file
     *
     * @param trackerServer the tracker server
     * @param groupName     the group name of storage server
     * @param filename      filename on storage server
     * @return storage servers, return null if fail
     */
    public ServerInfo[] getFetchServerInfoList(TrackerServer trackerServer, String groupName, String filename) throws IOException {
        return getServerInfoList(trackerServer, ProtoCommon.TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ALL, groupName, filename);
    }

    /**
     * query storage server to download file
     *
     * @param trackerServer the tracker server
     * @param cmd           command code, ProtoCommon.TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE or
     *                      ProtoCommon.TRACKER_PROTO_CMD_SERVICE_QUERY_UPDATE
     * @param groupName     the group name of storage server
     * @param filename      filename on storage server
     * @return storage server Socket object, return null if fail
     */
    protected ServerInfo[] getServerInfoList(TrackerServer trackerServer,
                                             byte cmd, String groupName, String filename) throws IOException {
        byte[] header;
        byte[] bFileName;
        byte[] bGroupName;
        byte[] bs;
        int len;
        String ipAddr;
        int port;
        boolean bNewConnection;
        Socket trackerSocket;

        if (trackerServer == null) {
            trackerServer = getTrackerServer();
            if (trackerServer == null) {
                return null;
            }
            bNewConnection = true;
        } else {
            bNewConnection = false;
        }
        trackerSocket = trackerServer.getSocket();
        OutputStream out = trackerSocket.getOutputStream();

        try {
            bs = groupName.getBytes(ClientGlobal.G_CHARSET);
            bGroupName = new byte[ProtoCommon.FDFS_GROUP_NAME_MAX_LEN];
            bFileName = filename.getBytes(ClientGlobal.G_CHARSET);

            if (bs.length <= ProtoCommon.FDFS_GROUP_NAME_MAX_LEN) {
                len = bs.length;
            } else {
                len = ProtoCommon.FDFS_GROUP_NAME_MAX_LEN;
            }
            Arrays.fill(bGroupName, (byte) 0);
            System.arraycopy(bs, 0, bGroupName, 0, len);

            header = ProtoCommon.packHeader(cmd, ProtoCommon.FDFS_GROUP_NAME_MAX_LEN + bFileName.length, (byte) 0);
            byte[] wholePkg = new byte[header.length + bGroupName.length + bFileName.length];
            System.arraycopy(header, 0, wholePkg, 0, header.length);
            System.arraycopy(bGroupName, 0, wholePkg, header.length, bGroupName.length);
            System.arraycopy(bFileName, 0, wholePkg, header.length + bGroupName.length, bFileName.length);
            out.write(wholePkg);

            ProtoCommon.RecvPackageInfo pkgInfo = ProtoCommon.recvPackage(trackerSocket.getInputStream(),
                    ProtoCommon.TRACKER_PROTO_CMD_RESP, -1);
            errno = pkgInfo.errno;
            if (pkgInfo.errno != 0) {
                return null;
            }

            if (pkgInfo.body.length < ProtoCommon.TRACKER_QUERY_STORAGE_FETCH_BODY_LEN) {
                throw new IOException("Invalid body length: " + pkgInfo.body.length);
            }

            if ((pkgInfo.body.length - ProtoCommon.TRACKER_QUERY_STORAGE_FETCH_BODY_LEN) % (ProtoCommon.FDFS_IPADDR_SIZE - 1) != 0) {
                throw new IOException("Invalid body length: " + pkgInfo.body.length);
            }

            int serverCount = 1 + (pkgInfo.body.length - ProtoCommon.TRACKER_QUERY_STORAGE_FETCH_BODY_LEN) / (ProtoCommon.FDFS_IPADDR_SIZE - 1);

            ipAddr = new String(pkgInfo.body, ProtoCommon.FDFS_GROUP_NAME_MAX_LEN, ProtoCommon.FDFS_IPADDR_SIZE - 1).trim();
            int offset = ProtoCommon.FDFS_GROUP_NAME_MAX_LEN + ProtoCommon.FDFS_IPADDR_SIZE - 1;

            port = (int) ProtoCommon.buff2long(pkgInfo.body, offset);
            offset += ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE;

            ServerInfo[] servers = new ServerInfo[serverCount];
            servers[0] = new ServerInfo(ipAddr, port);
            for (int i = 1; i < serverCount; i++) {
                servers[i] = new ServerInfo(new String(pkgInfo.body, offset, ProtoCommon.FDFS_IPADDR_SIZE - 1).trim(), port);
                offset += ProtoCommon.FDFS_IPADDR_SIZE - 1;
            }

            return servers;
        } catch (IOException ex) {
            if (!bNewConnection) {
                try {
                    trackerServer.close();
                } catch (IOException ex1) {
                    ex1.printStackTrace();
                }
            }

            throw ex;
        } finally {
            if (bNewConnection) {
                try {
                    trackerServer.close();
                } catch (IOException ex1) {
                    ex1.printStackTrace();
                }
            }
        }
    }

    /**
     * query storage server to download file
     *
     * @param trackerServer the tracker server
     * @param file_id       the file id(including group name and filename)
     * @return storage server Socket object, return null if fail
     */
    public StorageServer getFetchStorageServer1(TrackerServer trackerServer, String file_id) throws IOException {
        String[] parts = new String[2];
        errno = StorageClient1.split_file_id(file_id, parts);
        if (errno != 0) {
            return null;
        }

        return this.getFetchStorageServer(trackerServer, parts[0], parts[1]);
    }

    /**
     * get storage servers to download file
     *
     * @param trackerServer the tracker server
     * @param file_id       the file id(including group name and filename)
     * @return storage servers, return null if fail
     */
    public ServerInfo[] getFetchServerInfo1(TrackerServer trackerServer, String file_id) throws IOException {
        String[] parts = new String[2];
        errno = StorageClient1.split_file_id(file_id, parts);
        if (errno != 0) {
            return null;
        }

        return this.getFetchServerInfoList(trackerServer, parts[0], parts[1]);
    }

    /**
     * list groups
     *
     * @param trackerServer the tracker server
     * @return group stat array, return null if fail
     */
    public StructGroupStat[] listGroups(TrackerServer trackerServer) throws IOException {
        byte[] header;
        boolean bNewConnection;
        Socket trackerSocket;

        if (trackerServer == null) {
            trackerServer = getTrackerServer();
            if (trackerServer == null) {
                return null;
            }
            bNewConnection = true;
        } else {
            bNewConnection = false;
        }

        trackerSocket = trackerServer.getSocket();
        OutputStream out = trackerSocket.getOutputStream();

        try {
            header = ProtoCommon.packHeader(ProtoCommon.TRACKER_PROTO_CMD_SERVER_LIST_GROUP, 0, (byte) 0);
            out.write(header);

            ProtoCommon.RecvPackageInfo pkgInfo = ProtoCommon.recvPackage(trackerSocket.getInputStream(),
                    ProtoCommon.TRACKER_PROTO_CMD_RESP, -1);
            errno = pkgInfo.errno;
            if (pkgInfo.errno != 0) {
                return null;
            }

            ProtoStructDecoder<StructGroupStat> decoder = new ProtoStructDecoder<StructGroupStat>();
            return decoder.decode(pkgInfo.body, StructGroupStat.class, StructGroupStat.getFieldsTotalSize());
        } catch (IOException ex) {
            if (!bNewConnection) {
                try {
                    trackerServer.close();
                } catch (IOException ex1) {
                    ex1.printStackTrace();
                }
            }

            throw ex;
        } catch (Exception ex) {
            ex.printStackTrace();
            errno = ProtoCommon.ERR_NO_EINVAL;
            return null;
        } finally {
            if (bNewConnection) {
                try {
                    trackerServer.close();
                } catch (IOException ex1) {
                    ex1.printStackTrace();
                }
            }
        }
    }

    /**
     * query storage server stat info of the group
     *
     * @param trackerServer the tracker server
     * @param groupName     the group name of storage server
     * @return storage server stat array, return null if fail
     */
    public StructStorageStat[] listStorages(TrackerServer trackerServer, String groupName) throws IOException {
        final String storageIpAddr = null;
        return this.listStorages(trackerServer, groupName, storageIpAddr);
    }

    /**
     * query storage server stat info of the group
     *
     * @param trackerServer the tracker server
     * @param groupName     the group name of storage server
     * @param storageIpAddr the storage server ip address, can be null or empty
     * @return storage server stat array, return null if fail
     */
    public StructStorageStat[] listStorages(TrackerServer trackerServer,
                                            String groupName, String storageIpAddr) throws IOException {
        byte[] header;
        byte[] bGroupName;
        byte[] bs;
        int len;
        boolean bNewConnection;
        Socket trackerSocket;

        if (trackerServer == null) {
            trackerServer = getTrackerServer();
            if (trackerServer == null) {
                return null;
            }
            bNewConnection = true;
        } else {
            bNewConnection = false;
        }
        trackerSocket = trackerServer.getSocket();
        OutputStream out = trackerSocket.getOutputStream();

        try {
            bs = groupName.getBytes(ClientGlobal.G_CHARSET);
            bGroupName = new byte[ProtoCommon.FDFS_GROUP_NAME_MAX_LEN];

            if (bs.length <= ProtoCommon.FDFS_GROUP_NAME_MAX_LEN) {
                len = bs.length;
            } else {
                len = ProtoCommon.FDFS_GROUP_NAME_MAX_LEN;
            }
            Arrays.fill(bGroupName, (byte) 0);
            System.arraycopy(bs, 0, bGroupName, 0, len);

            int ipAddrLen;
            byte[] bIpAddr;
            if (storageIpAddr != null && storageIpAddr.length() > 0) {
                bIpAddr = storageIpAddr.getBytes(ClientGlobal.G_CHARSET);
                if (bIpAddr.length < ProtoCommon.FDFS_IPADDR_SIZE) {
                    ipAddrLen = bIpAddr.length;
                } else {
                    ipAddrLen = ProtoCommon.FDFS_IPADDR_SIZE - 1;
                }
            } else {
                bIpAddr = null;
                ipAddrLen = 0;
            }

            header = ProtoCommon.packHeader(ProtoCommon.TRACKER_PROTO_CMD_SERVER_LIST_STORAGE, ProtoCommon.FDFS_GROUP_NAME_MAX_LEN + ipAddrLen, (byte) 0);
            byte[] wholePkg = new byte[header.length + bGroupName.length + ipAddrLen];
            System.arraycopy(header, 0, wholePkg, 0, header.length);
            System.arraycopy(bGroupName, 0, wholePkg, header.length, bGroupName.length);
            if (ipAddrLen > 0) {
                System.arraycopy(bIpAddr, 0, wholePkg, header.length + bGroupName.length, ipAddrLen);
            }
            out.write(wholePkg);

            ProtoCommon.RecvPackageInfo pkgInfo = ProtoCommon.recvPackage(trackerSocket.getInputStream(),
                    ProtoCommon.TRACKER_PROTO_CMD_RESP, -1);
            errno = pkgInfo.errno;
            if (pkgInfo.errno != 0) {
                return null;
            }

            ProtoStructDecoder<StructStorageStat> decoder = new ProtoStructDecoder<StructStorageStat>();
            return decoder.decode(pkgInfo.body, StructStorageStat.class, StructStorageStat.getFieldsTotalSize());
        } catch (IOException ex) {
            if (!bNewConnection) {
                try {
                    trackerServer.close();
                } catch (IOException ex1) {
                    ex1.printStackTrace();
                }
            }

            throw ex;
        } catch (Exception ex) {
            ex.printStackTrace();
            errno = ProtoCommon.ERR_NO_EINVAL;
            return null;
        } finally {
            if (bNewConnection) {
                try {
                    trackerServer.close();
                } catch (IOException ex1) {
                    ex1.printStackTrace();
                }
            }
        }
    }

    /**
     * delete a storage server from the tracker server
     *
     * @param trackerServer the connected tracker server
     * @param groupName     the group name of storage server
     * @param storageIpAddr the storage server ip address
     * @return true for success, false for fail
     */
    private boolean deleteStorage(TrackerServer trackerServer,
                                  String groupName, String storageIpAddr) throws IOException {
        byte[] header;
        byte[] bGroupName;
        byte[] bs;
        int len;
        Socket trackerSocket;

        trackerSocket = trackerServer.getSocket();
        OutputStream out = trackerSocket.getOutputStream();

        bs = groupName.getBytes(ClientGlobal.G_CHARSET);
        bGroupName = new byte[ProtoCommon.FDFS_GROUP_NAME_MAX_LEN];

        if (bs.length <= ProtoCommon.FDFS_GROUP_NAME_MAX_LEN) {
            len = bs.length;
        } else {
            len = ProtoCommon.FDFS_GROUP_NAME_MAX_LEN;
        }
        Arrays.fill(bGroupName, (byte) 0);
        System.arraycopy(bs, 0, bGroupName, 0, len);

        int ipAddrLen;
        byte[] bIpAddr = storageIpAddr.getBytes(ClientGlobal.G_CHARSET);
        if (bIpAddr.length < ProtoCommon.FDFS_IPADDR_SIZE) {
            ipAddrLen = bIpAddr.length;
        } else {
            ipAddrLen = ProtoCommon.FDFS_IPADDR_SIZE - 1;
        }

        header = ProtoCommon.packHeader(ProtoCommon.TRACKER_PROTO_CMD_SERVER_DELETE_STORAGE, ProtoCommon.FDFS_GROUP_NAME_MAX_LEN + ipAddrLen, (byte) 0);
        byte[] wholePkg = new byte[header.length + bGroupName.length + ipAddrLen];
        System.arraycopy(header, 0, wholePkg, 0, header.length);
        System.arraycopy(bGroupName, 0, wholePkg, header.length, bGroupName.length);
        System.arraycopy(bIpAddr, 0, wholePkg, header.length + bGroupName.length, ipAddrLen);
        out.write(wholePkg);

        ProtoCommon.RecvPackageInfo pkgInfo = ProtoCommon.recvPackage(trackerSocket.getInputStream(),
                ProtoCommon.TRACKER_PROTO_CMD_RESP, 0);
        errno = pkgInfo.errno;
        return pkgInfo.errno == 0;
    }

    /**
     * delete a storage server from the global FastDFS cluster
     *
     * @param groupName     the group name of storage server
     * @param storageIpAddr the storage server ip address
     * @return true for success, false for fail
     */
    public boolean deleteStorage(String groupName, String storageIpAddr) throws IOException {
        return this.deleteStorage(ClientGlobal.G_TRACKER_GROUP, groupName, storageIpAddr);
    }

    /**
     * delete a storage server from the FastDFS cluster
     *
     * @param trackerGroup  the tracker server group
     * @param groupName     the group name of storage server
     * @param storageIpAddr the storage server ip address
     * @return true for success, false for fail
     */
    public boolean deleteStorage(TrackerGroup trackerGroup,
                                 String groupName, String storageIpAddr) throws IOException {
        int serverIndex;
        int notFoundCount;
        TrackerServer trackerServer;

        notFoundCount = 0;
        InetSocketAddress[] trackerServers = trackerGroup.getTrackerServers();
        for (serverIndex = 0; serverIndex < trackerServers.length; serverIndex++) {
            try {
                trackerServer = trackerGroup.getTrackerServer(serverIndex);
            } catch (IOException ex) {
                ex.printStackTrace(System.err);
                errno = ProtoCommon.ECONNREFUSED;
                return false;
            }

            try {
                StructStorageStat[] storageStats = listStorages(trackerServer, groupName, storageIpAddr);
                if (storageStats == null) {
                    if (errno == ProtoCommon.ERR_NO_ENOENT) {
                        notFoundCount++;
                    } else {
                        return false;
                    }
                } else if (storageStats.length == 0) {
                    notFoundCount++;
                } else if (storageStats[0].getStatus() == ProtoCommon.FDFS_STORAGE_STATUS_ONLINE ||
                        storageStats[0].getStatus() == ProtoCommon.FDFS_STORAGE_STATUS_ACTIVE) {
                    errno = ProtoCommon.ERR_NO_EBUSY;
                    return false;
                }
            } finally {
                try {
                    trackerServer.close();
                } catch (IOException ex1) {
                    ex1.printStackTrace();
                }
            }
        }

        if (notFoundCount == trackerServers.length) {
            errno = ProtoCommon.ERR_NO_ENOENT;
            return false;
        }

        notFoundCount = 0;
        for (serverIndex = 0; serverIndex < trackerServers.length; serverIndex++) {
            try {
                trackerServer = trackerGroup.getTrackerServer(serverIndex);
            } catch (IOException ex) {
                System.err.println("connect to server " + trackerServers[serverIndex].getAddress().getHostAddress() + ":" + trackerServers[serverIndex].getPort() + " fail");
                ex.printStackTrace(System.err);
                errno = ProtoCommon.ECONNREFUSED;
                return false;
            }

            try {
                if (!this.deleteStorage(trackerServer, groupName, storageIpAddr)) {
                    if (errno != 0) {
                        if (errno == ProtoCommon.ERR_NO_ENOENT) {
                            notFoundCount++;
                        } else if (errno != ProtoCommon.ERR_NO_EALREADY) {
                            return false;
                        }
                    }
                }
            } finally {
                try {
                    trackerServer.close();
                } catch (IOException ex1) {
                    ex1.printStackTrace();
                }
            }
        }

        if (notFoundCount == trackerServers.length) {
            errno = ProtoCommon.ERR_NO_ENOENT;
            return false;
        }

        if (errno == ProtoCommon.ERR_NO_ENOENT) {
            errno = 0;
        }

        return errno == 0;
    }
}
