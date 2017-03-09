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
import java.net.Socket;

/**
 * Server Info
 *
 * @author Happy Fish / YuQing
 * @version Version 1.7
 */
public class ServerInfo {

    private String ipAddr;
    private int port;

    /**
     * Constructor
     *
     * @param ip_addr address of the server
     * @param port    the port of the server
     */
    public ServerInfo(String ip_addr, int port) {
        this.ipAddr = ip_addr;
        this.port = port;
    }

    /**
     * return the ip address
     *
     * @return the ip address
     */
    public String getIpAddr() {
        return this.ipAddr;
    }

    /**
     * return the port of the server
     *
     * @return the port of the server
     */
    public int getPort() {
        return this.port;
    }

    /**
     * connect to server
     *
     * @return connected Socket object
     */
    public Socket connect() throws IOException {
        Socket sock = new Socket();
        sock.setReuseAddress(true);
        sock.setSoTimeout(ClientGlobal.G_NETWORK_TIMEOUT);
        sock.connect(new InetSocketAddress(ipAddr, port), ClientGlobal.G_CONNECT_TIMEOUT);
        return sock;
    }

    public void setIpAddr(String ipAddr) {
        this.ipAddr = ipAddr;
    }

    public void setPort(int port) {
        this.port = port;
    }
}