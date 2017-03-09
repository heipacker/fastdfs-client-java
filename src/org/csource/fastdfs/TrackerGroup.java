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
 * Tracker server group
 * @author Happy Fish / YuQing
 * @version Version 1.17
 */
public class TrackerGroup {
    private Integer lock;
    private int trackerServerIndex;
    private InetSocketAddress[] trackerServers;

    /**
     * Constructor
     * @param tracker_servers tracker servers
     */
    public TrackerGroup(InetSocketAddress[] tracker_servers) {
        this.trackerServers = tracker_servers;
        this.lock = new Integer(0);
        this.trackerServerIndex = 0;
    }

    /**
     * return connected tracker server
     * @return connected tracker server, null for fail
     */
    public TrackerServer getConnection(int serverIndex) throws IOException {
        Socket sock = new Socket();
        sock.setReuseAddress(true);
        sock.setSoTimeout(ClientGlobal.G_NETWORK_TIMEOUT);
        sock.connect(this.trackerServers[serverIndex], ClientGlobal.G_CONNECT_TIMEOUT);
        return new TrackerServer(sock, this.trackerServers[serverIndex]);
    }

    /**
     * return connected tracker server
     * @return connected tracker server, null for fail
     */
    public TrackerServer getConnection() throws IOException {
        int current_index;

        synchronized (lock) {
            this.trackerServerIndex++;
            if (this.trackerServerIndex >= this.trackerServers.length) {
                this.trackerServerIndex = 0;
            }

            current_index = this.trackerServerIndex;
        }

        try {
            return this.getConnection(current_index);
        } catch (IOException ex) {
            System.err.println("connect to server " + this.trackerServers[current_index].getAddress().getHostAddress() + ":" + this.trackerServers[current_index].getPort() + " fail");
            ex.printStackTrace(System.err);
        }

        for (int i = 0; i < this.trackerServers.length; i++) {
            if (i == current_index) {
                continue;
            }

            try {
                TrackerServer trackerServer = this.getConnection(i);

                synchronized (lock) {
                    if (this.trackerServerIndex == current_index) {
                        this.trackerServerIndex = i;
                    }
                }

                return trackerServer;
            } catch (IOException ex) {
                System.err.println("connect to server " + this.trackerServers[i].getAddress().getHostAddress() + ":" + this.trackerServers[i].getPort() + " fail");
                ex.printStackTrace(System.err);
            }
        }

        return null;
    }

    public Object clone() {
        InetSocketAddress[] trackerServers = new InetSocketAddress[this.trackerServers.length];
        for (int i = 0; i < trackerServers.length; i++) {
            trackerServers[i] = new InetSocketAddress(this.trackerServers[i].getAddress().getHostAddress(), this.trackerServers[i].getPort());
        }

        return new TrackerGroup(trackerServers);
    }

    public Integer getLock() {
        return lock;
    }

    public void setLock(Integer lock) {
        this.lock = lock;
    }

    public int getTrackerServerIndex() {
        return trackerServerIndex;
    }

    public void setTrackerServerIndex(int trackerServerIndex) {
        this.trackerServerIndex = trackerServerIndex;
    }

    public InetSocketAddress[] getTrackerServers() {
        return trackerServers;
    }

    public void setTrackerServers(InetSocketAddress[] trackerServers) {
        this.trackerServers = trackerServers;
    }
}
