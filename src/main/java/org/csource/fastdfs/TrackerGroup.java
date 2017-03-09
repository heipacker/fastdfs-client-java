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
 *
 * @author Happy Fish / YuQing
 * @version Version 1.17
 */
public class TrackerGroup {

    private Object lock = new Object();

    private int trackerServerIndex;

    private InetSocketAddress[] trackerServers;

    /**
     * Constructor
     *
     * @param tracker_servers tracker servers
     */
    public TrackerGroup(InetSocketAddress[] tracker_servers) {
        this.trackerServers = tracker_servers;
        this.trackerServerIndex = 0;
    }

    /**
     * return connected tracker server
     *
     * @return connected tracker server, null for fail
     */
    public TrackerServer getTrackerServer(int serverIndex) throws IOException {
        Socket socket = new Socket();
        socket.setReuseAddress(true);
        socket.setSoTimeout(ClientGlobal.G_NETWORK_TIMEOUT);
        socket.connect(trackerServers[serverIndex], ClientGlobal.G_CONNECT_TIMEOUT);
        return new TrackerServer(socket, trackerServers[serverIndex]);
    }

    /**
     * return connected tracker server
     *
     * @return connected tracker server, null for fail
     */
    public TrackerServer getTrackerServer() throws IOException {
        int currentIndex;
        synchronized (lock) {
            trackerServerIndex++;
            if (trackerServerIndex >= trackerServers.length) {
                trackerServerIndex = 0;
            }
            currentIndex = trackerServerIndex;
        }
        try {
            return getTrackerServer(currentIndex);
        } catch (IOException ex) {
            InetSocketAddress trackerServer = trackerServers[currentIndex];
            System.err.println("connect to server " + trackerServer.getAddress().getHostAddress() + ":" + trackerServer.getPort() + " fail");
            ex.printStackTrace(System.err);
        }
        for (int i = currentIndex; i < trackerServers.length; i++) {
            try {
                TrackerServer trackerServer = getTrackerServer(i);
                synchronized (lock) {
                    if (trackerServerIndex == currentIndex) {
                        trackerServerIndex = i;
                    }
                }
                return trackerServer;
            } catch (IOException ex) {
                InetSocketAddress trackerServer = trackerServers[i];
                System.err.println("connect to server " + trackerServer.getAddress().getHostAddress() + ":" + trackerServer.getPort() + " fail");
                ex.printStackTrace(System.err);
            }
        }
        return null;
    }

    @Override
    public Object clone() {
        InetSocketAddress[] trackerServers = new InetSocketAddress[this.trackerServers.length];
        for (int i = 0; i < trackerServers.length; i++) {
            InetSocketAddress trackerServer = this.trackerServers[i];
            trackerServers[i] = new InetSocketAddress(trackerServer.getAddress().getHostAddress(), trackerServer.getPort());
        }
        return new TrackerGroup(trackerServers);
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
