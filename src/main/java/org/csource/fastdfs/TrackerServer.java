/**
 * Copyright (C) 2008 Happy Fish / YuQing
 * <p>
 * FastDFS Java Client may be copied only under the terms of the GNU Lesser
 * General Public License (LGPL).
 * Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
 */

package org.csource.fastdfs;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * Tracker Server Info
 *
 * @author Happy Fish / YuQing
 * @version Version 1.11
 */
public class TrackerServer implements Closeable {
    private Socket socket;
    private InetSocketAddress inetSocketAddress;

    /**
     * Constructor
     *
     * @param socket            Socket of server
     * @param inetSocketAddress the server info
     */
    public TrackerServer(Socket socket, InetSocketAddress inetSocketAddress) {
        this.socket = socket;
        this.inetSocketAddress = inetSocketAddress;
    }

    /**
     * get the connected socket
     *
     * @return the socket
     */
    public Socket getSocket() throws IOException {
        if (this.socket == null) {
            this.socket = ClientGlobal.getSocket(this.inetSocketAddress);
        }

        return this.socket;
    }

    /**
     * get the server info
     *
     * @return the server info
     */
    public InetSocketAddress getInetSocketAddress() {
        return this.inetSocketAddress;
    }

    public OutputStream getOutputStream() throws IOException {
        return this.socket.getOutputStream();
    }

    public InputStream getInputStream() throws IOException {
        return this.socket.getInputStream();
    }

    public void close() throws IOException {
        if (this.socket != null) {
            try {
                ProtoCommon.closeSocket(this.socket);
            } finally {
                this.socket = null;
            }
        }
    }

    protected void finalize() throws Throwable {
        this.close();
    }
}
