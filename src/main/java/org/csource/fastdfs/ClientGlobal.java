/**
 * Copyright (C) 2008 Happy Fish / YuQing
 * <p>
 * FastDFS Java Client may be copied only under the terms of the GNU Lesser
 * General Public License (LGPL).
 * Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
 **/

package org.csource.fastdfs;

import org.csource.common.FastDFSClientException;
import org.csource.common.IniFileReader;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * Global variables
 *
 * @author Happy Fish / YuQing
 * @version Version 1.11
 */
public class ClientGlobal {
    public static int G_CONNECT_TIMEOUT; //millisecond
    public static int G_NETWORK_TIMEOUT; //millisecond
    public static String G_CHARSET;
    public static int G_TRACKER_HTTP_PORT;
    public static boolean G_ANTI_STEAL_TOKEN;  //if anti-steal token
    public static String G_SECRET_KEY;   //generage token secret key
    public static TrackerGroup G_TRACKER_GROUP;

    public static final int DEFAULT_CONNECT_TIMEOUT = 5;  //second
    public static final int DEFAULT_NETWORK_TIMEOUT = 30; //second

    private ClientGlobal() {
    }

    /**
     * load global variables
     *
     * @param confFilename config filename
     */
    public static void init(String confFilename) throws IOException, FastDFSClientException {
        IniFileReader iniReader = new IniFileReader(confFilename);
        String[] szTrackerServers;
        String[] parts;

        G_CONNECT_TIMEOUT = iniReader.getIntValue("connect_timeout", DEFAULT_CONNECT_TIMEOUT);
        if (G_CONNECT_TIMEOUT < 0) {
            G_CONNECT_TIMEOUT = DEFAULT_CONNECT_TIMEOUT;
        }
        G_CONNECT_TIMEOUT *= 1000; //millisecond

        G_NETWORK_TIMEOUT = iniReader.getIntValue("network_timeout", DEFAULT_NETWORK_TIMEOUT);
        if (G_NETWORK_TIMEOUT < 0) {
            G_NETWORK_TIMEOUT = DEFAULT_NETWORK_TIMEOUT;
        }
        G_NETWORK_TIMEOUT *= 1000; //millisecond

        G_CHARSET = iniReader.getStrValue("charset");
        if (G_CHARSET == null || G_CHARSET.length() == 0) {
            G_CHARSET = "ISO8859-1";
        }

        szTrackerServers = iniReader.getValues("tracker_server");
        if (szTrackerServers == null) {
            throw new FastDFSClientException("item \"tracker_server\" in " + confFilename + " not found");
        }

        InetSocketAddress[] tracker_servers = new InetSocketAddress[szTrackerServers.length];
        for (int i = 0; i < szTrackerServers.length; i++) {
            parts = szTrackerServers[i].split(":", 2);
            if (parts.length != 2) {
                throw new FastDFSClientException("the value of item \"tracker_server\" is invalid, the correct format is host:port");
            }

            tracker_servers[i] = new InetSocketAddress(parts[0].trim(), Integer.parseInt(parts[1].trim()));
        }
        G_TRACKER_GROUP = new TrackerGroup(tracker_servers);

        G_TRACKER_HTTP_PORT = iniReader.getIntValue("http.tracker_http_port", 80);
        G_ANTI_STEAL_TOKEN = iniReader.getBoolValue("http.anti_steal_token", false);
        if (G_ANTI_STEAL_TOKEN) {
            G_SECRET_KEY = iniReader.getStrValue("http.secret_key");
        }
    }

    /**
     * construct Socket object
     *
     * @param ip_addr ip address or hostname
     * @param port    port number
     * @return connected Socket object
     */
    public static Socket getSocket(String ip_addr, int port) throws IOException {
        Socket sock = new Socket();
        sock.setSoTimeout(ClientGlobal.G_NETWORK_TIMEOUT);
        sock.connect(new InetSocketAddress(ip_addr, port), ClientGlobal.G_CONNECT_TIMEOUT);
        return sock;
    }

    /**
     * construct Socket object
     *
     * @param addr InetSocketAddress object, including ip address and port
     * @return connected Socket object
     */
    public static Socket getSocket(InetSocketAddress addr) throws IOException {
        Socket sock = new Socket();
        sock.setSoTimeout(ClientGlobal.G_NETWORK_TIMEOUT);
        sock.connect(addr, ClientGlobal.G_CONNECT_TIMEOUT);
        return sock;
    }
}
