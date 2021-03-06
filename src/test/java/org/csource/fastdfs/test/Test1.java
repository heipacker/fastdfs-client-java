package org.csource.fastdfs.test;

import org.csource.common.NameValuePair;
import org.csource.fastdfs.*;

import java.net.InetSocketAddress;

public class Test1 {
    public static void main(String args[]) {
        try {
            ClientGlobal.init("src/main/resources/fdfs_client.conf");
            System.out.println("network_timeout=" + ClientGlobal.G_NETWORK_TIMEOUT + "ms");
            System.out.println("charset=" + ClientGlobal.G_CHARSET);

            TrackerGroup tg = new TrackerGroup(new InetSocketAddress[]{new InetSocketAddress("192.168.0.196", 22122)});
            TrackerClient tc = new TrackerClient(tg);

            TrackerServer ts = tc.getTrackerServer();
            if (ts == null) {
                System.out.println("getTrackerServer return null");
                return;
            }

            StorageServer ss = tc.getStorageServer(ts);
            if (ss == null) {
                System.out.println("getStorageServer return null");
            }

            StorageClient1 sc1 = new StorageClient1(ts, ss);

            NameValuePair[] meta_list = null;  //new NameValuePair[0];
            String item = "c:/windows/system32/notepad.exe";
            String fileid = sc1.upload_file1(item, "exe", meta_list); //�����쳣

            System.out.println("Upload local file " + item + " ok, fileid=" + fileid);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }
}
