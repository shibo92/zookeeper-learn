package com.shibo.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.Date;

/**
 * @author by shibo on 2020/5/28.
 */
public class AdminClient implements Watcher {
    ZooKeeper zk;
    String hostPort;

    public AdminClient(String hostPort) {
        this.hostPort = hostPort;
    }

    void start() throws Exception {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    void listState() throws KeeperException, InterruptedException {
        try {
            Stat stat = new Stat();
            byte masterData[] = zk.getData("/master", false, stat);
            Date startDate = new Date(stat.getCtime());
            System.out.println("Master:" + new String(masterData) + " since " + startDate);
        } catch (KeeperException.NoNodeException | InterruptedException e) {
            System.out.println("no master");
        }

        System.out.println("workers ===>");
        for (String w : zk.getChildren("/workers", false)) {
            byte data[] = zk.getData("/workers/" + w, false, null);
            String state = new String(data);
            System.out.println("\t" + w + " : " + state);

        }

        System.out.println("tasks ===>");
        for (String t : zk.getChildren("/assign", false)) {
            System.out.println("\t" + t);

        }
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println(event);
    }

    public static void main(String[] args) throws Exception {
        AdminClient c = new AdminClient("127.0.0.1:2181");
        c.start();
        c.listState();
    }
}
