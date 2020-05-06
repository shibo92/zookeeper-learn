package com.shibo.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author by shibo on 2020/5/6.
 */
public class Master implements org.apache.zookeeper.Watcher {
    private static final Logger logger = LoggerFactory.getLogger(Master.class);

    ZooKeeper zk;
    String hostPort;

    public Master(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZk() {
        try {
            zk = new ZooKeeper(hostPort, 15000, this);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void process(WatchedEvent event) {
        System.out.println(event);
    }

    public static void main(String[] args) throws InterruptedException {
        Master m = new Master("localhost:2181");
        m.startZk();
        Thread.sleep(60000);
    }
}
