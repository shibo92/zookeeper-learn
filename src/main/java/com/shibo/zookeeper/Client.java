package com.shibo.zookeeper;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author by shibo on 2020/5/28.
 */
public class Client implements Watcher {
    private static final Logger logger = LoggerFactory.getLogger(Master.class);

    ZooKeeper zk;
    String hostPort;

    public Client(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZk() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    String queueCommand(String command) throws Exception {
        while (true) {
            try {
                String name = zk.create("/task/task-",
                        command.getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT_SEQUENTIAL);
                return name;
            } catch (KeeperException.NodeExistsException | InterruptedException e) {
                throw new Exception(" already appears to be running");
            } catch (KeeperException.ConnectionLossException e) {

            }
        }
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println(event);
    }

    public static void main(String[] args) throws Exception {
        Client c = new Client("127.0.0.1:2181");
        c.startZk();
        String name = c.queueCommand("c");
        System.out.println("Created ==> " + name);
    }
}
