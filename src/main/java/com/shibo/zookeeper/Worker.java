package com.shibo.zookeeper;

import com.sun.jmx.snmp.SnmpUnknownAccContrModelException;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

/**
 * 从节点
 *
 * @author by shibo on 2020/5/21.
 */
public class Worker implements Watcher {
    private static final Logger logger = LoggerFactory.getLogger(Worker.class);
    private Random random = new Random(this.hashCode());
    ZooKeeper zk;
    String hostPort;
    String serverId = Integer.toHexString(random.nextInt());
    private String status;
    private String name
;

    public Worker(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    @Override
    public void process(WatchedEvent event) {
        logger.error(event.toString() + "," + hostPort);
    }

    void register() {
        zk.create("/workers/work-" + serverId,
                "Idle".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                createWorkerCallback, null);
    }

    AsyncCallback.StringCallback createWorkerCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    register();
                    return;
                case OK:
                    logger.info("register successfully:" + serverId);
                    break;
                case NODEEXISTS:
                    logger.warn("Already registered :" + serverId);
                    break;
                default:
                    // 失败后重新尝试创建
                    logger.error("something wrong:", KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    AsyncCallback.StatCallback statusUpdateCallback = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    updateStatus((String) ctx);
                    return;
                default:
            }
        }
    };

    synchronized private void updateStatus(String status) {
        if (this.status.equals(status)) {
            zk.setData("workers/" + name, status.getBytes(), -1, statusUpdateCallback, status);
        }
    }

    private void setStatus(String status) {
        this.status = status;
        updateStatus(status);
    }

    public static void main(String[] args) throws Exception {
        Worker worker = new Worker("2188");
        worker.startZK();
        worker.register();
        Thread.sleep(1000);
    }
}
