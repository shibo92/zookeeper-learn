package com.shibo.zookeeper;


import org.apache.zookeeper.WatchedEvent;

/**
 * @author by shibo on 2020/5/6.
 */
public interface Watcher {
    void process(WatchedEvent event);
}
