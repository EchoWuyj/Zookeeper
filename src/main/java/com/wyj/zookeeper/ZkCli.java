package com.wyj.zookeeper;

import com.sun.xml.internal.bind.v2.TODO;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;

/*
 *客户端逻辑通用步骤(Zookeeper客户端)
 * 1.创建客户端对象
 * 2.使用客户端对象进行操作
 * 3.关闭客户端
 */
public class ZkCli {
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        //参数解读
        //1.connectString 连接字符串
        //2.sessionTimeout 超时时长(多久联系不上就挂了)
        //3.watcher 客户端自带监听器

        //TODO 注意:连接字符串写一个节点也行,整体作为一个集群,会将请求打给leader
        String connectString="hadoop102:2181,hadoop103:2181,hadoop104:2181";
        int  sessionTimeout=5000;//单位毫秒

        ZooKeeper zk = new ZooKeeper(connectString, 5000, new Watcher() {
            @Override
           /*Watcher用来判断和是否和服务器连接还是断开了
            连接和关闭各执行一次process方法,一般里面啥都不写*/
            public void process(WatchedEvent event) {
            }
        });

        //TODO 获取子节点列表,不监听
        List<String> children = zk.getChildren("/", false);
        for (String child : children) {
            System.out.println(child);
        }
        zk.close();
    }
}
