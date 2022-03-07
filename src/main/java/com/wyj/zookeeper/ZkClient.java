package com.wyj.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @Author Wuyj
 * @DateTime 2022-01-26 18:01
 * @Version 1.0
 */
public class ZkClient {

    private ZooKeeper zk;

    //初始化方法
    @Before
    public void init() throws IOException {
        String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
        int sessionTimeout = 5000;
        zk = new ZooKeeper(connectString, 5000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
            }
        });
    }

    //关闭方法
    @After
    public void close() throws InterruptedException {
        zk.close();
    }

    //1.获取子节点,并且不监听
    @Test
    public void ls() throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren("/", false);
        System.out.println(children);
    }

    //2.获取子节点,并且监听
    @Test
    public void lsAndWatch() throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren("/", new Watcher() {//本次监听单次有效
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event);//WatchedEvent state:Closed type:None path:null
                System.out.println("0426 is best of atguigu");
            }
        });

        //客户端退出时(此时没有线程睡眠),会将所有的没有监听到的事件都执行一遍,即触发process()方法
        System.out.println(children);

        //通过线程睡眠保证不让客户端关闭,这样才能保持一直监听的状态
        //即使触发回调函数,执行process()方法,但是Thread.sleep(Long.MAX_VALUE)方法也过不去,即一直卡线程这里
        Thread.sleep(Long.MAX_VALUE);
    }

    //3.获取子节点,并循环监听
    //通过递归实现,方法中得传入形参
    public void lsAndWatch(String path) throws KeeperException, InterruptedException {
        //将原来具体的路径替换成传入参数path
        List<String> children = zk.getChildren(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event);
                //TODO 触发一个监听,同时注册下一个监听
                try {
                    lsAndWatch(path);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        //lsAndWatch(path);死递归,一直在注册监听,后面 Thread.sleep(Long.MAX_VALUE)代码执行不到
        System.out.println(children);
    }

    //通过testLsAndWatch()方法来测试lsAndWatch()方法
    @Test
    public void testLsAndWatch() throws KeeperException, InterruptedException {
        lsAndWatch("/");
        //TODO 线程睡眠放到调用方法的中,防止客户端退出
        // 注意线程睡眠代码放的位置
        Thread.sleep(Long.MAX_VALUE);
    }

    //4.创建不同类型的节点
    @Test
    public void create() throws KeeperException, InterruptedException {
        //参数说明:
        //ZooDefs.Ids.OPEN_ACL_UNSAFE 设置权限为开放的,不安全的模式
        //"atguigu" 为创建节点的路径
        //"sgg".getBytes() 为节点对应值的字节数组
        //CreateMode是枚举类型,可以选择相应的节点类型

        //zk.create("/wyj", "xiaowu".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        //zk.create("/xiyouji/xiaobailong", "aobing".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

        //TODO 这种条件下创建临时节点是没法看到的,执行完该段代码之后,程序就已经退出了
        // 需要通过线程睡眠才能看到创建临时节点的效果
        //zk.create("/xiyouji/gaolaozhuang", "xiaozhu".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        zk.create("/xiyouji/huoyanshan", "红孩儿".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        Thread.sleep(Long.MAX_VALUE);
    }

    //5.获取节点的值,不监听
    @Test
    public void get() throws KeeperException, InterruptedException {

        /*
        写的粗糙一点
        byte[] data = zk.getData("/wyj", false, null);//状态结构体没有,则使用null代替
        System.out.println(new String(data));
        */

        //写的精致一点
        //zk.exists 返回的值为结构体对象
        Stat stat = zk.exists("/wyj", false);
        //对结构体进行判断
        if (stat == null) {
            System.out.println("你想查询的节点不存在");
            return;//结束方法,后续的代码不用执行
        }

        //再去进行值的获取
        byte[] data = zk.getData("/wyj", false, stat);
        System.out.println(new String(data));
    }

    //6.获取节点的值并监听
    @Test
    public void getAndWatch() throws KeeperException, InterruptedException {
        Stat stat = zk.exists("/wyj", false);
        if (stat == null) {
            System.out.println("你想查询的节点不存在");
            return;
        }
        byte[] data = zk.getData("/wyj", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event);
            }
        }, stat);
        System.out.println(new String(data));
        Thread.sleep(Long.MAX_VALUE);

        //在zookeeper客户端通过修改 set /wyj "wuyongjian"
        //控制台输出:WatchedEvent state:SyncConnected type:NodeDataChanged path:/wyj
    }

    //7.设置节点的值
    @Test
    public void set() throws KeeperException, InterruptedException {
        //写的精致一点
        //zk.exists 返回的值为结构体对象
        Stat stat = zk.exists("/wyj", false);
        //对结构体进行判断
        if (stat == null) {
            System.out.println("你想查询的节点不存在");
            return;
        }
        //通过stat对象将Version版本号传进去
        //TODO 必须保证版本号是正确的才能进行值设置,否则会报错的
        zk.setData("/wyj", "xiaojian".getBytes(), stat.getVersion());
    }

    //8.删除空节点
    @Test
    public void delete() throws KeeperException, InterruptedException {
        //写的精致一点
        //zk.exists 返回的值为结构体对象
        Stat stat = zk.exists("/wyj", false);
        if (stat == null) {
            System.out.println("你想查询的节点不存在");
            return;
        }
        zk.delete("/wyj", stat.getVersion());
    }

    //9.删除非空节点,递归实现
    //TODO 不要写test注解,通过testDelete()方法来调用即可
    public void delete(String path) throws KeeperException, InterruptedException {
        Stat stat = zk.exists(path, false);
        //对结构体进行判断
        if (stat == null) {
            System.out.println("你想查询的节点不存在");
            return;
        }
        //获取子节点
        //TODO 先从最内层的节点开始删除
        List<String> children = zk.getChildren(path, false);
        for (String child : children) {
            //System.out.println(child);打印就是该节点
            //删除时都是从根目录开始删除的,通过拼接的方式将子节点的路径写完整,获取递归的删除路径
            delete(path + "/" + child);
        }

        //真正的删除逻辑
        zk.delete(path, stat.getVersion());
    }

    //测试:删除非空节点
    @Test
    public void testDelete() throws KeeperException, InterruptedException {
        delete("/xiyouji");
    }
}

