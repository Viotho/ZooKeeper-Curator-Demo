import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CuratorDemo {

    public static CuratorFramework init() {
        String connectionString = "host:port,host:port";
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        return CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
    }

    public static CuratorFramework initWithBuilder() {
        // 退避重试策略
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        String connectString = "host:port,host:port";
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder().connectString(connectString)
                .sessionTimeoutMs(1000)
                .connectionTimeoutMs(1000)
                .retryPolicy(retryPolicy)
                // 创建隔离命名空间
                .namespace("namespace")
                .build();
        return curatorFramework;
    }

    public void curatorWithTransaction() throws Exception {
        try(CuratorFramework client = CuratorDemo.initWithBuilder();) {
            client.start();
            ArrayList<CuratorOp> curatorOps = new ArrayList<>();
            CuratorOp createParent = client.transactionOp().create().forPath("/node2", "content".getBytes());
            CuratorOp createChildren = client.transactionOp().create().forPath("/node2/node3", "content".getBytes());
            CuratorOp setData = client.transactionOp().setData().forPath("/node2", "new content".getBytes());

            curatorOps.add(createParent);
            curatorOps.add(createChildren);
            curatorOps.add(setData);

            List<CuratorTransactionResult> results = client.transaction().forOperations(curatorOps);
            for (CuratorTransactionResult result : results) {
                System.out.println(result.getForPath() + "-" + result.getType());
            }
        }
    }

    public void asynchronous() throws Exception {
        try(CuratorFramework client = CuratorDemo.initWithBuilder();) {
            client.start();
            ExecutorService executorService = Executors.newCachedThreadPool();
            client.create().creatingParentContainersIfNeeded().inBackground(new BackgroundCallback() {
                @Override
                public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
                    System.out.println("Code: " + curatorEvent.getResultCode());
                    System.out.println("Type: " + curatorEvent.getType());
                    System.out.println("Context: " + curatorEvent.getContext());
                }
            }, "context", executorService).forPath("/node4");
            executorService.shutdown();
        }
    }

    public static void main(String[] args) throws Exception {
        try(CuratorFramework client = CuratorDemo.initWithBuilder();) {
            client.start();
            client.create().creatingParentContainersIfNeeded()
                    .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
                    .forPath("/node1", "content".getBytes());

            byte[] bytes = client.getData().forPath("/node1");
            System.out.println(new String(bytes));

            Stat stat = new Stat();
            client.getData().storingStatIn(stat).forPath("/node1");
            client.setData().withVersion(1).forPath("/node1", "new content".getBytes());
            client.create().orSetData().forPath("/node2", "content".getBytes());

            client.checkExists().forPath("/node1");
            client.getChildren().forPath("/node1");
            client.delete().quietly().guaranteed().deletingChildrenIfNeeded().withVersion(1).forPath("/node1");
        }
    }
}