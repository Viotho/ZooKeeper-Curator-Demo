import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.*;

public class CuratorRecipeDemo {

    public void usingCuratorCache() {
        try(CuratorFramework client = CuratorDemo.initWithBuilder()) {
            client.start();
            CuratorCache curatorCache = CuratorCache.builder(client, "/node").build();
            CuratorCacheListener cacheListener = CuratorCacheListener.builder()
                    .forChanges((oldNode, node) -> System.out.println("Node changed. Old: " + oldNode  + "New: " + node))
                    .forCreates(node -> System.out.println("Node created: " + node))
                    .forDeletes(oldNode -> System.out.println("Node deleted. Old value: " + oldNode))
                    .forInitialized(() -> System.out.println("Cache Initialized"))
                    .build();

            curatorCache.listenable().addListener(cacheListener);
            curatorCache.start();
        }
    }

    public void curatorCacheAdaptor() {
        try(CuratorFramework client = CuratorDemo.initWithBuilder()) {
            client.start();
            CuratorCache curatorCache = CuratorCache.builder(client, "/node1").build();
            CuratorCacheListener oldVersionListener = CuratorCacheListener.builder()
                    .forNodeCache(() -> System.out.println("Node Changed."))
                    .forPathChildrenCache("/root", client, (theClient, event) -> {
                        System.out.println("Type: " + event.getType());
                        System.out.println("Path: " + event.getData().getPath());
                        System.out.println("Data: " + new String(event.getData().getData()));
                    })
                    .forTreeCache(client, (theClient, event) -> {
                        System.out.println("Type: " + event.getType());
                        System.out.println("Path: " + event.getData().getPath());
                        System.out.println("Data: " + new String(event.getData().getData()));
                    }).build();

            curatorCache.listenable().addListener(oldVersionListener);
            curatorCache.start();
        }
    }
}
