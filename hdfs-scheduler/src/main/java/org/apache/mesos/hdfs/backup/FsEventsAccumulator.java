package org.apache.mesos.hdfs.backup;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.inotify.Event.EventType;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.util.Trie;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FsEventsAccumulator implements Runnable {
    private static final Log log = LogFactory.getLog(FsEventsAccumulator.class);
    private static final String ZK_OFFSET_PATH = "/hdfs/backup/offset";

    private final HdfsFrameworkConfig hdfsFrameworkConfig;
    private final Trie<String, Event> fsEventTree;
    private final ZkClient zkClient;
    private long lastCommittedTxId;
    private volatile long lastTxId;
    private boolean isStarted;

    private final ScheduledExecutorService backupExecutor;
    private final Kryo kryo = new Kryo();

    public FsEventsAccumulator(HdfsFrameworkConfig hdfsFrameworkConfig) {
        this.hdfsFrameworkConfig = hdfsFrameworkConfig;
        this.fsEventTree = new Trie<>();
        this.backupExecutor = Executors.newScheduledThreadPool(2);
        this.zkClient = new ZkClient(hdfsFrameworkConfig.getHaZookeeperQuorum());
        if (!zkClient.exists(ZK_OFFSET_PATH)) {
            zkClient.createPersistent(ZK_OFFSET_PATH, true);
        } else {
            String data = zkClient.readData(ZK_OFFSET_PATH);
            if (data != null && !data.isEmpty()) {
                this.lastTxId = Long.parseLong(data);
            }
        }
    }

    public void start() {
        backupExecutor.execute(this);
        isStarted = true;
    }

    public boolean isStarted() {
        return isStarted;
    }

    public synchronized byte[] flush() {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final Output output = new Output(baos);
        kryo.writeObject(output, fsEventTree);
        output.flush();

        fsEventTree.removeChildren();

        return baos.toByteArray();
    }

    public synchronized boolean hasPendingEvents() {
        return !fsEventTree.getChildren().isEmpty();
    }

    @Override
    public void run() {
        try {
            HdfsAdmin admin = new HdfsAdmin(
                    URI.create(String.format("hdfs://%s/", hdfsFrameworkConfig.getFrameworkName())),
                    new Configuration());
            final DFSInotifyEventInputStream eventStream;
            if (lastTxId > 0) {
                eventStream = admin.getInotifyEventStream(lastTxId);
            } else {
                eventStream = admin.getInotifyEventStream();
            }

            backupExecutor.scheduleAtFixedRate(new OffsetCommitter(), 5, 5, TimeUnit.SECONDS);

            EventBatch eventBatch;
            while (!Thread.currentThread().isInterrupted()) {
                eventBatch = eventStream.take();
                lastTxId = eventBatch.getTxid();
                for (org.apache.hadoop.hdfs.inotify.Event event : eventBatch.getEvents()) {
                    switch (event.getEventType()) {
                        case APPEND: {
                            org.apache.hadoop.hdfs.inotify.Event.AppendEvent actualEvent =
                                (org.apache.hadoop.hdfs.inotify.Event.AppendEvent) event;
                            final Path changedPath = Paths.get(actualEvent.getPath());
                            handleEvent(changedPath, new Event.AppendEvent(actualEvent));

                            break;
                        }
                        case CLOSE: {
                            org.apache.hadoop.hdfs.inotify.Event.CloseEvent actualEvent =
                                (org.apache.hadoop.hdfs.inotify.Event.CloseEvent) event;
                            final Path changedPath = Paths.get(actualEvent.getPath());
                            handleEvent(changedPath, new Event.CloseEvent(actualEvent));

                            break;
                        }
                        case CREATE: {
                            org.apache.hadoop.hdfs.inotify.Event.CreateEvent actualEvent =
                                (org.apache.hadoop.hdfs.inotify.Event.CreateEvent) event;
                            final Path changedPath = Paths.get(actualEvent.getPath());
                            handleEvent(changedPath, new Event.CreateEvent(actualEvent));

                            break;
                        }
                        case METADATA: {
                            org.apache.hadoop.hdfs.inotify.Event.MetadataUpdateEvent actualEvent =
                                (org.apache.hadoop.hdfs.inotify.Event.MetadataUpdateEvent) event;
                            final Path changedPath = Paths.get(actualEvent.getPath());
                            handleEvent(changedPath, new Event.MetadataUpdateEvent(actualEvent));

                            break;
                        }
                        case RENAME: {
                            org.apache.hadoop.hdfs.inotify.Event.RenameEvent actualEvent =
                                (org.apache.hadoop.hdfs.inotify.Event.RenameEvent)  event;
                            final Path to = Paths.get(actualEvent.getDstPath());
                            handleEvent(to, new Event.RenameEvent(actualEvent));

                            break;
                        }
                        case UNLINK: {
                            org.apache.hadoop.hdfs.inotify.Event.UnlinkEvent actualEvent =
                                (org.apache.hadoop.hdfs.inotify.Event.UnlinkEvent) event;
                            final Path changedPath = Paths.get(actualEvent.getPath());
                            handleEvent(changedPath, new Event.UnlinkEvent(actualEvent));

                            break;
                        }
                    }
                }
            }
        } catch (IOException | InterruptedException | MissingEventsException e) {
            log.error(e);
        }
    }

    public synchronized void handleEvent(Path changedPath, Event event) {
        Trie<String, Event> current = fsEventTree;
        for (Iterator<Path> iterator = changedPath.iterator(); iterator.hasNext(); ) {
            String key = iterator.next().getFileName().toString();
            if (iterator.hasNext()) {
                if (current.getChild(key) != null) {
                    current = current.getChild(key);
                } else {
                    current = new Trie<>(key, null);
                }
            } else {
                Trie<String, Event> child = new Trie<>(key, event);
                current = current.addChild(child);
                if (event.getEventType() == EventType.UNLINK) {
                    current.removeChildren();
                }
            }
        }
    }

    class OffsetCommitter implements Runnable {

        @Override
        public void run() {
            if (lastTxId != lastCommittedTxId) {
                zkClient.writeData(ZK_OFFSET_PATH, lastTxId);
                lastCommittedTxId = lastTxId;
            }
        }
    }
}

