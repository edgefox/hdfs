package org.apache.mesos.hdfs.executor;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.apache.mesos.hdfs.util.Trie;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class BackupExecutor implements Executor {
    private static final Log log = LogFactory.getLog(BackupExecutor.class);

    private final HdfsFrameworkConfig hdfsFrameworkConfig;
    private final Kryo kryo;
    private final FileSystem srcDfs;
    private final FileSystem dstDfs;
    private boolean stopped;

    /**
     * The constructor for the node which saves the configuration.
     */
    @Inject
    BackupExecutor(HdfsFrameworkConfig hdfsFrameworkConfig) throws IOException {
        this.hdfsFrameworkConfig = hdfsFrameworkConfig;
        this.kryo = new Kryo();
        this.srcDfs = FileSystem.get(URI.create(String.format("hdfs://%s", hdfsFrameworkConfig.getFrameworkName())), new Configuration());
        this.dstDfs = FileSystem.get(URI.create(hdfsFrameworkConfig.getBackupDestination()), new Configuration());
    }

    public static void main(String[] args) {
        Injector injector = Guice.createInjector();
        MesosExecutorDriver driver = new MesosExecutorDriver(
                injector.getInstance(BackupExecutor.class));
        System.exit(driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1);
    }

    @Override
    public void registered(ExecutorDriver driver, Protos.ExecutorInfo executorInfo, Protos.FrameworkInfo frameworkInfo, Protos.SlaveInfo slaveInfo) {
        log.info("Executor registered with the slave");
    }

    @Override
    public void reregistered(ExecutorDriver driver, Protos.SlaveInfo slaveInfo) {
        log.info("Executor reregistered with the slave");
    }

    @Override
    public void disconnected(ExecutorDriver driver) {
        log.info("Executor disconnected from the slave");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void launchTask(ExecutorDriver driver, Protos.TaskInfo task) {
        driver.sendStatusUpdate(Protos.TaskStatus.newBuilder()
                .setExecutorId(Protos.ExecutorID.newBuilder().setValue(HDFSConstants.BACKUP_EXECUTOR_ID))
                .setState(Protos.TaskState.TASK_RUNNING).build());
        final Input input = new Input(task.getData().newInput());
        Trie<String, Event> fsEventTree = (Trie<String, Event>)kryo.readObject(input, Trie.class);
        try {
            processChanges(fsEventTree);
        } catch (IOException e) {
            //TODO: what to do next?
            e.printStackTrace();
        }
        driver.sendStatusUpdate(Protos.TaskStatus.newBuilder()
                .setExecutorId(Protos.ExecutorID.newBuilder().setValue(HDFSConstants.BACKUP_EXECUTOR_ID))
                .setState(Protos.TaskState.TASK_FINISHED).build());
    }

    @Override
    public void killTask(ExecutorDriver driver, Protos.TaskID taskId) {
        log.info("Kill signal from master");
        stopped = true;
    }

    @Override
    public void frameworkMessage(ExecutorDriver driver, byte[] data) {
        log.info(String.format("Message from master: %s", String.valueOf(data)));
    }

    @Override
    public void shutdown(ExecutorDriver driver) {
        log.info("Shutting down the framework");
        stopped = true;
        driver.sendStatusUpdate(Protos.TaskStatus.newBuilder()
                .setExecutorId(Protos.ExecutorID.newBuilder().setValue(HDFSConstants.BACKUP_EXECUTOR_ID))
                .setState(Protos.TaskState.TASK_FINISHED).build());
    }

    @Override
    public void error(ExecutorDriver driver, String message) {
        log.error(this.getClass().getName() + ".error: " + message);
    }

    private void processChanges(Trie<String, Event> fsTree) throws IOException {
        final List<Path> pathsToDelete = processNonDeleteChanges(fsTree);
        if (!stopped) {
            for (Path deletePath : pathsToDelete) {
                dstDfs.delete(deletePath, true);
            }
        }
    }

    private List<Path> processNonDeleteChanges(Trie<String, Event> fsTree) throws IOException {
        List<Path> pathsToDelete = new ArrayList<>();
        for (Trie<String, Event> entry : fsTree.getChildren().values()) {
            if (stopped) {
                return pathsToDelete;
            }

            final Event event = entry.getModel();
            if (event == null) {
                continue;
            }

            switch (event.getEventType()) {
                //TODO: not sure that we should handle append event, since we can do that when close is triggered, thus making less requests.
/*                case APPEND: {
                    Event.AppendEvent actualEvent = ((Event.AppendEvent) event);
                    final Path dfsPath = new Path(actualEvent.getPath());
                    FileUtil.copy(srcDfs, dfsPath, dstDfs, dfsPath, false, true, new Configuration());

                    break;
                }*/
                case CLOSE: {
                    Event.CloseEvent actualEvent = ((Event.CloseEvent) event);
                    final Path dfsPath = new Path(actualEvent.getPath());
                    FileUtil.copy(srcDfs, dfsPath, dstDfs, dfsPath, false, true, new Configuration());

                    break;
                }
                case CREATE: {
                    Event.CreateEvent actualEvent = ((Event.CreateEvent) event);
                    final Path dfsPath = new Path(actualEvent.getPath());
                    if (actualEvent.getiNodeType() == Event.CreateEvent.INodeType.DIRECTORY) {
                        dstDfs.mkdirs(dfsPath);
                    } else {
                        FileUtil.copy(srcDfs, dfsPath, dstDfs, dfsPath, false, true, new Configuration());
                    }

                    break;
                }
                case METADATA: {
                    Event.MetadataUpdateEvent actualEvent = ((Event.MetadataUpdateEvent) event);
                    final Path dfsPath = new Path(actualEvent.getPath());
                    dstDfs.setAcl(dfsPath, actualEvent.getAcls());
                    dstDfs.setOwner(dfsPath, actualEvent.getOwnerName(), actualEvent.getGroupName());
                    dstDfs.setPermission(dfsPath, actualEvent.getPerms());
                    dstDfs.setReplication(dfsPath, (short) actualEvent.getReplication());
                    for (XAttr xAttr : actualEvent.getxAttrs()) {
                        dstDfs.setXAttr(dfsPath, xAttr.getName(), xAttr.getValue());
                    }
                    dstDfs.setTimes(dfsPath, actualEvent.getMtime(), actualEvent.getAtime());

                    break;
                }
                case RENAME: {
                    Event.RenameEvent actualEvent = ((Event.RenameEvent) event);
                    final Path srcPath = new Path(actualEvent.getSrcPath());
                    final Path dstPath = new Path(actualEvent.getDstPath());
                    FileUtil.copy(dstDfs, srcPath, dstDfs, dstPath, true, true, new Configuration());

                    break;
                }
                case UNLINK: {
                    Event.UnlinkEvent actualEvent = ((Event.UnlinkEvent) event);
                    final Path dfsPath = new Path(actualEvent.getPath());
                    pathsToDelete.add(dfsPath);

                    break;
                }
            }

            if (!entry.getChildren().isEmpty()) {
                pathsToDelete.addAll(processNonDeleteChanges(entry));
            }
        }

        return pathsToDelete;
    }
}
