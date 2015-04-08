import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by timbrooks on 3/25/15.
 */
public class LogWriter implements Runnable {

    private final int pageSize;
    private final int maxWriteCount;
    private final LogSerializer logSerializer;
    private final FileNameFn fileNameFn;
    private final ErrorCallback errorCallback;
    private final BlockingQueue<Object> queue;
    private final Object sentinel = new Object();
    private final Lock shutdownLock = new ReentrantLock();
    private final Condition shutdownCondition = shutdownLock.newCondition();
    private volatile boolean running = true;

    public LogWriter(Map<String, Integer> configs, BlockingQueue<Object> queue, FileNameFn fileNameFn) {
        this(configs, queue, new NoOpSerializer(), fileNameFn, new DefaultErrorHandler());
    }

    public LogWriter(Map<String, Integer> configs, BlockingQueue<Object> queue, LogSerializer logSerializer,
                     FileNameFn fileNameFn, ErrorCallback errorCallback) {
        Integer configPageSize = configs.get("pageSize");
        Integer configFileSize = configs.get("fileSize");
        this.pageSize = configPageSize == null ? 4096 : configPageSize;
        int fileSize = configFileSize == null ? 536870912 : configFileSize;
        this.maxWriteCount = fileSize / pageSize;
        this.queue = queue;
        this.logSerializer = logSerializer;
        this.fileNameFn = fileNameFn;
        this.errorCallback = errorCallback;
    }

    @Override
    public void run() {
        final ByteBuffer buffer = ByteBuffer.allocate(pageSize);

        while (running) {
            write(fileNameFn.generateFileName(), buffer);
        }
    }

    public void safeStop(long timeout, TimeUnit unit) throws InterruptedException {
        if (timeout == -1) {
            queue.put(sentinel);
        } else {
            queue.offer(sentinel, timeout, unit);
        }
        try {
            shutdownLock.lock();
            if (timeout == -1) {
                shutdownCondition.await();
            } else {
                shutdownCondition.await(timeout, unit);
            }
        } finally {
            shutdownLock.unlock();
        }
    }

    public void unsafeStop() throws InterruptedException {
        this.running = false;
        queue.put(sentinel);
    }

    private void write(String filePath, ByteBuffer buffer) {

        try (FileChannel channel = new RandomAccessFile(filePath, "rw").getChannel()) {
            for (int i = 0; i < maxWriteCount; ++i) {
                Object message = queue.take();
                if (message != sentinel) {
                    handleMessage(buffer, channel, message);
                } else {
                    shutdown(buffer, channel);
                }
                if (!running) {
                    return;
                }
            }
        } catch (IOException e) {
            errorCallback.error(e);
        } catch (InterruptedException e) {
            Thread.interrupted();
            e.printStackTrace();
        }
    }

    private void handleMessage(ByteBuffer buffer, FileChannel channel, Object message) {
        String logMessage = logLine(message);
        byte[] bytes = logMessage.getBytes();
        int messageSize = bytes.length;
        if (messageSize > pageSize) {
            throw new RuntimeException("Message is too Long!");
        } else if (messageSize > buffer.remaining()) {
            flushBuffer(buffer, channel);
        }
        buffer.put(bytes, 0, messageSize);
    }

    private String logLine(Object logMessage) {
        String serializedMessage = logSerializer.serialize(logMessage);
        return serializedMessage + "\n";
    }

    private void flushBuffer(ByteBuffer buffer, FileChannel channel) {
        buffer.flip();
        try {
            channel.write(buffer);
        } catch (IOException e) {
            errorCallback.error(e);
        }
        buffer.clear();
    }

    private void shutdown(ByteBuffer buffer, FileChannel channel) {
        running = false;
        flushBuffer(buffer, channel);
        try {
            shutdownLock.lock();
            shutdownCondition.signalAll();
        } finally {
            shutdownLock.unlock();
        }
    }
}


