import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by timbrooks on 3/25/15.
 */
public class LogWriter implements Runnable {

    private final static int FILE_SIZE = 536870912;
    private final static int PAGE_SIZE = 4096;
    private final static int MAX_WRITE_COUNT = 131072;
    private final LogSerializer logSerializer;
    private final FileNameFn fileNameFn;
    private final ErrorCallback errorCallback;
    private final BlockingQueue<Object> queue;
    private volatile int fence = 1;

    public LogWriter(BlockingQueue<Object> queue, LogSerializer logSerializer, FileNameFn fileNameFn, ErrorCallback
            errorCallback) {
        this.queue = queue;
        this.logSerializer = logSerializer;
        this.fileNameFn = fileNameFn;
        this.errorCallback = errorCallback;
    }

    @Override
    public void run() {
        ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);
        final AtomicReference<String> filepath = new AtomicReference<>(fileNameFn.generateFileName());

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                if (fence == 1) {
                    try (FileChannel channel = new RandomAccessFile(filepath.get(), "rw").getChannel()) {
                        flushBuffer(buffer, channel);
                    } catch (IOException e) {
                        errorCallback.error(e);
                    }
                }
            }
        }));

        while (true) {
            writer(filepath.get(), buffer);
            filepath.set(fileNameFn.generateFileName());
        }
    }

    private void writer(String filePath, ByteBuffer buffer) {

        try (FileChannel channel = new RandomAccessFile(filePath, "rw").getChannel()) {
            for (int i = 0; i < MAX_WRITE_COUNT; ++i) {
                String logMessage = logLine(queue.take());
                byte[] bytes = logMessage.getBytes();
                int messageSize = bytes.length;
                if (messageSize > PAGE_SIZE) {
                    throw new RuntimeException("Message is too Long!");
                } else if (messageSize > buffer.remaining()) {
                    flushBuffer(buffer, channel);
                }
                buffer.put(bytes, 0, messageSize);
            }
        } catch (IOException e) {
            errorCallback.error(e);
        } catch (InterruptedException e) {
            Thread.interrupted();
            e.printStackTrace();
        }
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

    private String logLine(Object logMessage) {
        String serializedMessage = logSerializer.serialize(logMessage);
        return serializedMessage + "\n";
    }
}


