import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by timbrooks on 4/8/15.
 */
public class LogWriterTest {

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    private LogWriter writer;
    private BlockingQueue<Object> queue;

    @Before
    public void setUp() {
        queue = new ArrayBlockingQueue<>(1000);
        Map<String, Integer> config = new HashMap<>();
        config.put("pageSize", 16);
        config.put("fileSize", 64);

        FileNameGenerator fileNameFn = new FileNameGenerator();
        System.out.println(fileNameFn.generateFileName());
        System.out.println(fileNameFn.generateFileName());
        this.writer = new LogWriter(config, queue, new JsonLogSerializer(), fileNameFn, new ErrorHandler());
    }

    @Test
    public void testBufferNotFlushedPageSizeNotMet() throws Exception {
        new Thread(writer).start();


        queue.add("Lolzer");

        writer.safeStop(-1, TimeUnit.MILLISECONDS);

    }

    private static class JsonLogSerializer implements LogSerializer {
        @Override
        public String serialize(Object object) {
            return object.toString();
        }
    }

    private class FileNameGenerator implements FileNameFn {
        volatile int count = 0;

        @Override
        public String generateFileName() {
            File file = new File(folder.getRoot(), "log" + ++count);
            return file.getPath();
        }
    }

    private static class ErrorHandler implements ErrorCallback {
        @Override
        public void error(Throwable error) {

        }
    }
}
