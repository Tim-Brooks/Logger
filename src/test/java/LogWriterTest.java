import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

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
        this.writer = new LogWriter(config, queue, new NoOpSerializer(), fileNameFn, new ErrorHandler());
    }

    @Test
    public void testBufferNotFlushedPageSizeNotMet() throws Exception {
        new Thread(writer).start();

        char[] chars = "abcdefghijklmnopqrstuvwxyz".toCharArray();
        byte[] message = new byte[15];
        Random random = new Random();
        for (int i = 0; i < message.length; ++i) {
            message[i] = (byte) chars[random.nextInt(25)];
        }

        String stringMessage = new String(message);

        queue.add(stringMessage);

        writer.safeStop(-1, TimeUnit.MILLISECONDS);

        BufferedReader reader = new BufferedReader(new FileReader(new File(folder.getRoot(), "log0")));
        assertEquals(stringMessage, reader.readLine());

    }

    private class FileNameGenerator implements FileNameFn {
        volatile int count = 0;

        @Override
        public String generateFileName() {
            File file = new File(folder.getRoot(), "log" + count++);
            return file.getPath();
        }
    }

    private static class ErrorHandler implements ErrorCallback {
        @Override
        public void error(Throwable error) {

        }
    }
}
