import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by timbrooks on 4/8/15.
 */
public class LogWriterTest {

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    private LogWriter writer;
    private BlockingQueue<Object> queue;
    private FileNameGenerator fileNameFn;

    @Before
    public void setUp() {
        queue = new ArrayBlockingQueue<>(1000);
        Map<String, Integer> config = new HashMap<>();
        config.put("pageSize", 16);
        config.put("fileSize", 64);

        this.fileNameFn = new FileNameGenerator(folder.getRoot());
        this.writer = new LogWriter(config, queue, new NoOpSerializer(), fileNameFn, new ErrorHandler());
    }

    @Test
    public void testBufferFlushedWhenPageSizeMet() throws Exception {
        File logFile = fileNameFn.getCurrentFile();

        new Thread(writer).start();

        String message = generateMessage(7);
        queue.add(message);

        assertTrue(pollForFile(logFile, 100));
        assertEquals(0, pollForFileLines(logFile, 1, 100).size());

        String message2 = generateMessage(7);
        queue.add(message2);

        List<String> lines = pollForFileLines(logFile, 2, 100);

        assertEquals(2, lines.size());
        assertEquals(message, lines.get(0));
        assertEquals(message2, lines.get(1));

        String message3 = generateMessage(1);

        assertEquals(2, pollForFileLines(logFile, 2, 100).size());
        queue.add(message3);

        String message4 = generateMessage(15);
        queue.add(message4);

        lines = pollForFileLines(logFile, 3, 100);
        assertEquals(3, lines.size());
        assertEquals(message3, lines.get(2));


        writer.safeStop(1000, TimeUnit.MILLISECONDS);

    }

    private boolean pollForFile(File file, long millisTimeout) throws Exception {
        long timeSpentPolling = 0;
        long start = System.currentTimeMillis();
        while (millisTimeout > timeSpentPolling) {
            if (file.exists()) {
                return true;
            }
            timeSpentPolling = System.currentTimeMillis() - start;
            Thread.sleep(10);
        }
        return false;
    }

    private String generateMessage(int byteCount) {
        char[] chars = "abcdefghijklmnopqrstuvwxyz".toCharArray();
        byte[] message = new byte[byteCount];
        Random random = new Random();
        for (int i = 0; i < message.length; ++i) {
            message[i] = (byte) chars[random.nextInt(25)];
        }
        return new String(message);
    }

    private List<String> pollForFileLines(File file, int lineCount, long millisTimeout) throws Exception {
        List<String> lines = new ArrayList<>();

        long timeSpentPolling = 0;
        long start = System.currentTimeMillis();
        while (millisTimeout > timeSpentPolling) {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
            if (lines.size() == lineCount) {
                return lines;
            }
            lines = new ArrayList<>();
            timeSpentPolling = System.currentTimeMillis() - start;
            Thread.sleep(10);
        }
        return lines;
    }

    private static class ErrorHandler implements ErrorCallback {
        @Override
        public void error(Throwable error) {

        }
    }
}
