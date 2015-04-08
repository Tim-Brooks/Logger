import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by timbrooks on 4/8/15.
 */
public class LogWriterTest {

    private LogWriter writer;

    @Before
    public void setUp() {
        BlockingQueue<Object> queue = new ArrayBlockingQueue<>(1000);
        this.writer = new LogWriter(queue, new JsonLogSerializer(),
                new FileNameGenerator(),
                new ErrorHandler());
    }

    @Test
    public void testBufferNotFlushedPageSizeMet() {

    }

    private static class JsonLogSerializer implements LogSerializer {
        @Override
        public String serialize(Object object) {
            return "";
        }
    }

    private static class FileNameGenerator implements FileNameFn {
        @Override
        public String generateFileName() {
            return "";
        }
    }

    private static class ErrorHandler implements ErrorCallback {
        @Override
        public void error(Throwable error) {

        }
    }
}
