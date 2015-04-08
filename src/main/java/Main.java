import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by timbrooks on 1/17/15.
 */
public class Main {

    public static void main(String[] args) {
        System.out.println(16 >> 1);
        System.out.println((1 << 13) >>> 1);

        BlockingQueue<Object> queue = new ArrayBlockingQueue<>(10000);
        LogWriter logWriter = new LogWriter(queue, new LogSerializer() {
            @Override
            public String serialize(Object object) {
                return "New line bro2";
            }
        }, new FileNameFn() {
            @Override
            public String generateFileName() {
                return "/Users/timbrooks/Desktop/nooo";
            }
        }, new ErrorCallback() {
            @Override
            public void error(Throwable throwable) {
                throwable.printStackTrace();
            }
        });

        for (int i = 0; i < 1000; ++i) {
            queue.add(i);
        }
    }
}
