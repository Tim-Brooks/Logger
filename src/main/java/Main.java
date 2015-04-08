import java.util.HashMap;
import java.util.Map;

/**
 * Created by timbrooks on 1/17/15.
 */
public class Main {

    public static void main(String[] args) {
        System.out.println(16 >> 1);
        System.out.println((1 << 13) >>> 1);

        Map<String, Integer> configs = new HashMap<>();
        configs.put("pageSize", 4096);
        configs.put("fileSize", 536870914);

        Integer configPageSize = configs.get("pageSize");
        Integer configFileSize = configs.get("fileSize");
        int pageSize = configPageSize == null ? 4096 : configPageSize;
        int fileSize = configFileSize == null ? 536870912 : configFileSize;
        int maxWriteCount = fileSize / pageSize;

        System.out.println(pageSize);
        System.out.println(fileSize);
        System.out.println(maxWriteCount);

//        BlockingQueue<Object> queue = new ArrayBlockingQueue<>(10000);
//        LogWriter logWriter = new LogWriter(configs, queue, new LogSerializer() {
//            @Override
//            public String serialize(Object object) {
//                return "New line bro2";
//            }
//        }, new FileNameFn() {
//            @Override
//            public String generateFileName() {
//                return "/Users/timbrooks/Desktop/nooo";
//            }
//        }, new ErrorCallback() {
//            @Override
//            public void error(Throwable throwable) {
//                throwable.printStackTrace();
//            }
//        });
//
//        for (int i = 0; i < 1000; ++i) {
//            queue.add(i);
//        }
    }
}
