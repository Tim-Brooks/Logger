package main.java;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by timbrooks on 1/17/15.
 */
public class Main {

    public static void main(String[] args) {
        System.out.println(16 >> 1);
        System.out.println((1 << 13) >>> 1);
//        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1000, TimeUnit.MINUTES, new
//                ArrayBlockingQueue<>(1));
//
//        CountDownLatch latch = new CountDownLatch(1);
//
//        CompletableFuture<Integer> t = CompletableFuture.supplyAsync(() -> {
//            try {
//                latch.await();
//            } catch (InterruptedException e) {
//
//            }
//            return 4;
//        }, executor).thenApply(i
//                -> i * 8);
//
//        CompletableFuture<Integer> l = CompletableFuture.supplyAsync(() -> {
//            try {
//                latch.await();
//            } catch (InterruptedException e) {
//
//            }
//            return 4;
//        }, executor).thenApply(i
//                -> i * 8);
//        CompletableFuture<Integer> j = CompletableFuture.supplyAsync(() -> 4, executor).thenApply(i -> i * 8);
//        System.out.println(t.join());
//        System.out.println(l.join());
//        System.out.println(j.join());

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
