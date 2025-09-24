import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class App {
    public static void main(String[] args) throws Exception {
        BlockingQueue<String> queue = new LinkedBlockingDeque<>();
        // TestThread productionThread1 = new TestThread("Producer Thread 1", false, queue);
        // TestThread productionThread2 = new TestThread("Producer Thread 2", false, queue);
        // TestThread consumerThread = new TestThread("Consumer Thread", true, queue);

        // productionThread1.start();
        // productionThread2.start();
        // consumerThread.start();

        TestThread productionThread1 = new TestThread("Producer Thread", false, queue);
        TestThread consumerThread = new TestThread("Consumer Thread", true, queue);

        consumerThread.start();
        productionThread1.start();
    }
}
