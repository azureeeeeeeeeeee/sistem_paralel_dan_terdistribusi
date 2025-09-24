import java.io.File;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;

public class TestThread extends Thread {
    private String threadName;
    private Boolean isConsumer; // consumer thread yang print sedangkan prodcuer thread yang baca file
    private BlockingQueue<String> queue; // Ini untk shared memory antara 2 therd yang mau dbiat

    TestThread(String name, Boolean isConsumer, BlockingQueue<String> queue) {
        this.threadName = name;
        this.isConsumer = isConsumer; 
        this.queue = queue; 
        System.out.println("Creating thread " + this.threadName);
    };

    public void run() {
        if (this.isConsumer) {
            try {
                // tampilkan line yang sudah dibaca thread 1
                String line = queue.poll();
                if (line == null) {
                    System.out.println(this.threadName + " : shared memory is currently empty");
                }
                while (!(line = queue.take()).equals("EOF")) {
                    System.out.println(this.threadName + " : " + line);
                }
            } catch (Exception e) {
                System.out.println(e.getLocalizedMessage());
                return;
            }
        } else {
            // Baca file txt
            File file = new File("src/main.txt");

            // cek line per line, lalu masukkan ke shred memory
            try (Scanner reader = new Scanner(file)) {
                while (reader.hasNextLine()) {
                    System.out.println(this.threadName + " adding line to shared memory");
                    queue.add(reader.nextLine());
                }
            } catch (Exception e) {
                System.out.println("Error: " + e.getLocalizedMessage());
                return;
            }
            queue.add("EOF");
        }
    }
}
