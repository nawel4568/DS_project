package refactor;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class Utils {
    public static final boolean DEBUG = true;

    private static final Random rand = new Random();

    public static void delay() {
        try {Thread.sleep(rand.nextInt(50));} catch (Exception ignored) {}
    }

    public static void clearFileContent(String fileName) {
        try (FileWriter fw = new FileWriter(fileName, false)) {
            // Creating FileWriter in non-append mode clears the file content
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static class FileAdd {
        private final String fileName;

        public FileAdd(String fileName) {
            this.fileName = fileName;
        }



        public void appendToFile(String content) {
            try (FileWriter fw = new FileWriter(fileName, true)) {
                fw.write(content);
                fw.write(System.lineSeparator()); // Add a new line after each content
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

}
