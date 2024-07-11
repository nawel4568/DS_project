package org.example;

import java.io.FileWriter;
import java.io.IOException;

class FileAdd{
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
