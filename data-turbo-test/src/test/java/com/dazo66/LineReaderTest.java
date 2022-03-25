package com.dazo66;

import com.dazo66.data.turbo.util.LineReader;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;

/**
 * @author dazo66
 **/
public class LineReaderTest {

    @Test
    public void test() throws IOException {
        PipedOutputStream outputStream = new PipedOutputStream();
        String testString = "|a|b|c|d|e|f|g|\n";
        PipedInputStream inputStream = new PipedInputStream(outputStream);
        outputStream.write(testString.getBytes(StandardCharsets.UTF_8));
        LineReader reader = new LineReader(new InputStreamReader(inputStream));
        Assert.assertEquals(reader.readLine(), testString.replace("\n", ""));
        inputStream.close();
        outputStream.close();
        PipedOutputStream outputStream1 = new PipedOutputStream();
        PipedInputStream inputStream1 = new PipedInputStream(outputStream1);
        reader = new LineReader(new InputStreamReader(inputStream1), '|');
        outputStream1.write(testString.getBytes(StandardCharsets.UTF_8));
        outputStream1.close();
        String[] split = testString.split("\\|");
        for (String s : split) {
            String line = reader.readLine();
            System.out.println("line: " + line);
            Assert.assertEquals(s, line);
        }

    }


}
