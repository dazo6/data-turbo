package com.dazo66;

import com.dazo66.data.turbo.util.FileChannelPool;
import com.dazo66.data.turbo.util.IOUtils;
import org.junit.Test;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author dazo66
 **/
public class IOTest {

    @Test
    public void test() throws IOException, InterruptedException {
        // 生成一个文件 再验证
        String fileName = "ioTest.test";
        File file = new File(fileName);
        file.createNewFile();
        FileOutputStream outputStream = new FileOutputStream(file);
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(outputStream);
        int count = 10240;
        byte[] bytes = new byte[count];
        Arrays.fill(bytes, ((byte) 0));
        bufferedOutputStream.write(bytes);
        bufferedOutputStream.flush();
        bufferedOutputStream.close();

        FileChannelPool pool = new FileChannelPool(file);
        Random random = new Random(fileName.hashCode());
        ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 10, 10L, TimeUnit.MINUTES,
                new LinkedBlockingDeque<>());
        int all = 1000;
        for (int i = 0; i < all; i++) {
            Random finalRandom1 = random;
            executor.execute(() -> {
                int r;
                synchronized (finalRandom1) {
                    r = finalRandom1.nextInt(count);
                }
                pool.writeOneBit(r);
            });
        }
        while (pool.getWriterQueueCount() != 0) {
            Thread.sleep(1000L);
        }
        // 重新初始化随机数
        CountDownLatch latch2 = new CountDownLatch(all);
        random = new Random(fileName.hashCode());
        for (int i = 0; i < all; i++) {
            Random finalRandom = random;
            executor.execute(() -> {
                FileChannel fileChannel = pool.getFileChannel();
                try {
                    int r;
                    synchronized (finalRandom) {
                        r = finalRandom.nextInt(count);
                    }
                    assert IOUtils.getOneBit(fileChannel, r);
                } catch (IOException e) {
                    System.out.println(e.getMessage());
                } finally {
                    pool.putFileChannel(fileChannel);
                    latch2.countDown();
                }
            });
        }
        try {
            latch2.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        executor.shutdown();
        file.delete();
    }


}
