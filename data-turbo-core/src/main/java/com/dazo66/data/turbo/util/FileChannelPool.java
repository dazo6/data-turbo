package com.dazo66.data.turbo.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;


/**
 * @author dazo66
 */
public class FileChannelPool {

    private static final int CAPACITY = 50;
    private static final int QUEUE_MAX = 80;
    private static final AtomicLong CREATE_NUM = new AtomicLong();
    private static final AtomicLong KILL_NUM = new AtomicLong();
    public final FileChannel writerChannel;
    private final LinkedBlockingDeque<Long> writerQueue = new LinkedBlockingDeque<>(1000);
    private final Thread writeThread;
    private File file;
    private ConcurrentLinkedQueue<FileChannel> filePool;
    private volatile boolean running;
    private volatile boolean cleaning;
    private AtomicLong inNum;
    private AtomicLong outNum;

    public FileChannelPool(File file) throws FileNotFoundException {
        if (file == null || !file.exists() || file.isDirectory()) {
            throw new FileNotFoundException("The sourceFile doesn't exist or is a directory : " + file);
        }
        this.file = file;
        this.filePool = new ConcurrentLinkedQueue<>();
        this.inNum = new AtomicLong();
        this.outNum = new AtomicLong();
        this.running = true;
        this.cleaning = false;
        this.writerChannel = new RandomAccessFile(file, "rw").getChannel();
        this.writeThread = new Thread(() -> {
            long l = System.currentTimeMillis();
            while (true) {
                try {
                    IOUtils.setOneBit(writerChannel, writerQueue.take());
                    if (System.currentTimeMillis() - l > 10000) {
                        writerChannel.force(true);
                        l = System.currentTimeMillis();
                    }
                } catch (IOException e) {
                    System.out.println(e.getMessage());
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        writeThread.setDaemon(true);
        writeThread.start();
    }

    public static long getCreateNum() {
        return CREATE_NUM.get();
    }

    public static long getKillNum() {
        return KILL_NUM.get();
    }

    public int getWriterQueueCount() {
        return writerQueue.size();
    }

    public FileChannel getFileChannel() {
        FileChannel channel = filePool.poll();
        if (channel == null) {
            RandomAccessFile in;
            try {
                in = new RandomAccessFile(file, "r");
                channel = in.getChannel();
                CREATE_NUM.incrementAndGet();
            } catch (FileNotFoundException e) {
                throw new RuntimeException("Creating new sourceFile channel failed for sourceFile" +
                        " " + file, e);
            }
        } else {
            outNum.incrementAndGet();
        }
        return channel;
    }

    public void writeOneBit(long offsetBit) {
        try {
            writerQueue.put(offsetBit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void putFileChannel(FileChannel channel) {
        if (channel == null) {
            return;
        } else if (!channel.isOpen() || !running) {
            closeChannel(channel);
            return;
        }

        long cacheNum = inNum.get() - outNum.get();
        if (cacheNum > QUEUE_MAX) {
            clean();
            closeChannel(channel);
            return;
        }

        if (filePool.offer(channel)) {
            inNum.incrementAndGet();
        } else {
            closeChannel(channel);
        }
    }

    private synchronized void clean() {
        cleaning = true;
        while (filePool.size() > QUEUE_MAX / 2) {
            closeChannel(filePool.poll());
        }
        cleaning = false;
    }

    public synchronized void shutDown() throws Exception {
        if (running) {

            while (!filePool.isEmpty()) {
                closeChannel(filePool.poll());
            }
            writeThread.interrupt();
            writerChannel.close();
            running = false;
        }
    }

    public void flush() throws Exception {
        while (true) {
            if (!writerQueue.isEmpty()) {
                Thread.sleep(100L);
            } else {
                break;
            }
        }
        writerChannel.force(true);
    }

    private void closeChannel(FileChannel channel) {
        if (channel == null) {
            return;
        }
        try {
            channel.close();
            KILL_NUM.incrementAndGet();
        } catch (IOException e) {
            System.out.println(e);
        }
    }
}
