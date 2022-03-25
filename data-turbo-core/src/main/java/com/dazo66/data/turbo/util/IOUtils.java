package com.dazo66.data.turbo.util;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author dazo66
 **/
public class IOUtils {

    private static final int READ_MAX = 10;
    public static String baseDir = System.getProperty("user.home");

    /**
     * 设置一个bit为1
     * 写入操作建议使用一个线程进行，使用一个线程加队列的模式。
     *
     * @param fileChannel file通道 一定要有写入权限
     * @param offset      偏移量
     * @throws IOException
     */
    public static boolean setOneBit(FileChannel fileChannel, long offset) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(1);
        fileChannel.position(offset >>> 3);
        byte b = ((byte) (1 << 7 >>> (offset % 8)));
        fileChannel.read(byteBuffer);
        byteBuffer.flip();
        if ((byteBuffer.get() & b) > 0) {
            return false;
        }
        byteBuffer.flip();
        byteBuffer.put(0, ((byte) (byteBuffer.get() | b)));
        byteBuffer.flip();
        fileChannel.position(offset >>> 3);
        while (byteBuffer.hasRemaining()) {
            fileChannel.write(byteBuffer);
        }
        return true;
    }

    /**
     * 设置一个bit为1
     *
     * @param fileChannel file通道
     * @param offset      偏移量
     * @throws IOException 如果出现了异常
     */
    public static boolean getOneBit(FileChannel fileChannel, long offset) throws IOException {
        fileChannel.position(offset);
        ByteBuffer byteBuffer = ByteBuffer.allocate(1);
        fileChannel.position(offset >>> 3);
        byte b = ((byte) (1 << 7 >>> (offset % 8)));
        fileChannel.read(byteBuffer);
        byteBuffer.flip();
        byte b1 = byteBuffer.get();
        return (b1 & b) != 0;
    }

    public static byte[] readBytes(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[1024];
        int len = in.read(buf);
        while (len != -1) {
            out.write(buf, 0, len);
            len = in.read(buf);
        }

        return out.toByteArray();
    }

    public static byte[] readBytes(FileChannel channel, long position, int size) throws IOException {
        long fileSize = channel.size();
        if (fileSize - position < size) {
            throw new IndexOutOfBoundsException("Reading size is out of the sourceFile bound " +
                    "which sourceFile size is " + fileSize + ", position is " + position + ", " +
                    "size is " + size);
        }

        channel.position(position);
        ByteBuffer indexBuffer = ByteBuffer.allocate(size);
        int indexLen = channel.read(indexBuffer);
        while (indexBuffer.hasRemaining() && indexLen > 0) {
            indexLen = channel.read(indexBuffer);
        }

        if (indexBuffer.remaining() > 0) {
            String msg =
                    "Expect to read " + size + " bytes, actually read " + (indexBuffer.limit() - indexBuffer.remaining()) + " bytes";
            throw new RuntimeException(msg);
        }

        return indexBuffer.array();
    }

    public static ByteBuffer readDirectBytes(FileChannel channel, long position, int indexBytes) throws IOException {
        long fileSize = channel.size();
        if (fileSize - position < indexBytes) {
            throw new IndexOutOfBoundsException("Reading size is out of the sourceFile bound " +
                    "which sourceFile size is " + fileSize + ", position is " + position + ", " +
                    "size is " + indexBytes);
        }

        channel.position(position);
        ByteBuffer result = ByteBuffer.allocateDirect(indexBytes);
        result.position(0);

        ByteBuffer buff = ByteBuffer.allocateDirect(1024);
        int indexLen = channel.read(buff);
        while (result.hasRemaining() && indexLen > 0) {
            buff.position(0);

            int len = Math.min(indexLen, result.remaining());
            for (int i = 0; i < len; i++) {
                result.put(buff.get());
            }
            buff.position(0);
            indexLen = channel.read(buff);
        }

        if (result.remaining() > 0) {
            String msg =
                    "Expect to read " + indexBytes + " bytes, actually read " + (result.limit() - result.remaining()) + " bytes";
            throw new RuntimeException(msg);
        }

        return result;
    }

    public static ByteBuffer readHeapBytes(FileChannel channel, long position, int indexBytes) throws IOException {
        long fileSize = channel.size();
        if (fileSize - position < indexBytes) {
            throw new IndexOutOfBoundsException("Reading size is out of the sourceFile bound " +
                    "which sourceFile size is " + fileSize + ", position is " + position + ", " +
                    "size is " + indexBytes);
        }

        channel.position(position);
        ByteBuffer result = ByteBuffer.allocate(indexBytes);
        result.position(0);

        ByteBuffer buff = ByteBuffer.allocateDirect(1024);
        int indexLen = channel.read(buff);
        while (result.hasRemaining() && indexLen > 0) {
            buff.position(0);

            int len = Math.min(indexLen, result.remaining());
            for (int i = 0; i < len; i++) {
                result.put(buff.get());
            }

            buff.position(0);
            indexLen = channel.read(buff);
        }

        if (result.remaining() > 0) {
            String msg = "readDirectBytes method, Expect to read " + indexBytes + " bytes, " +
                    "actually read " + (result.limit() - result.remaining()) + " bytes";
            throw new RuntimeException(msg);
        }

        return result;
    }

    public static boolean readFullBuffer(FileChannel channel, ByteBuffer buffer) throws IOException {
        int baseLen = channel.read(buffer);
        int flag = 0;
        while (baseLen > 0 && buffer.hasRemaining() && flag < READ_MAX) {
            baseLen = channel.read(buffer);
            flag++;
        }

        return !buffer.hasRemaining();
    }

    public static byte[] read(InputStream in, int count) throws IOException {
        byte[] b = new byte[count];
        int i = in.read(b);
        if (i != count) {
            throw new RuntimeException("Expect to read " + count + " bytes, actually read " + i + " bytes");
        }
        return b;
    }

    public static long readLong(InputStream in) throws IOException {
        return Longs.fromByteArray(read(in, 8));
    }

    public static int readInt(InputStream in) throws IOException {
        return Ints.fromByteArray(read(in, 4));
    }

    public static void inputStreamToOutputStream(InputStream in, OutputStream out) throws IOException {
        byte[] b = new byte[1024];
        int i;
        while ((i = in.read(b)) > 0) {
            out.write(b, 0, i);
        }
        out.flush();
    }

    public static String createTempFile(String name) throws IOException {
        String file = baseDir + String.format("/data-turbo/%d-%s", System.currentTimeMillis(),
                name);
        File file1 = new File(file);
        file1.getParentFile().mkdirs();
        file1.createNewFile();
        return file;
    }

}
