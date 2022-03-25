package com.dazo66.data.turbo.util;

import com.dazo66.data.turbo.model.LoadEnum;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author dazo66
 **/
public class ByteHolder {

    private final LoadEnum loadEnum;
    private final int stepCount;
    private final ThreadLocal<FileChannel> channelThreadLocal = new ThreadLocal<>();
    private long baseOffset;
    private long length;
    private ByteBuffer byteBuffer;
    private FileChannelPool fileChannelPool;

    public ByteHolder(LoadEnum loadEnum, FileChannelPool fileChannelPool, long baseOffset,
                      long length, int stepCount) {
        this.loadEnum = loadEnum;
        this.stepCount = stepCount;
        if (loadEnum == LoadEnum.HEAP) {
            if (length < Integer.MAX_VALUE) {
                try {
                    FileChannel fileChannel = fileChannelPool.getFileChannel();
                    byteBuffer = IOUtils.readHeapBytes(fileChannel, baseOffset,
                            Ints.checkedCast(length));
                    this.baseOffset = 0;
                    this.length = length;
                    fileChannelPool.putFileChannel(fileChannel);
                    fileChannelPool.shutDown();
                } catch (Exception e) {
                    throw new RuntimeException("load from disk has error:", e);
                }
            }
        } else if (loadEnum == LoadEnum.DISK) {
            this.fileChannelPool = fileChannelPool;
            this.baseOffset = baseOffset;
            this.length = length;
        } else {
            throw new RuntimeException();
        }
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    public byte[] read(long offset, int length) throws IOException {
        if (loadEnum == LoadEnum.HEAP) {
            byte[] bytes = new byte[length];
            byteBuffer.position(Ints.checkedCast(offset));
            byteBuffer.get(bytes, 0, length);
            return bytes;
        } else if (loadEnum == LoadEnum.DISK) {
            if (channelThreadLocal.get() == null) {
                channelThreadLocal.set(fileChannelPool.getFileChannel());
            }
            FileChannel fileChannel = channelThreadLocal.get();
            ByteBuffer byteBuffer = ByteBuffer.allocate(length);
            fileChannel.position(baseOffset + offset);
            fileChannel.read(byteBuffer);
            return byteBuffer.array();
        } else {
            throw new RuntimeException();
        }
    }

    public long indexOf(long offset, String endString) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        while (true) {
            if (offset >= baseOffset + length) {
                return -1;
            }
            long min = baseOffset + length - offset;
            int len = min > stepCount ? stepCount : ((Long) min).intValue();
            byte[] bytes = read(offset, len);
            buffer.write(bytes);
            String s = buffer.toString();
            int i = s.indexOf(endString);
            if (i != -1) {
                return i;
            }
            offset += len;
        }
    }

    public String read(long offset, String endString) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        while (true) {
            if (offset >= baseOffset + length) {
                return null;
            }
            long min = baseOffset + length - offset;
            int len = min > stepCount ? stepCount : ((Long) min).intValue();
            byte[] bytes = read(offset, len);
            buffer.write(bytes);
            String s = buffer.toString();
            int i = s.indexOf(endString);
            if (i != -1) {
                return s.substring(0, i);
            }
            offset += len;
        }
    }

    public void clean() {
        try {
            FileChannel channel = channelThreadLocal.get();
            if (channel != null) {
                channelThreadLocal.remove();
                fileChannelPool.putFileChannel(channel);
            }
        } catch (Exception e) {
            // ignore
        }
    }

}
