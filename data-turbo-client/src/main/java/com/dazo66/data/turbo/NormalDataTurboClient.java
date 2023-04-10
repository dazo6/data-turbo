package com.dazo66.data.turbo;

import com.dazo66.data.turbo.key.predictor.IKeyComparator;
import com.dazo66.data.turbo.key.predictor.StringKeyComparator;
import com.dazo66.data.turbo.model.DataTurboDetail;
import com.dazo66.data.turbo.model.DataTurboResult;
import com.dazo66.data.turbo.util.ByteHolder;
import com.dazo66.data.turbo.util.ByteUtils;
import com.dazo66.data.turbo.util.DataTurboConstants;
import com.dazo66.data.turbo.util.FileChannelPool;
import com.dazo66.data.turbo.util.IOUtils;
import com.dazo66.data.turbo.util.Ints;
import com.dazo66.data.turbo.util.Longs;
import com.dazo66.data.turbo.util.Pair;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * kv查询客户端
 *
 * @author dazo66
 **/
public class NormalDataTurboClient extends AbstractDataTurboClient {

    /**
     * 一些常量
     */
    private final static byte[] SPLIT_CHAR3_BYTE =
            DataTurboConstants.BUILDER_CONSTANTS_NORMAL_SPLIT_CHAR_3.getBytes(StandardCharsets.UTF_8);
    private final static byte[] splitChar2Byte =
            DataTurboConstants.BUILDER_CONSTANTS_NORMAL_SPLIT_CHAR_2.getBytes(StandardCharsets.UTF_8);
    private final static int splitChar2Len = splitChar2Byte.length;
    /**
     * 索引map
     */
    private final TreeMap<String, Long> indexMap = new TreeMap<>();
    /**
     * 每一步读取的字节数
     */
    private final int preStepReadCount = 512;
    /**
     * 默认的比较器
     */
    private final StringKeyComparator stringKeyComparator = new StringKeyComparator();
    /**
     * 构建时间
     */
    private Long buildTime;
    /**
     * 数据总数
     */
    private Long count;
    /**
     * values字节holder
     */
    private ByteHolder valuesHolder;
    /**
     * 字段名称
     */
    private String[] fields;
    /**
     * 引用数据块字节holder
     */
    private ByteHolder referenceHolder;
    /**
     * fileChannel 池
     */
    private FileChannelPool fileChannelPool;
    /**
     * 索引数据块长度
     */
    private long indexBlockLength;
    /**
     * values数据块长度
     */
    private long valueBlockLength;
    /**
     * 引用数据块长度
     */
    private long referenceBlockLength;

    public NormalDataTurboClient(DataTurboDetail dataTurboDetail) {
        super(dataTurboDetail);
    }

    @Override
    public DataTurboResult search(String key) {
        if (buildTime == null) {
            throw new RuntimeException("client are not load");
        }
        try {
            Map.Entry<String, Long> leftEntry = indexMap.floorEntry(key);
            Map.Entry<String, Long> rightEntry = indexMap.ceilingEntry(key);
            if (leftEntry == null || rightEntry == null) {
                return null;
            }
            return binarySearch(leftEntry, rightEntry, key);
        } catch (Exception e) {
            throw new RuntimeException("[data-turbo] search with exception: ", e);
        } finally {
            referenceHolder.clean();
            valuesHolder.clean();
        }
    }

    /**
     * 名单查询 就是复用了kv查询
     *
     * @param key 目标key
     * @return true or false
     */
    @Override
    public boolean searchNameList(String key) {
        return search(key) != null;
    }

    /**
     * 从构造函数传入的detail文件进行加载
     *
     * @throws Exception 加载时的错误抛出
     */
    @Override
    public void load() throws Exception {
        DataTurboDetail dataTurboDetail = getDataTurboDetail();
        BufferedInputStream in =
                new BufferedInputStream(new FileInputStream(dataTurboDetail.getDataFile()));
        buildTime = IOUtils.readLong(in);
        count = IOUtils.readLong(in);
        indexBlockLength = IOUtils.readLong(in);
        valueBlockLength = IOUtils.readLong(in);
        referenceBlockLength = IOUtils.readLong(in);
        int fieldBlockLength = IOUtils.readInt(in);
        fields =
                new String(IOUtils.read(in, fieldBlockLength)).split(DataTurboConstants.BUILDER_CONSTANTS_NORMAL_SPLIT_CHAR_1);
        byte[] read = IOUtils.read(in, Ints.checkedCast(indexBlockLength));
        loadIndex(read);
        fileChannelPool = new FileChannelPool(new File(dataTurboDetail.getDataFile()));
        valuesHolder = new ByteHolder(getDataTurboDetail().getLoadEnum(), fileChannelPool,
                40 + 4 + fieldBlockLength + indexBlockLength, valueBlockLength, preStepReadCount);
        referenceHolder = new ByteHolder(getDataTurboDetail().getLoadEnum(), fileChannelPool,
                40 + 4 + fieldBlockLength + indexBlockLength + valueBlockLength,
                referenceBlockLength, preStepReadCount);
        in.close();
    }

    /**
     * 关闭整个客户端
     *
     * @throws Exception 可能会出现异常
     */
    @Override
    public void close() throws Exception {
        fileChannelPool.shutDown();
    }

    /**
     * 获得key比较器
     *
     * @return key
     */
    @Override
    public IKeyComparator getComparator() {
        return stringKeyComparator;
    }

    protected DataTurboResult binarySearch(Map.Entry<String, Long> left,
                                           Map.Entry<String, Long> right, String targetKey) throws IOException {
        if (getComparator().compare(left.getKey(), right.getKey()) <= 0) {
            // 相等直接返回
            if (getComparator().compare(left.getKey(), targetKey) == 0) {
                return getNextRecord(left.getValue());
            }
            if (getComparator().compare(right.getKey(), targetKey) == 0) {
                return getNextRecord(left.getValue());
            }
            // 如果是最后一点数据 就不进行二分了 直接全读了
            if (right.getValue() - left.getValue() <= preStepReadCount * 2L) {
                return findInScope(left.getValue(), right.getValue(), targetKey);
            }
            long min = left.getValue() / 2 + right.getValue() / 2;
            // 如果中间没有值了
            Map.Entry<String, Long> minKey = getNextPosition(min);
            if (minKey == null) {
                return null;
            }
            if (minKey.getValue().equals(right.getValue())) {
                return findInScope(left.getValue(), right.getValue(), targetKey);
            }
            // 如果可以比较就继续二分查找
            int i = getComparator().compare(minKey.getKey(), targetKey);
            if (i < 0) {
                return binarySearch(minKey, right, targetKey);
            }
            if (i > 0) {
                return binarySearch(left, minKey, targetKey);
            }
            // 相等就直接返回
            return getNextRecord(minKey.getValue());
        } else {
            throw new RuntimeException("left key must lower than right key");
        }
    }

    /**
     * 在某一个范围内进行查找
     *
     * @param start     开始字节数
     * @param end       结束字节数
     * @param targetKey 目标key
     * @return 如果存在返回解析好的result 如果不存在返回null
     * @throws IOException 可能会存在io异常
     */
    protected DataTurboResult findInScope(Long start, Long end, String targetKey) throws IOException {
        Map.Entry<String, Long> nextPosition = getNextPosition(start);
        while (nextPosition != null && nextPosition.getValue() <= end) {
            int i = getComparator().compare(targetKey, nextPosition.getKey());
            if (i == 0) {
                return getNextRecord(nextPosition.getValue());
            } else if (i < 0) {
                return null;
            }
            nextPosition =
                    getNextPosition(nextPosition.getValue() + ByteUtils.getByteCountUTF8(nextPosition.getKey()) + 1);
        }
        return null;

        /*byte[] bytes = valuesHolder.read(start, Ints.checkedCast(end - start));
        String cache = new String(bytes, StandardCharsets.UTF_8);
        String[] split = cache.split(DataTurboConstants.BUILDER_CONSTANTS_NORMAL_SPLIT_CHAR_3);
        for (String s : split) {
            String[] kv = s.split(DataTurboConstants.BUILDER_CONSTANTS_NORMAL_SPLIT_CHAR_2, 2);
            if (kv.length == 2 && getComparator().compare(kv[0], targetKey) == 0) {
                return new DataTurboResult(kv[0], buildValue(kv[1]));
            } else if (kv.length == 2 && getComparator().compare(kv[0], targetKey) > 0) {
                break;
            }
        }
        return null;*/
    }

    /**
     * 根据给定的偏移量寻找下一个记录
     *
     * @param start 偏移量
     * @return 下一个结果
     * @throws IOException 可能会存在io异常
     */
    protected DataTurboResult getNextRecord(Long start) throws IOException {
        String key = valuesHolder.read(start,
                DataTurboConstants.BUILDER_CONSTANTS_NORMAL_SPLIT_CHAR_2);
        if (key == null) {
            return null;
        }
        int keyByteLength = ByteUtils.getByteCountUTF8(key);
        String value = valuesHolder.read(start + keyByteLength + splitChar2Len,
                DataTurboConstants.BUILDER_CONSTANTS_NORMAL_SPLIT_CHAR_3);
        return new DataTurboResult(key, buildValue(value));
    }

    /**
     * 根据存储的value字符串构建values
     *
     * @param value value字符串
     * @return string-string 格式的kv对象
     * @throws IOException 如果遇见引用型value 可能会出现io异常
     */
    protected Map<String, String> buildValue(String value) throws IOException {
        if (value.startsWith(DataTurboConstants.BUILDER_CONSTANTS_NORMAL_SPLIT_CHAR_4)) {
            byte[] bytes = value.substring(1).getBytes(StandardCharsets.UTF_8);
            long pos = Longs.fromBytes(bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5]
                    , bytes[6], bytes[7]);
            int count = Ints.fromBytes(bytes[8], bytes[9], bytes[10], bytes[11]);
            byte[] read = referenceHolder.read(pos, count);
            value = new String(read, StandardCharsets.UTF_8);
        }
        String[] split = value.split(DataTurboConstants.BUILDER_CONSTANTS_NORMAL_SPLIT_CHAR_2);
        if (split.length != fields.length) {
            throw new RuntimeException(String.format("value string has wrong expect field count: " +
                    "%d, get: %d", fields.length, split.length));
        }
        Map<String, String> map = new HashMap<>(fields.length, 1.0f);
        for (int i = 0; i < split.length; i++) {
            map.put(fields[i], split[i]);
        }
        return map;
    }

    /**
     * 获得下一个位置 一般是从截断的位置开始 往下寻找
     *
     * @param start 开始的偏移量
     * @return key-offset 格式的数据对
     * @throws IOException 可能会存在io异常
     */
    protected Map.Entry<String, Long> getNextPosition(long start) throws IOException {
        ByteArrayOutputStream keyBuffer = new ByteArrayOutputStream();
        while (true) {
            long end = valuesHolder.getBaseOffset() + valueBlockLength;
            int length = end - start > preStepReadCount ? preStepReadCount :
                    ((Long) (end - start)).intValue();
            if (length <= 0) {
                return null;
            }
            byte[] bytes = valuesHolder.read(start, length);
            int i = ByteUtils.indexOfBytes(bytes, SPLIT_CHAR3_BYTE, 0);
            if (i == -1) {
                start += length;
            } else {
                int j = ByteUtils.indexOfBytes(bytes, splitChar2Byte, i);
                if (j != -1) {
                    return Pair.of(new String(bytes, i + 1, j - i - 1, StandardCharsets.UTF_8),
                            start + i + 1);
                } else {
                    long keyStart = start + i + 1;
                    start += length;
                    keyBuffer.write(bytes, i + 1, bytes.length - i - 1);
                    while (true) {
                        end = start + valueBlockLength;
                        length = end - start > preStepReadCount ? preStepReadCount :
                                ((Long) (end - start)).intValue();
                        bytes = valuesHolder.read(start, length);
                        j = ByteUtils.indexOfBytes(bytes, splitChar2Byte, 0);
                        if (j != -1) {
                            keyBuffer.write(bytes, 0, j);
                            return Pair.of(keyBuffer.toString("utf-8"), keyStart);
                        } else {
                            start += length;
                            keyBuffer.write(bytes);
                        }
                    }
                }
            }
        }
    }

    /**
     * 加载索引
     *
     * @param indexBytes 索引数据块
     */
    protected void loadIndex(byte[] indexBytes) {
        String[] indexString =
                new String(indexBytes).split(DataTurboConstants.BUILDER_CONSTANTS_NORMAL_SPLIT_CHAR_2);
        for (String s : indexString) {
            String[] split = s.split(DataTurboConstants.BUILDER_CONSTANTS_NORMAL_SPLIT_CHAR_1);
            if (split.length == 2) {
                indexMap.put(split[0], Long.parseLong(split[1]));
            } else {
                throw new RuntimeException("index file has error");
            }
        }
    }
}
