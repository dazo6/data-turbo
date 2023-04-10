package com.dazo66.data.turbo;

import com.dazo66.data.turbo.model.DataTurboDetail;
import com.dazo66.data.turbo.model.KeeperVersion;
import com.dazo66.data.turbo.util.DataTurboConstants;
import com.dazo66.data.turbo.util.DateUtils;
import com.dazo66.data.turbo.util.IOUtils;
import com.dazo66.data.turbo.util.Ints;
import com.dazo66.data.turbo.util.LineReader;
import com.dazo66.data.turbo.util.Longs;
import com.dazo66.data.turbo.util.MemoryUtils;
import com.dazo66.data.turbo.util.Pair;
import com.dazo66.data.turbo.util.SplitUtils;
import com.dazo66.data.turbo.util.SynchronizedTreeMap;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import static com.dazo66.data.turbo.util.IOUtils.createTempFile;

/**
 * ----注意----
 * 采用分块打包的技术 会手动进行fgc
 * 如果是在线服务非常不推荐在高峰期调用打包服务
 * <p>
 * 普通kv查询数据的打包类
 * 采用分块打包的技术，分块缓存后打包
 * todo 未进行引用能力的实现 但是遵循了协议
 * todo 未进行加密能力的实现 但是遵循了协议
 * <p>
 * 可用配置如下
 * [not must] {@linkplain DataTurboConstants#BUILDER_CONFIG_NORMAL_ERROR_TOLERATING} 容忍的错误数
 * 默认是long的最大值
 * [not must] {@linkplain DataTurboConstants#BUILDER_CONFIG_NORMAL_INDEX_COUNT} 索引数 默认是512
 * [not must] {@linkplain DataTurboConstants#BUILDER_CONFIG_NORMAL_MAX_COUNT_SPLIT} 多少key数量进行一次分块
 * 默认为-1 在内存不够时会自动分片
 * [not must] {@linkplain DataTurboConstants#BUILDER_CONFIG_NORMAL_WRITE_TIME} 是否写入打包时间
 * 一般会在文件头写入打包时间 测试时使用 用于校验不同模式下打包的文件是否一致
 *
 * @author dazo66
 **/
public class NormalDataTurboBuilder extends AbstractDataTurboBuilder {

    /**
     * 临时打包数据的缓存类 形式为 key + value体
     */
    private final SynchronizedTreeMap<String, String> tempMap =
            new SynchronizedTreeMap<>(new TreeMap<>((o1, o2) -> getKeyPredictor().compare(o1, o2)));
    /**
     * 初步统计的keyCount 不同临时数据块中的key可能有重复，实际的数量会写入文件块中
     */
    private final AtomicLong keyCount = new AtomicLong();
    /**
     * 临时文件列表
     */
    private final List<String> tempFiles = new CopyOnWriteArrayList<>();
    /**
     * 字段列表 只会读取map中这里存在的字段 如果不存在会改成空字符串
     */
    private final List<String> fields;
    /**
     * 临时的行读取器 通过指定的分割符进行读取 形式为 临时文件名 + 行读取器
     */
    private final Map<String, LineReader> tempReaders = new ConcurrentHashMap<>();
    /**
     * 临时的文件最小值存储map 在读取临时文件时使用 每次读取会从这里选择最小的往下读取
     */
    private final Map<String, String> tempRecentOfFile = new SynchronizedTreeMap<>(new TreeMap<>());
    /**
     * 临时存储索引的map 索引数量通过
     * {@see DataTurboConstants.BUILDER_CONFIG_NORMAL_INDEX_COUNT}
     * 指定
     */
    private final Map<String, Long> indexMap = new TreeMap<>();
    /**
     * 错误数 如果包含非法字符 / 重复key 则是错误数据
     */
    private final AtomicLong errorCount = new AtomicLong();
    /**
     * 错误容忍数 当错误达到这个数会终止打包
     */
    private final AtomicLong errorTolerating;
    /**
     * 多少数量分块一次 如果为负数 则自动根据内存情况来
     */
    private final int maxCountPreSplit;
    /**
     * 是否写入打包时间，一般不用处理，测试用于比较文件相等的一种手段
     */
    private final boolean writeTime;
    /**
     * 索引数量通过
     * {@see DataTurboConstants.BUILDER_CONFIG_NORMAL_INDEX_COUNT}
     * 指定
     */
    private final int indexCount;
    /**
     * 错误文件名
     */
    private String errorFile;
    /**
     * 错误输出流
     */
    private OutputStream errorMsgOutputStream;
    /**
     * value临时文件输出流 临时存储value文件快
     */
    private BufferedOutputStream valueDataStream;
    /**
     * value临时文件名
     */
    private String valueTempFile;

    public NormalDataTurboBuilder(DataTurboDetail dataTurboDetail) {
        super(dataTurboDetail);
        this.fields = Arrays.asList(dataTurboDetail.getFields());
        if (fields.stream().anyMatch(s -> s.contains(DataTurboConstants.BUILDER_CONSTANTS_NORMAL_SPLIT_CHAR_1))) {
            throw new IllegalArgumentException("field name has illegal char: \\u0011");
        }
        errorTolerating =
                new AtomicLong(Long.parseLong(dataTurboDetail.getProp(DataTurboConstants.BUILDER_CONFIG_NORMAL_ERROR_TOLERATING, "9223372036854775807")));
        indexCount =
                Integer.parseInt(dataTurboDetail.getProp(DataTurboConstants.BUILDER_CONFIG_NORMAL_INDEX_COUNT, "512"));
        maxCountPreSplit =
                Integer.parseInt(dataTurboDetail.getProp(DataTurboConstants.BUILDER_CONFIG_NORMAL_MAX_COUNT_SPLIT, "-1"));
        writeTime =
                Boolean.parseBoolean(dataTurboDetail.getProp(DataTurboConstants.BUILDER_CONFIG_NORMAL_WRITE_TIME, "true"));
        try {
            errorFile = createTempFile(String.format("%s-error.txt", dataTurboDetail.getDataId()));
            errorMsgOutputStream = new BufferedOutputStream(new FileOutputStream(errorFile));
        } catch (Exception e) {
            if (errorMsgOutputStream != null) {
                try {
                    errorMsgOutputStream.close();
                } catch (Exception e1) {
                    // ignore
                }
            }
            errorMsgOutputStream = System.err;
            System.out.println("creat error with error switch to system error out");
        }
    }

    @Override
    public int getMaxCountPreSplit() {
        return maxCountPreSplit;
    }

    /**
     * 打包数据输入口
     * 打包服务会存在手动fGC的情况。
     * 非常不推荐在在线服务中嵌入打包服务。
     *
     * @param key key
     * @param map 一个map作为一条记录的值
     * @return 如果数据重复则返回false
     * 因为采用的是分块打包技术，无法一次看出传入的key在全局是否重复
     * 可以通过看打包过程中产生的error文件来检查
     */
    @Override
    public boolean inputData(String key, Map<String, Object> map) {
        if (isSplit()) {
            try {
                String tempFile = storeTemp();
                tempMap.clear();
                tempFiles.add(tempFile);
                System.gc();
            } catch (IOException e) {
                throw new RuntimeException("store temp file with error", e);
            }
        }
        if (tempMap.containsKey(key)) {
            addErrorCount("duplicate key: " + key);
            return false;
        } else {
            tempMap.put(key, getValueString(key, map));
            keyCount.getAndIncrement();
            return true;
        }
    }

    @Override
    public DataTurboDetail build() throws Exception {
        valueTempFile = createTempFile(String.format("%s-value.temp",
                getDataTurboDetail().getDataId()));
        valueDataStream = new BufferedOutputStream(new FileOutputStream(valueTempFile));
        initTempFileReader();
        long valueBlockLength = buildValueBlock();
        byte[] indexBlock = buildIndexBlock();
        byte[] fieldBlock = buildFieldBlock();
        String realFile = createTempFile(String.format("%s.normal",
                getDataTurboDetail().getDataId()));
        BufferedOutputStream realStream = new BufferedOutputStream(new FileOutputStream(realFile));
        // 写入打包日期
        if (writeTime) {
            realStream.write(Longs.toByteArray(System.currentTimeMillis()));
        } else {
            realStream.write(Longs.toByteArray(0L));
        }
        // 写入关键词数量
        realStream.write(Longs.toByteArray(keyCount.get()));
        // 写入索引数据块大小
        realStream.write(Longs.toByteArray(indexBlock.length));
        // 写入values数据块的大小
        realStream.write(Longs.toByteArray(valueBlockLength));
        // 写入引用数据块大小
        realStream.write(Longs.toByteArray(0L));
        // 写入字段数据块长度
        realStream.write(Ints.toByteArray(fieldBlock.length));
        // 写入字段数据
        realStream.write(fieldBlock);
        // 写入索引数据块
        realStream.write(indexBlock);
        BufferedInputStream valueBlockInputStream =
                new BufferedInputStream(new FileInputStream(valueTempFile));
        // 写入真实数据块长度
        IOUtils.inputStreamToOutputStream(valueBlockInputStream, realStream);
        valueBlockInputStream.close();
        valueDataStream.close();
        realStream.close();
        cleanTempFile();
        getDataTurboDetail().setDataFile(realFile).setDataVersion(DateUtils.getDataVersion()).setKeeperVersion(KeeperVersion.VERSION);
        return getDataTurboDetail();
    }

    protected void addErrorCount(String errorMsg) {
        try {
            errorMsgOutputStream.write((errorMsg + "\n").getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            // ignore
        }
        if (errorTolerating.get() > errorCount.get()) {
            errorCount.getAndIncrement();
        } else {
            try {
                errorMsgOutputStream.flush();
            } catch (IOException e) {
                // ignore
            }
            throw new RuntimeException("error count is over than tolerated count");
        }
    }

    protected String getValueString(String key, Map<String, Object> values) {
        List<String> list = new ArrayList<>();
        // 一条记录只添加一次错误数
        boolean flag = true;
        for (String field : fields) {
            Object o = values.getOrDefault(field, "");
            String value = Objects.toString(o);
            if (value == null) {
                value = "";
            }
            if (value.contains(DataTurboConstants.BUILDER_CONSTANTS_NORMAL_SPLIT_CHAR_2) || value.contains(DataTurboConstants.BUILDER_CONSTANTS_NORMAL_SPLIT_CHAR_3)) {
                if (flag) {
                    addErrorCount(String.format("illegal char, key: %s, field: %s, value: %s", key, field, value));
                    flag = false;
                }
                // 修复错误数据
                value = value.replace(DataTurboConstants.BUILDER_CONSTANTS_NORMAL_SPLIT_CHAR_2, "")
                        .replace(DataTurboConstants.BUILDER_CONSTANTS_NORMAL_SPLIT_CHAR_3, "");
            }
            list.add(value);
        }
        return SplitUtils.join(list, DataTurboConstants.BUILDER_CONSTANTS_NORMAL_SPLIT_CHAR_2);
    }

    /**
     * 先把临时文件储存起来
     *
     * @return 返回临时文件名
     */
    protected String storeTemp() throws IOException {
        synchronized (tempMap) {
            String pathname = createTempFile(String.format("%s-%d.temp",
                    getDataTurboDetail().getDataId(), tempFiles.size()));
            BufferedOutputStream outputStream =
                    new BufferedOutputStream(new FileOutputStream(pathname));
            for (Map.Entry<String, String> entry : tempMap.entrySet()) {
                String s = entry.getKey() + DataTurboConstants.BUILDER_CONSTANTS_NORMAL_SPLIT_CHAR_1 + entry.getValue() + DataTurboConstants.BUILDER_CONSTANTS_NORMAL_SPLIT_CHAR_2;
                outputStream.write(s.getBytes(StandardCharsets.UTF_8));
            }
            outputStream.flush();
            outputStream.close();
            return pathname;
        }
    }

    protected boolean isSplit() {
        if (getMaxCountPreSplit() <= 0) {
            // 检查是否有1G空余内存
            return !MemoryUtils.checkFreeMemory(1024 * 1024 * 1024L);
        }
        // 如果是固定临界值
        return getMaxCountPreSplit() < tempMap.size();
    }

    protected void cleanTempFile() {
        closeTempFile();
        for (String file : tempFiles) {
            try {
                new File(file).delete();
            } catch (Exception e) {
                // ignore
            }
        }
        try {
            new File(valueTempFile).delete();
        } catch (Exception e) {
            // ignore
        }
        // 如果没有错误数据就删除错误文件
        if (errorCount.get() == 0) {
            try {
                new File(errorFile).delete();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    private void closeTempFile() {
        for (LineReader lineReader : tempReaders.values()) {
            try {
                lineReader.close();
            } catch (IOException e) {
                // ignore
            }
        }
        try {
            errorMsgOutputStream.flush();
            errorMsgOutputStream.close();
        } catch (Exception e) {
            // ignore
        }
    }

    protected byte[] buildFieldBlock() {
        return SplitUtils.join(fields, DataTurboConstants.BUILDER_CONSTANTS_NORMAL_SPLIT_CHAR_1).getBytes(StandardCharsets.UTF_8);
    }

    protected byte[] buildIndexBlock() {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byte[] split1 = DataTurboConstants.BUILDER_CONSTANTS_NORMAL_SPLIT_CHAR_1.getBytes(StandardCharsets.UTF_8);
        byte[] split2 = DataTurboConstants.BUILDER_CONSTANTS_NORMAL_SPLIT_CHAR_2.getBytes(StandardCharsets.UTF_8);
        int i = 0;
        for (String key : indexMap.keySet()) {
            try {
                byteArrayOutputStream.write(key.getBytes(StandardCharsets.UTF_8));
                byteArrayOutputStream.write(split1);
                byteArrayOutputStream.write(indexMap.get(key).toString().getBytes(StandardCharsets.UTF_8));
                if (i != indexMap.size() - 1) {
                    byteArrayOutputStream.write(split2);
                }
                i++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return byteArrayOutputStream.toByteArray();
    }

    protected long buildValueBlock() throws IOException {
        Pair<String, String> nextFromCache;
        long count = 0L;
        long byteCount = 0L;
        // 初始化最小hash
        for (String s : tempFiles) {
            tempRecentOfFile.put(s, "");
        }
        while ((nextFromCache = getNextFromCache()) != null) {
            byte[] bytes = (nextFromCache.getLeft() + DataTurboConstants.BUILDER_CONSTANTS_NORMAL_SPLIT_CHAR_2 + nextFromCache.getRight() + DataTurboConstants.BUILDER_CONSTANTS_NORMAL_SPLIT_CHAR_3).getBytes(StandardCharsets.UTF_8);
            valueDataStream.write(bytes);
            long l = keyCount.get() / indexCount;
            if ((tempRecentOfFile.isEmpty() && tempMap.isEmpty()) || count % l == 0) {
                indexMap.put(nextFromCache.getLeft(), byteCount);
            }
            count++;
            byteCount += bytes.length;
        }
        // 重新设置key数量 防止多个分片文件中出现重复key
        keyCount.set(count);
        valueDataStream.flush();
        return byteCount;
    }

    protected void initTempFileReader() {
        tempFiles.forEach(s -> {
            try {
                tempReaders.put(s, new LineReader(new FileReader(s), DataTurboConstants.BUILDER_CONSTANTS_NORMAL_SPLIT_CHAR_2.charAt(0)));
            } catch (FileNotFoundException e) {
                closeTempFile();
                throw new RuntimeException("can not find file: " + s);
            }
        });
    }

    protected Pair<String, String> getNextFromCache() throws IOException {
        // 如果缓存map为空 就一次获取多点缓存
        if (tempMap.size() == 0) {
            for (String file : tempReaders.keySet()) {
                if (tempRecentOfFile.get(file) != null) {
                    loadFromCache(tempReaders.get(file));
                }
            }
        }
        // 如果还是为空map 说明已经全部获取完毕了
        if (tempMap.size() == 0) {
            return null;
        }
        // 先获得最小的key
        String currentMin = tempRecentOfFile.isEmpty() ? null : tempRecentOfFile.values().stream().min((o1, o2) -> getKeyPredictor().compare(o1, o2)).get();
        String first = tempMap.firstKey();
        // 如果已经比文件中的下一个要小了 就先返回
        if (first != null && (currentMin == null || getKeyPredictor().compare(currentMin, first) > 0)) {
            return Pair.of(first, tempMap.remove(first));
        }
        // 如果当前缓存的没有最小的 就从lineReader中取
        for (String file : tempReaders.keySet()) {
            if (getKeyPredictor().compare(tempRecentOfFile.get(file), first) <= 0) {
                String value = loadFromCache(tempReaders.get(file));
                if (value == null) {
                    tempRecentOfFile.remove(file);
                } else {
                    tempRecentOfFile.put(file, value);
                }
            }
        }
        String retKey = tempMap.firstKey();
        return Pair.of(retKey, tempMap.remove(retKey));
    }

    protected String loadFromCache(LineReader reader) throws IOException {
        String ret = null;
        while (true) {
            String s = reader.readLine();
            // 文件到头
            if (s == null) {
                return ret;
            }
            // 可能会写入空行
            if (s.isEmpty()) {
                continue;
            }
            // 根据分隔符分割
            String[] split = s.split(DataTurboConstants.BUILDER_CONSTANTS_NORMAL_SPLIT_CHAR_1, 2);
            if (split.length == 2) {
                String key = split[0];
                if (ret == null) {
                    ret = key;
                }
                if (tempMap.containsKey(key)) {
                    addErrorCount("duplicate key: " + key);
                    continue;
                }
                tempMap.put(key, split[1]);
                if (getKeyPredictor().compare(key, ret) != 0) {
                    return key;
                }
            } else {
                addErrorCount("temp file has error: " + s);
                closeTempFile();
                throw new RuntimeException("cache file has error");
            }
        }
    }

}
