package com.dazo66;

import com.dazo66.data.turbo.BloomDataTurboBuilder;
import com.dazo66.data.turbo.BloomDataTurboClient;
import com.dazo66.data.turbo.model.DataTurboDetail;
import com.dazo66.data.turbo.model.LoadEnum;
import com.dazo66.data.turbo.util.DataTurboConstants;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Random;

/**
 * @author dazo66
 **/
public class BloomTest {

    private static final String str =
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()<>?:";
    private static final String str1 = "收到时代和我抢会发生看到击破强迫你脾气饿哦是丢啊回收丢啊丢啊和深度和";
    private static final long seed = 12379127398123L;
    public static Random random;

    public static String getRandomString() {
        StringBuilder sb = new StringBuilder();
        int length = 40 + random.nextInt(100);
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(str.length());
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }

    public static String getRandomString1() {
        StringBuilder sb = new StringBuilder();
        int length = 40 + random.nextInt(100);
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(str1.length());
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }

    private static DataTurboDetail build(DataTurboDetail dataTurboDetail, int count,
                                         long randomSeed) throws Exception {
        BloomDataTurboBuilder bloomDataTurboBuilder = new BloomDataTurboBuilder(dataTurboDetail);
        random = new Random(randomSeed);
        for (int i = 0; i < count; i++) {
            String randomString = getRandomString();
            bloomDataTurboBuilder.inputData(randomString, null);
        }
        return bloomDataTurboBuilder.build();
    }

    private static long check(DataTurboDetail dataTurboDetail, int count, long randomSeed) throws Exception {
        BloomDataTurboClient bloomDataTurboClient = new BloomDataTurboClient(dataTurboDetail);
        bloomDataTurboClient.load();
        random = new Random(randomSeed);
        Long totalTime = 0L;
        for (int i = 0; i < count; i++) {
            String randomString = getRandomString();
            long l1 = System.nanoTime();
            boolean condition = bloomDataTurboClient.searchNameList(randomString);
            totalTime += System.nanoTime() - l1;
            if (!condition) {
                throw new RuntimeException("bloom 过滤器检查失败" + i + randomString);
            }
        }
        return totalTime;
    }

    @AfterClass
    public static void checkFileDif() throws Exception {
        File file = new File("test/test.bloom");
        File file1 = new File("test/test.bloom_heap");
        File file2 = new File("test/test.bloom_disk");
        file.delete();
        file1.delete();
        file2.delete();
    }

    @Test
    public void testHeapModeBuildDiskModeLoad() throws Exception {
        random = new Random(seed);
        int testCount = 10000;
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        DataTurboDetail dataTurboDetail = new DataTurboDetail();
        dataTurboDetail.setLoadEnum(LoadEnum.HEAP);
        dataTurboDetail.setDataId("testBloom");
        dataTurboDetail.setProp(DataTurboConstants.BUILDER_CONFIG_BLOOM_EXCEPT_COUNT,
                ((Integer) testCount).toString());
        // dataTurboDetail.setProp(DataTurboConstants.BUILDER_CONFIG_BLOOM_FPP, "0.001");
        dataTurboDetail.setDataFile("test/test.bloom");
        DataTurboDetail dataTurboDetail1 = build(dataTurboDetail, testCount, seed);
        stopWatch.split();
        dataTurboDetail1.setLoadEnum(LoadEnum.DISK);
        long totalTime = check(dataTurboDetail1, testCount, seed);
        stopWatch.stop();
        System.out.println(stopWatch.toSplitString());
        System.out.println("布隆过滤器测试完毕,全部都能检测到");
        System.out.println("读取平均花费的时间为：" + totalTime / testCount + "ns");
    }

    @Test
    public void testDiskModeBuildHeapModeLoad() throws Exception {
        random = new Random(seed);
        int testCount = 10000;
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        DataTurboDetail dataTurboDetail = new DataTurboDetail();
        dataTurboDetail.setLoadEnum(LoadEnum.DISK);
        dataTurboDetail.setDataId("testBloom");
        dataTurboDetail.setProp(DataTurboConstants.BUILDER_CONFIG_BLOOM_EXCEPT_COUNT,
                ((Integer) testCount).toString());
        // dataTurboDetail.setProp(DataTurboConstants.BUILDER_CONFIG_BLOOM_FPP, "0.001");
        dataTurboDetail.setDataFile("test/test.bloom");
        DataTurboDetail dataTurboDetail1 = build(dataTurboDetail, testCount, seed);
        stopWatch.split();
        dataTurboDetail1.setLoadEnum(LoadEnum.HEAP);
        long totalTime = check(dataTurboDetail1, testCount, seed);
        stopWatch.stop();
        System.out.println(stopWatch.toSplitString());
        System.out.println("布隆过滤器测试完毕,全部都能检测到");
        System.out.println("读取平均花费的时间为：" + totalTime / testCount + "ns");
    }

    @Test
    public void testHeapMode() throws Exception {
        random = new Random(seed);
        int testCount = 400000;
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        DataTurboDetail dataTurboDetail = new DataTurboDetail();
        dataTurboDetail.setLoadEnum(LoadEnum.HEAP);
        dataTurboDetail.setDataId("testBloom");
        dataTurboDetail.setProp(DataTurboConstants.BUILDER_CONFIG_BLOOM_EXCEPT_COUNT,
                ((Integer) testCount).toString());
        dataTurboDetail.setProp(DataTurboConstants.BUILDER_CONFIG_BLOOM_FPP, "0.00000000000000001");
        dataTurboDetail.setDataFile("test/test.bloom_heap");
        DataTurboDetail dataTurboDetail1 = build(dataTurboDetail, testCount, seed);
        stopWatch.split();
        long totalTime = check(dataTurboDetail1, testCount, seed);
        long trueCount = checkFalse(dataTurboDetail1, testCount * 10);

        stopWatch.stop();
        System.out.println(stopWatch.toSplitString());
        System.out.println("内存布隆过滤器测试完毕,全部都能检测到");
        System.out.printf("反向检查一共检查出%d个误判%n", trueCount);
        System.out.println("读取平均花费的时间为：" + totalTime / testCount + "ns");
    }

    private long checkFalse(DataTurboDetail dataTurboDetail1, int j) throws Exception {
        BloomDataTurboClient bloomDataTurboClient = new BloomDataTurboClient(dataTurboDetail1);
        bloomDataTurboClient.load();
        random.setSeed(System.currentTimeMillis());
        long ret = 0;
        for (int i = 0; i < j; i++) {
            String randomString = getRandomString1();
            boolean condition = bloomDataTurboClient.searchNameList(randomString);
            if (condition) {
                ret++;
                System.out.println("反向检查出一个误判");
            }
        }
        return ret;
    }

    @Test
    public void testDiskMode() throws Exception {
        random = new Random(seed);
        int testCount = 1000;
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        DataTurboDetail dataTurboDetail = new DataTurboDetail();
        dataTurboDetail.setLoadEnum(LoadEnum.DISK);
        dataTurboDetail.setDataId("testBloom");
        dataTurboDetail.setProp(DataTurboConstants.BUILDER_CONFIG_BLOOM_EXCEPT_COUNT,
                ((Integer) testCount).toString());
        // dataTurboDetail.setProp(DataTurboConstants.BUILDER_CONFIG_BLOOM_FPP, "0.001");
        dataTurboDetail.setDataFile("test/test.bloom_disk");
        DataTurboDetail dataTurboDetail1 = build(dataTurboDetail, testCount, seed);
        stopWatch.split();
        long totalTime = check(dataTurboDetail1, testCount, seed);
        stopWatch.stop();
        System.out.println(stopWatch.toSplitString());
        System.out.println("布隆过滤器测试完毕,全部都能检测到");
        System.out.println("读取平均花费的时间为：" + totalTime / testCount + "ns");
    }

    @Test
    public void checkFile() throws Exception {
        random = new Random(seed);
        int testCount = 1000;
        DataTurboDetail diskDetail = new DataTurboDetail();
        diskDetail.setLoadEnum(LoadEnum.DISK);
        diskDetail.setDataId("testBloom");
        diskDetail.setProp(DataTurboConstants.BUILDER_CONFIG_BLOOM_EXCEPT_COUNT,
                ((Integer) testCount).toString());
        // diskDetail.setProp(DataTurboConstants.BUILDER_CONFIG_BLOOM_FPP, "0.001");
        diskDetail.setDataFile("test/test.bloom_disk");
        diskDetail = build(diskDetail, testCount, seed);
        DataTurboDetail heapDetail = new DataTurboDetail();
        heapDetail.setLoadEnum(LoadEnum.HEAP);
        heapDetail.setDataId("testBloom");
        heapDetail.setProp(DataTurboConstants.BUILDER_CONFIG_BLOOM_EXCEPT_COUNT,
                ((Integer) testCount).toString());
        // heapDetail.setProp(DataTurboConstants.BUILDER_CONFIG_BLOOM_FPP, "0.001");
        heapDetail.setDataFile("test/test.bloom_heap");
        heapDetail = build(heapDetail, testCount, seed);
        File file1 = new File("test/test.bloom_heap");
        File file2 = new File("test/test.bloom_disk");
        BufferedInputStream b1 = new BufferedInputStream(new FileInputStream(file1));
        BufferedInputStream b2 = new BufferedInputStream(new FileInputStream(file2));
        int i = 0;
        while (true) {
            int i1 = b1.read();
            int i2 = b2.read();
            if (i1 == -1 || i2 == -1) {
                break;
            }
            if (i1 != i2) {
                System.out.printf("%d, %s : %s%n", i, Integer.toBinaryString(i1),
                        Integer.toBinaryString(i2));
                throw new RuntimeException("不同模式出来的数据不一致");
            }
            i++;
        }
        b1.close();
        b2.close();
        System.out.println("不同模式文件一致~");
    }

}
