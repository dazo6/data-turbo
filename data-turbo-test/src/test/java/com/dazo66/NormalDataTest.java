package com.dazo66;

import com.alibaba.fastjson.JSONObject;
import com.dazo66.data.turbo.NormalDataTurboBuilder;
import com.dazo66.data.turbo.NormalDataTurboClient;
import com.dazo66.data.turbo.model.DataTurboDetail;
import com.dazo66.data.turbo.model.DataTurboResult;
import com.dazo66.data.turbo.model.LoadEnum;
import com.dazo66.data.turbo.util.DataTurboConstants;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * @author dazo66
 **/
public class NormalDataTest {

    @Test
    public void test0Field() throws Exception {
        Random random = new Random(seed);
        int testCount = 10000;
        DataTurboDetail dataTurboDetail = new DataTurboDetail();
        dataTurboDetail.setLoadEnum(LoadEnum.DISK);
        dataTurboDetail.setDataId("testNormal3");
        dataTurboDetail.setFields(new String[]{});
        dataTurboDetail.setProp(DataTurboConstants.BUILDER_CONFIG_NORMAL_WRITE_TIME, "false");
        NormalDataTurboBuilder normalDataTurboBuilder =
                new NormalDataTurboBuilder(dataTurboDetail) {
            @Override
            public int getMaxCountPreSplit() {
                return 1000;
            }
        };
        for (int i = 0; i < testCount; i++) {
            Map<String, Object> data = new HashMap<>();
            data.put("text", getRandomString1(random));
            normalDataTurboBuilder.inputData(parseIntIp(i), data);
        }
        DataTurboDetail dataTurboDetail1 = normalDataTurboBuilder.build();
        NormalDataTurboClient client = new NormalDataTurboClient(dataTurboDetail1);
        client.load();
        Thread.sleep(3000L);
        random = new Random(seed);
        long l = System.currentTimeMillis();
        for (int i = 0; i < testCount; i++) {
            String s = getRandomString1(random);
            String key = parseIntIp(i);
            DataTurboResult result = client.search(key);
            if (result == null) {
                System.out.println(key);
                throw new RuntimeException();
            } else {
                // System.out.println(true);
            }
        }
        System.out.println(System.currentTimeMillis() - l);
        System.out.println(((double) (System.currentTimeMillis() - l) / (testCount)));
        FileWriter output = new FileWriter(com.dazo66.data.turbo.util.IOUtils.createTempFile(
                "test.detail"));
        IOUtils.write(JSONObject.toJSONString(dataTurboDetail1), output);
        output.flush();
        output.close();
        for (int i = 0; i < 1000000; i++) {
            String s = getRandomString1(random);
            DataTurboResult result = client.search(s);
            if (result != null) {
                System.out.println(s);
                throw new RuntimeException();
            }
        }
    }

    @Test
    public void test0FieldHeap() throws Exception {
        Random random = new Random(seed);
        random.setSeed(seed);
        int testCount = 10000;
        DataTurboDetail dataTurboDetail = new DataTurboDetail();
        dataTurboDetail.setLoadEnum(LoadEnum.HEAP);
        dataTurboDetail.setDataId("testNormal1");
        dataTurboDetail.setFields(new String[]{});
        dataTurboDetail.setProp(DataTurboConstants.BUILDER_CONFIG_NORMAL_WRITE_TIME, "false");
        NormalDataTurboBuilder normalDataTurboBuilder =
                new NormalDataTurboBuilder(dataTurboDetail) {
            @Override
            public int getMaxCountPreSplit() {
                return 1000;
            }
        };
        for (int i = 0; i < testCount; i++) {
            Map<String, Object> data = new HashMap<>();
            data.put("text", getRandomString1(random));
            normalDataTurboBuilder.inputData(parseIntIp(i), data);
        }
        DataTurboDetail dataTurboDetail1 = normalDataTurboBuilder.build();
        NormalDataTurboClient client = new NormalDataTurboClient(dataTurboDetail1);
        client.load();
        random = new Random(seed);
        long l = System.currentTimeMillis();
        for (int i = 0; i < testCount; i++) {
            String s = getRandomString1(random);
            String key = parseIntIp(i);
            DataTurboResult result = client.search(key);
            if (result == null) {
                System.out.println(key);
                throw new RuntimeException();
            } else {
                // System.out.println(true);
            }
        }
        System.out.println(System.currentTimeMillis() - l);
        System.out.println(((double) (System.currentTimeMillis() - l) / (testCount)));
        FileWriter output = new FileWriter(com.dazo66.data.turbo.util.IOUtils.createTempFile(
                "test.detail"));
        IOUtils.write(JSONObject.toJSONString(dataTurboDetail1), output);
        output.flush();
        output.close();
        for (int i = 0; i < 1000000; i++) {
            String s = getRandomString1(random);
            DataTurboResult result = client.search(s);
            if (result != null) {
                System.out.println(s);
                throw new RuntimeException();
            }
        }
    }

    @Test
    public void test2Field() throws Exception {
        int testCount = 10000;
        Random random = new Random(seed);
        DataTurboDetail dataTurboDetail = new DataTurboDetail();
        dataTurboDetail.setLoadEnum(LoadEnum.HEAP);
        dataTurboDetail.setDataId("testNormal1");
        dataTurboDetail.setFields(new String[]{"text1", "text2"});
        dataTurboDetail.setProp(DataTurboConstants.BUILDER_CONFIG_NORMAL_WRITE_TIME, "false");
        NormalDataTurboBuilder normalDataTurboBuilder =
                new NormalDataTurboBuilder(dataTurboDetail) {
            @Override
            public int getMaxCountPreSplit() {
                return -1;
            }
        };
        for (int i = 0; i < testCount; i++) {
            Map<String, Object> data = new HashMap<>();
            data.put("text1", getRandomString1(random));
            data.put("text2", getRandomString1(random));
            normalDataTurboBuilder.inputData(getRandomString(random), data);
        }
        DataTurboDetail dataTurboDetail1 = normalDataTurboBuilder.build();
        NormalDataTurboClient client = new NormalDataTurboClient(dataTurboDetail1);
        client.load();
        random = new Random(seed);
        long l = System.currentTimeMillis();
        for (int i = 0; i < testCount; i++) {
            String s1 = getRandomString1(random);
            String s2 = getRandomString1(random);
            String key = getRandomString(random);
            DataTurboResult result = client.search(key);
            if (result == null || !s1.equals(result.getString("text1")) || !s2.equals(result.getString("text2"))) {
                System.out.println(key);
                throw new RuntimeException();
            } else {
                // System.out.println(true);
            }
        }
        System.out.println(System.currentTimeMillis() - l);
        System.out.println(((double) (System.currentTimeMillis() - l) / (testCount)));
        FileWriter output = new FileWriter(com.dazo66.data.turbo.util.IOUtils.createTempFile(
                "test.detail"));
        IOUtils.write(JSONObject.toJSONString(dataTurboDetail1), output);
        output.flush();
        output.close();
        for (int i = 0; i < 10000; i++) {
            String s = getRandomString1(random);
            DataTurboResult result = client.search(s);
            if (result != null) {
                System.out.println(s);
                throw new RuntimeException();
            }
        }
    }

    @Test
    public void testEmptyKey() throws Exception {
        int testCount = 1;
        Random random = new Random(seed);
        DataTurboDetail dataTurboDetail = new DataTurboDetail();
        dataTurboDetail.setLoadEnum(LoadEnum.DISK);
        dataTurboDetail.setDataId("testNormal2");
        dataTurboDetail.setFields(new String[]{"text1", "text2"});
        dataTurboDetail.setProp(DataTurboConstants.BUILDER_CONFIG_NORMAL_WRITE_TIME, "false");
        NormalDataTurboBuilder normalDataTurboBuilder =
                new NormalDataTurboBuilder(dataTurboDetail) {
            @Override
            public int getMaxCountPreSplit() {
                return -1;
            }
        };
        for (int i = 0; i < testCount; i++) {
            Map<String, Object> data = new HashMap<>();
            data.put("text1", getRandomString2(random));
            data.put("text2", getRandomString2(random));
            normalDataTurboBuilder.inputData("", data);
        }
        DataTurboDetail dataTurboDetail1 = normalDataTurboBuilder.build();
        NormalDataTurboClient client = new NormalDataTurboClient(dataTurboDetail1);
        client.load();
        random = new Random(seed);
        long l = System.currentTimeMillis();
        for (int i = 0; i < testCount; i++) {
            String s1 = getRandomString2(random);
            String s2 = getRandomString2(random);
            String key = "";
            DataTurboResult result = client.search(key);
            if (result == null || !s1.equals(result.getString("text1")) || !s2.equals(result.getString("text2"))) {
                System.out.println(key);
                throw new RuntimeException();
            } else {
                // System.out.println(true);
            }
        }
        System.out.println(System.currentTimeMillis() - l);
        System.out.println(((double) (System.currentTimeMillis() - l) / (testCount)));
        FileWriter output = new FileWriter(com.dazo66.data.turbo.util.IOUtils.createTempFile(
                "test.detail"));
        IOUtils.write(JSONObject.toJSONString(dataTurboDetail1), output);
        output.flush();
        output.close();
        for (int i = 0; i < 10000; i++) {
            String s = getRandomString2(random);
            DataTurboResult result = client.search(s);
            if (result != null) {
                System.out.println(s);
                throw new RuntimeException();
            }
        }
    }

    @Test
    public void testError() throws Exception {
        int testCount = 10000;
        Random random = new Random(seed);
        DataTurboDetail dataTurboDetail = new DataTurboDetail();
        dataTurboDetail.setLoadEnum(LoadEnum.DISK);
        dataTurboDetail.setDataId("testNormal3");
        dataTurboDetail.setFields(new String[]{"text1", "text2"});
        dataTurboDetail.setProp(DataTurboConstants.BUILDER_CONFIG_NORMAL_WRITE_TIME, "false");
        dataTurboDetail.setProp(DataTurboConstants.BUILDER_CONFIG_NORMAL_ERROR_TOLERATING, "0");
        NormalDataTurboBuilder normalDataTurboBuilder =
                new NormalDataTurboBuilder(dataTurboDetail) {
            @Override
            public int getMaxCountPreSplit() {
                return -1;
            }
        };
        for (int i = 0; i < testCount; i++) {
            Map<String, Object> data = new HashMap<>();
            data.put("text1", getRandomString(random));
            data.put("text2", getRandomString(random));
            String key = getRandomString(random);
            try {
                normalDataTurboBuilder.inputData(key, data);
            } catch (Exception e) {
                if (!key.equals("c7Xs")) {
                    throw new RuntimeException(e);
                }
            }
        }
        DataTurboDetail dataTurboDetail1 = normalDataTurboBuilder.build();
        NormalDataTurboClient client = new NormalDataTurboClient(dataTurboDetail1);
        client.load();
        random = new Random(seed);
        long l = System.currentTimeMillis();
        for (int i = 0; i < testCount; i++) {
            String s1 = getRandomString(random);
            String s2 = getRandomString(random);
            String key = getRandomString(random);
            DataTurboResult result = client.search(key);
            if (result == null || !s1.equals(result.getString("text1")) || !s2.equals(result.getString("text2"))) {
                System.out.println(key);
                if (!key.equals("c7Xs")) {
                    throw new RuntimeException();
                }
            } else {
                // System.out.println(true);
            }
        }
        System.out.println(System.currentTimeMillis() - l);
        System.out.println(((double) (System.currentTimeMillis() - l) / (testCount)));
        FileWriter output = new FileWriter(com.dazo66.data.turbo.util.IOUtils.createTempFile(
                "test.detail"));
        IOUtils.write(JSONObject.toJSONString(dataTurboDetail1), output);
        output.flush();
        output.close();
        for (int i = 0; i < 10000; i++) {
            String s = getRandomString2(random);
            DataTurboResult result = client.search(s);
            if (result != null) {
                System.out.println(s);
                throw new RuntimeException();
            }
        }
    }

    @Test
    public void testLongField() throws Exception {
        int testCount = 2000;
        Random random = new Random(seed);
        DataTurboDetail dataTurboDetail = new DataTurboDetail();
        dataTurboDetail.setLoadEnum(LoadEnum.DISK);
        dataTurboDetail.setDataId("testNormal4");
        dataTurboDetail.setFields(new String[]{"text1", "text2"});
        dataTurboDetail.setProp(DataTurboConstants.BUILDER_CONFIG_NORMAL_WRITE_TIME, "false");
        NormalDataTurboBuilder normalDataTurboBuilder =
                new NormalDataTurboBuilder(dataTurboDetail) {
            @Override
            public int getMaxCountPreSplit() {
                return -1;
            }
        };
        for (int i = 0; i < testCount; i++) {
            Map<String, Object> data = new HashMap<>();
            data.put("text1", getRandomString2(random));
            data.put("text2", getRandomString2(random));
            normalDataTurboBuilder.inputData(getRandomString2(random), data);
        }
        DataTurboDetail dataTurboDetail1 = normalDataTurboBuilder.build();
        NormalDataTurboClient client = new NormalDataTurboClient(dataTurboDetail1);
        client.load();
        random = new Random(seed);
        long l = System.currentTimeMillis();
        for (int i = 0; i < testCount; i++) {
            String s1 = getRandomString2(random);
            String s2 = getRandomString2(random);
            String key = getRandomString2(random);
            DataTurboResult result = client.search(key);
            if (result == null || !s1.equals(result.getString("text1")) || !s2.equals(result.getString("text2"))) {
                System.out.println(key);
                throw new RuntimeException();
            } else {
                // System.out.println(true);
            }
        }
        System.out.println(System.currentTimeMillis() - l);
        System.out.println(((double) (System.currentTimeMillis() - l) / (testCount)));
        FileWriter output = new FileWriter(com.dazo66.data.turbo.util.IOUtils.createTempFile(
                "test.detail"));
        IOUtils.write(JSONObject.toJSONString(dataTurboDetail1), output);
        output.flush();
        output.close();
        for (int i = 0; i < 10000; i++) {
            String s = getRandomString2(random);
            DataTurboResult result = client.search(s);
            if (result != null) {
                System.out.println(s);
                throw new RuntimeException();
            }
        }
    }
    private static final String str =
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    private static final String str1 = "收到时代和我抢会发生看2361872631hdlkjsahadjcqwoiuey1983cpiuzhczoixj" +
            "到击破强迫你脾气饿哦是丢啊回收丢啊丢啊和深哦啊三大件三大件按时u大师u的白啤酒度和";
    private static final long seed = 12379127398123L;

    public static String getRandomString2(Random random) {
        StringBuilder sb = new StringBuilder();
        int length = 4000 + random.nextInt(30);
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(str1.length());
            sb.append(str1.charAt(number));
        }
        return sb.toString();
    }

    public static String getRandomString1(Random random) {
        StringBuilder sb = new StringBuilder();
        int length = 40 + random.nextInt(30);
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(str1.length());
            sb.append(str1.charAt(number));
        }
        return sb.toString();
    }

    public static String getRandomString(Random random) {
        StringBuilder sb = new StringBuilder();
        int length = 4 + random.nextInt(10);
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(str.length());
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }

    public static byte[] int2Bytes(int data) {
        byte[] bytes = new byte[4];

        for (int i = 0; i < 4; i++) {
            bytes[i] = (byte) ((data >> 24 - 8 * i) & 0xFF);
        }

        return bytes;
    }

    public static String parseIntIp(int ip) {
        byte[] bytes = int2Bytes(ip);
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 4; i++) {
            int network = bytes[i] & 0xFF;
            builder.append(network);
            if (i != 3) {
                builder.append(".");
            }
        }
        return builder.toString();
    }
}
