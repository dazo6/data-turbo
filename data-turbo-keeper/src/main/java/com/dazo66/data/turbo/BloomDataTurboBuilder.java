package com.dazo66.data.turbo;

import com.dazo66.data.turbo.model.DataTurboDetail;
import com.dazo66.data.turbo.model.KeeperVersion;
import com.dazo66.data.turbo.model.LoadEnum;
import com.dazo66.data.turbo.util.*;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * 布隆过滤器数据结构构建类
 * 可用配置如下
 * [must] {@linkplain DataTurboConstants#BUILDER_CONFIG_BLOOM_EXCEPT_COUNT} 预计的数量
 * [not must] {@linkplain DataTurboConstants#BUILDER_CONFIG_BLOOM_FPP} Bloom fpp 默认为 0.001
 *
 * @author dazo66
 **/
public class BloomDataTurboBuilder extends AbstractDataTurboBuilder {

    private IBloomFilter<CharSequence> bloomFilter;

    public BloomDataTurboBuilder(DataTurboDetail dataTurboDetail) {
        super(dataTurboDetail);
        String expectedInsertions =
                dataTurboDetail.getProp(DataTurboConstants.BUILDER_CONFIG_BLOOM_EXCEPT_COUNT);
        Preconditions.checkArgument(expectedInsertions != null, "%s must be not null",
                DataTurboConstants.BUILDER_CONFIG_BLOOM_EXCEPT_COUNT);
        int expect = Integer.parseInt(expectedInsertions);
        String charset = dataTurboDetail.getProp(DataTurboConstants.BUILDER_CONFIG_BLOOM_CHARSET,
                "utf-8");
        double fpp =
                Double.parseDouble(dataTurboDetail.getProp(DataTurboConstants.BUILDER_CONFIG_BLOOM_FPP, "0.001"));
        if (dataTurboDetail.getLoadEnum() == LoadEnum.HEAP) {
            bloomFilter = HeapBloomFilter.create(Funnels.stringFunnel(Charset.forName(charset)),
                    expect, fpp);
        } else if (dataTurboDetail.getLoadEnum() == LoadEnum.DISK) {
            bloomFilter = DiskBloomFilter.create(Funnels.stringFunnel(Charset.forName(charset)),
                    expect, fpp);
        }
    }

    @Override
    public int getMaxCountPreSplit() {
        return 0;
    }

    @Override
    public boolean inputData(String key, Map<String, Object> map) {
        return bloomFilter.put(key);
    }

    @Override
    public DataTurboDetail build() throws Exception {
        File file = new File(getDataTurboDetail().getDataFile());
        // 如果设定的是路径
        if (file.exists() && file.isDirectory()) {
            file = new File(file.getAbsolutePath() + "/" + String.format("%s_%d.bloom",
                    getDataTurboDetail().getDataId(), System.currentTimeMillis()));
        }
        if (!file.exists()) {
            File parentFile = file.getParentFile();
            if (parentFile != null) {
                parentFile.mkdirs();
            }
        }
        // 写入文件
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
        bloomFilter.writeTo(bufferedOutputStream);
        bufferedOutputStream.close();
        // 关闭bloom
        bloomFilter.close();
        // 构建新的detail
        DataTurboDetail ret = new DataTurboDetail();
        ret.setConfig(getDataTurboDetail().getConfig()).setDataFile(file.getAbsolutePath()).setDataFileEnum(getDataTurboDetail().getDataFileEnum()).setKeeperVersion(KeeperVersion.VERSION).setKeyEnum(getDataTurboDetail().getKeyEnum()).setFields(null).setDataId(getDataTurboDetail().getDataId()).setDataVersion(DateUtils.getDataVersion()).setLoadEnum(getDataTurboDetail().getLoadEnum());
        return ret;
    }
}
