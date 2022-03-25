package com.dazo66.data.turbo;

import com.dazo66.data.turbo.encrypt.IEncryptor;
import com.dazo66.data.turbo.key.predictor.IKeyComparator;
import com.dazo66.data.turbo.key.predictor.StringKeyComparator;
import com.dazo66.data.turbo.model.DataFileEnum;
import com.dazo66.data.turbo.model.DataTurboDetail;
import com.dazo66.data.turbo.model.KeyEnum;
import com.dazo66.data.turbo.model.LoadEnum;

import java.util.Map;

/**
 * 核心的数据构建器
 *
 * @author dazo66
 * @see DataTurboDetail
 * @see KeyEnum
 * @see LoadEnum
 * @see DataFileEnum
 * @see IKeyComparator
 * @see IEncryptor
 **/
public abstract class AbstractDataTurboBuilder {

    private final DataTurboDetail dataTurboDetail;
    private IKeyComparator keyPredictor = new StringKeyComparator();
    private IEncryptor encryptor = IEncryptor.DEFAULT;

    public AbstractDataTurboBuilder(DataTurboDetail dataTurboDetail) {
        this.dataTurboDetail = dataTurboDetail;
    }

    public DataTurboDetail getDataTurboDetail() {
        return dataTurboDetail;
    }

    /**
     * key关键词比较器
     *
     * @return
     */
    public IKeyComparator getKeyPredictor() {
        return keyPredictor;
    }

    public void setKeyPredictor(IKeyComparator keyPredictor) {
        this.keyPredictor = keyPredictor;
    }

    /**
     * 获得加密器
     *
     * @return 加密器
     */
    public IEncryptor getEncryptor() {
        return encryptor;
    }

    public void setEncryptor(IEncryptor encryptor) {
        this.encryptor = encryptor;
    }

    /**
     * 多少key进行一次分片
     * 这个数据量的key会都在内存中排序存着
     * 之后进行合并 不推荐设置过少，也不推荐设置过多
     * 与打包机器的内存和打包数据集的key类型有关
     *
     * @return key
     */
    public abstract int getMaxCountPreSplit();

    /**
     * 传入数据
     *
     * @param key key
     * @param map 一个map作为一条记录的值
     * @return 是否插入成功 如果有重复的key 或者value值过大会返回false
     * 上层需要保存插入错误的数据用于记录
     */
    public abstract boolean inputData(String key, Map<String, Object> map);

    /**
     * 以现有的数据进行构建
     *
     * @return 构建好的detail对象
     */
    public abstract DataTurboDetail build() throws Exception;


}
