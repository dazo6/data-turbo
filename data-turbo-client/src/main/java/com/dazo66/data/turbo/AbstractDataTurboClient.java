package com.dazo66.data.turbo;

import com.dazo66.data.turbo.key.predictor.IKeyComparator;
import com.dazo66.data.turbo.model.DataTurboDetail;
import com.dazo66.data.turbo.model.DataTurboResult;

import java.util.Objects;

/**
 * 查询客户端
 *
 * @author dazo66
 **/
public abstract class AbstractDataTurboClient {

    private volatile DataTurboDetail dataTurboDetail;

    public AbstractDataTurboClient(DataTurboDetail dataTurboDetail) {
        this.dataTurboDetail = dataTurboDetail;
    }

    public final DataTurboDetail getDataTurboDetail() {
        return dataTurboDetail;
    }

    /**
     * kv查询数据
     * 如果没查询到 返回null
     *
     * @param key key
     * @return 查询到的对象
     */
    public abstract DataTurboResult search(String key);

    /**
     * 名单查询数据
     *
     * @param key key
     * @return 查询到的对象
     */
    public abstract boolean searchNameList(String key);

    /**
     * 根据现有的detail加载数据
     *
     * @throws Exception 如果在加载中出现异常会通过这个抛出
     */
    public abstract void load() throws Exception;

    /**
     * 通过一个新的detail对象进行重新加载
     *
     * @param dataTurboDetail 新的detail对象
     * @throws Exception 如果在重新加载中出现异常会通过这个抛出
     */
    public void reload(DataTurboDetail dataTurboDetail) throws Exception {
        synchronized (this) {
            this.dataTurboDetail = dataTurboDetail;
            load();
        }
    }

    /**
     * 关闭这个客户端 释放所有的内存和文件句柄
     *
     * @throws Exception 如果在关闭的过程中出问题会通过这个抛出
     */
    public abstract void close() throws Exception;

    /**
     * key 比较器 通过重写这个可以改变key排序规则
     * 对于查询key和存储key存在结构不一致的情况下使用这个方法
     * 比如存储的key是range化的 而查询的是非range化的
     *
     * @return 返回key比较器 如果要实现自定义的key比较器可以使用这个方法
     */
    public abstract IKeyComparator getComparator();

    @Override
    public int hashCode() {
        return dataTurboDetail != null ? dataTurboDetail.hashCode() : 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AbstractDataTurboClient client = (AbstractDataTurboClient) o;

        return Objects.equals(dataTurboDetail, client.dataTurboDetail);
    }
}
