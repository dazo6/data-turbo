package com.dazo66.data.turbo.util;

import java.io.Closeable;
import java.io.OutputStream;

/**
 * @author dazo66
 */
public interface IBloomFilter<T> extends Closeable {

    /**
     * 添加元素
     *
     * @param t 元素
     * @return
     */
    boolean put(T t);

    /**
     * 输出到流
     *
     * @param outputStream 输出流
     * @return
     */
    void writeTo(OutputStream outputStream) throws Exception;


}
