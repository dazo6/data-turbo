package com.dazo66.data.turbo.model;

import com.dazo66.data.turbo.util.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * 通用的工厂类
 *
 * @author dazo66
 **/
public abstract class AbstractFactory<E, T, R> {

    private Map<E, Function<T, R>> map = new HashMap<>();

    /**
     * 注册对象进入
     *
     * @param e 枚举类型
     * @param f 构建方法
     * @return
     */
    public Function<T, R> register(E e, Function<T, R> f) {
        Preconditions.checkArgument(e != null && f != null, "Argument can not be null!");
        return map.put(e, f);
    }

    public R get(E e, T t) {
        Preconditions.checkArgument(e != null && t != null, "Argument can not be null!");
        Preconditions.checkArgument(map.containsKey(e), "Enum type must be contain!");
        return map.get(e).apply(t);
    }
}
