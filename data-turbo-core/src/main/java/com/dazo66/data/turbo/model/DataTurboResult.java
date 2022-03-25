package com.dazo66.data.turbo.model;

import java.util.Map;

/**
 * 查询后的返回类
 *
 * @author dazo66
 **/
public class DataTurboResult {

    private final String key;
    private final Map<String, String> data;

    public DataTurboResult(String key, Map<String, String> data) {
        this.key = key;
        this.data = data;
    }

    public String getKey() {
        return key;
    }

    public Map<String, String> getData() {
        return data;
    }

    public String getString(String key) {
        return data.get(key);
    }

    public Integer getInt(String key) {
        try {
            return Integer.parseInt(data.get(key));
        } catch (Exception e) {
            return null;
        }
    }

    public Boolean getBoolean(String key) {
        try {
            return Boolean.parseBoolean(data.get(key));
        } catch (Exception e) {
            return null;
        }
    }

    public Long getLong(String key) {
        try {
            return Long.parseLong(data.get(key));
        } catch (Exception e) {
            return null;
        }
    }

    public Double getDouble(String key) {
        try {
            return Double.parseDouble(data.get(key));
        } catch (Exception e) {
            return null;
        }
    }

    public Byte getByte(String key) {
        try {
            return Byte.parseByte(data.get(key));
        } catch (Exception e) {
            return null;
        }
    }
}
