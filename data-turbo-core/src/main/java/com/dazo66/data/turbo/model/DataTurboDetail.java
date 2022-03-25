package com.dazo66.data.turbo.model;

import com.dazo66.data.turbo.util.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 一个数据增强文件的细节文件
 * 生成构建器和读取器都会用到
 *
 * @author dazo66
 **/
public class DataTurboDetail {

    /**
     * 数据源id
     */
    private String dataId;
    /**
     * 数据文件
     */
    private String dataFile;
    /**
     * 数据版本，一般是 yyyyMMdd hh:mm:ss 格式
     */
    private String dataVersion;
    /**
     * 打包keeper版本
     */
    private String keeperVersion;

    /**
     * 关键词类型
     */
    private KeyEnum keyEnum;

    /**
     * 数据文件类型 不同的数据文件会生成不同的数据块
     *
     * @see DataFileEnum 参考这里面的数据文件定义
     */
    private DataFileEnum dataFileEnum;

    /**
     * 一些自定义配置
     */
    private Map<String, String> config = new HashMap<>();

    /**
     * 返回字段
     */
    private String[] fields;

    /**
     * 加载模式 默认为内存模式
     */
    private LoadEnum loadEnum = LoadEnum.HEAP;

    /**
     * 补丁对象又称增量对象
     * 会优先查这个对象内的数据
     */
    private List<DataTurboDetail> patches = new ArrayList<>();

    public String getDataId() {
        return dataId;
    }

    public DataTurboDetail setDataId(String dataId) {
        this.dataId = dataId;
        return this;
    }

    public LoadEnum getLoadEnum() {
        return loadEnum;
    }

    public DataTurboDetail setLoadEnum(LoadEnum loadEnum) {
        this.loadEnum = loadEnum;
        return this;
    }

    public String getDataFile() {
        return dataFile;
    }

    public DataTurboDetail setDataFile(String dataFile) {
        this.dataFile = dataFile;
        return this;
    }

    public String getDataVersion() {
        return dataVersion;
    }

    public DataTurboDetail setDataVersion(String dataVersion) {
        this.dataVersion = dataVersion;
        return this;
    }

    public String getKeeperVersion() {
        return keeperVersion;
    }

    public DataTurboDetail setKeeperVersion(String keeperVersion) {
        this.keeperVersion = keeperVersion;
        return this;
    }

    public KeyEnum getKeyEnum() {
        return keyEnum;
    }

    public DataTurboDetail setKeyEnum(KeyEnum keyEnum) {
        this.keyEnum = keyEnum;
        return this;
    }

    public String[] getFields() {
        return fields;
    }

    public DataTurboDetail setFields(String[] fields) {
        this.fields = fields;
        return this;
    }

    public DataFileEnum getDataFileEnum() {
        return dataFileEnum;
    }

    public DataTurboDetail setDataFileEnum(DataFileEnum dataFileEnum) {
        this.dataFileEnum = dataFileEnum;
        return this;
    }

    public Map<String, String> getConfig() {
        return config;
    }

    public DataTurboDetail setConfig(Map<String, String> config) {
        this.config = config;
        return this;
    }

    public DataTurboDetail setProp(String key, String value) {
        config.put(key, value);
        return this;
    }

    public String getProp(String key, String defaultValue) {
        return config.getOrDefault(key, defaultValue);
    }

    public String getProp(String key) {
        return config.get(key);
    }

    public List<DataTurboDetail> getPatches() {
        return patches;
    }

    public DataTurboDetail setPatches(List<DataTurboDetail> patches) {


        this.patches = patches;
        return this;
    }

    public DataTurboDetail addPatch(DataTurboDetail dataTurboDetail) {
        Preconditions.checkArgument(dataTurboDetail != null, "Patch can not be null");
        Preconditions.checkArgument(!dataTurboDetail.equals(this), "Patch can not be self");
        this.patches.add(dataTurboDetail);
        return this;
    }
}
