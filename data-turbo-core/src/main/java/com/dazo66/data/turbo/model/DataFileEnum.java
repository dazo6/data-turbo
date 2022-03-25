package com.dazo66.data.turbo.model;

/**
 * @author dazo66
 **/
public enum DataFileEnum {

    /**
     * 布隆过滤器类型的数据文件
     * <p>
     * 准确值可以通过detail的config配置 默认使用 0.001
     */
    BLOOM("BLOOM"),


    /**
     * 普通打包文件 使用的保留字符为3个 \u0011 \u0012 \u0013 \u0014
     * 数据文件定义：
     * 1. 8位long 表示数据生成的时间戳
     * 2. 8位long 表示数量
     * 3. 8位long 表示索引数据块大小
     * 4. 8位long 表示values数据块的大小
     * 5. 8位long 引用数据块大小
     * 6. 4位int  表示字段数据长度
     * 7. string  可用字段，以'\u0011' 作为分隔符 字段中不允许出现 '\u0011'
     * 8. 索引数据块  索引数据块
     * 索引文件包含key和位于信息数据块的pos(long)。
     * 索引数量越多后续二分查找数据越快。
     * 索引数默认512可通过配置调整。
     * 通过 key + \u0011 + 8位long + \u0012 循环形成。
     * 9. 信息数据块
     * 结构是 记录分隔符(\u0013) + key + 字段分隔符('\u0012') + fieldValue1 + 字段分隔符('\u0012') + fieldValue2 ...
     * 信息数据块是加密的通过分割符 '\u0011'
     * 字段值可以是引用格式  \u0014+8位long(pos)+4位int(len)
     * 如果在构建过程中原始数据遇到分隔符可以直接移除 或者是 写入异常文件中。
     * 加密算法可以自定义。
     * 10. 引用数据块
     * 重复的数据值可以放到这里
     */
    NORMAL("NORMAL");

    private final String id;

    DataFileEnum(String i) {
        this.id = i;
    }

    public String getId() {
        return id;
    }
}
