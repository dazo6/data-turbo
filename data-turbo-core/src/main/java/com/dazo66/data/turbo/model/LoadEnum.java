package com.dazo66.data.turbo.model;

/**
 * @author dazo66
 */

public enum LoadEnum {

    /**
     * 字符串类型的key
     */
    HEAP("heap"), DISK("disk");

    private String id;

    private LoadEnum(String i) {
        this.id = i;
    }

    public String getId() {
        return id;
    }

    ;


}
