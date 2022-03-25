package com.dazo66.data.turbo.model;

/**
 * @author dazo66
 **/
public enum KeyEnum {

    /**
     * 字符串类型的key
     */
    STRING("string");

    private String id;

    private KeyEnum(String i) {
        this.id = i;
    }

    public String getId() {
        return id;
    }

    ;
}
