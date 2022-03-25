package com.dazo66.data.turbo.key.predictor;

/**
 * @author dazo66
 */
public interface IKeyComparator {

    /**
     * 获得关键词的hash
     *
     * @param
     * @return
     */
    int compare(String key1, String key2);

}
