package com.dazo66.data.turbo.key.predictor;

/**
 * @author dazo66
 **/
public class StringKeyComparator implements IKeyComparator {
    @Override
    public int compare(String key1, String key2) {
        if (key1 == null && key2 == null) {
            return 0;
        }
        if (key1 == null) {
            return -1;
        }
        if (key2 == null) {
            return 1;
        }
        return key1.compareTo(key2);
    }
}
