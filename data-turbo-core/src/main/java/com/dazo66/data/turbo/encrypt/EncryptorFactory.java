package com.dazo66.data.turbo.encrypt;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 加密器工厂 用于获得加密器
 *
 * @author dazo66
 */
public class EncryptorFactory {

    private static Map<String, IEncryptor> map = new ConcurrentHashMap<>();

    static {
        map.put("none", new IEncryptor() {
            @Override
            public String decrypt(String src, String key) {
                return src;
            }

            @Override
            public String encrypt(String src, String key) {
                return src;
            }
        });
    }


    public static Set<String> getAllEncryptor() {
        return map.keySet();
    }

    public static void registerEncryptor(String key, IEncryptor encryptor) {
        if (map.containsKey(key)) {
            throw new RuntimeException("This encryptor already exists!");
        } else {
            map.put(key, encryptor);
        }
    }

}
