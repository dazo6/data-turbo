package com.dazo66.data.turbo.encrypt;

/**
 * @author dazo66
 */
public interface IEncryptor {

    IEncryptor DEFAULT = new IEncryptor() {
        @Override
        public String decrypt(String src, String key) {
            return src;
        }

        @Override
        public String encrypt(String src, String key) {
            return src;
        }
    };

    /**
     * 通过key进行加密
     *
     * @param src 源数据
     * @param key 加密密钥 如果是不需要密钥的加密方式 可以为null
     * @return 加密后的字符串
     */
    String decrypt(String src, String key);

    /**
     * 解密
     *
     * @param src 加密后的字符串
     * @param key 解密密钥 如果是不需要密钥的加密方式 可以为null
     * @return 解密后的字符串
     */
    String encrypt(String src, String key);
}
