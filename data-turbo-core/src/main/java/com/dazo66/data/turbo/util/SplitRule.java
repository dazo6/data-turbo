/*
 * Copyright (C) 2011 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

/*
 * MurmurHash3 was written by Austin Appleby, and is placed in the public
 * domain. The author hereby disclaims copyright to this source code.
 */

/*
 * Source:
 * https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp
 * (Modified to adapt to Guava coding conventions and to use the HashFunction interface)
 */

package com.dazo66.data.turbo.util;

/**
 * @author dazo66
 */
public interface SplitRule {

    /**
     * 获得字符类型 同样的类型会进行切割
     * 其中 返回0 则表示忽略
     *
     * @param c 传入的字符
     * @return 字符对应的枚举数值 0表示忽略
     */
    int getCharType(char c);
    /**
     * 默认分割方案 使用空格进行分割
     */
    SplitRule NORMAL_SPLIT_RULE = c -> c == ' ' ? 0 : 1;
}
