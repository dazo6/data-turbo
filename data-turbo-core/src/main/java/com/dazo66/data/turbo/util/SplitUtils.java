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

import java.util.ArrayList;
import java.util.List;

/**
 * @author dazo66
 **/
public class SplitUtils {

    public static List<String> split(String src, SplitRule rule) {
        int last = 0;
        StringBuilder builder = new StringBuilder();
        List<String> strings = new ArrayList<>();
        for (int j = 0; j < src.length(); j++) {
            char c = src.charAt(j);
            int t = rule.getCharType(c);
            if (j != 0) {
                if (last != t && builder.length() != 0) {
                    strings.add(builder.toString());
                    builder = new StringBuilder();
                }
            }
            if (t != 0) {
                builder.append(c);
            }
            last = t;
        }
        if (builder.length() != 0) {
            strings.add(builder.toString());
        }
        return strings;
    }

    public static String join(List<String> strings, String split) {
        if (strings == null) {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < strings.size(); i++) {
            builder.append(strings.get(i));
            if (i != strings.size() - 1) {
                builder.append(split);
            }
        }
        return builder.toString();
    }


}
