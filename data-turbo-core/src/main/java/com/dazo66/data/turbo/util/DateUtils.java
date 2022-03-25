package com.dazo66.data.turbo.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author dazo66
 **/
public class DateUtils {

    public static String getDataVersion() {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd-hhmmss");
        return format.format(new Date());
    }

}
