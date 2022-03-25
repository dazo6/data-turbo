package com.dazo66.data.turbo.util;

/**
 * @author dazo66
 **/
public class MemoryUtils {

    public static long getFreeMemoryOfJVM() {
        return Runtime.getRuntime().maxMemory() - (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
    }

    public static boolean checkFreeMemory(long mem) {
        boolean result = false;

        Runtime runTime = Runtime.getRuntime();
        long maxMem = runTime.maxMemory();
        long totalMem = runTime.totalMemory();
        long freeMem = runTime.freeMemory();
        if (mem < maxMem - totalMem + freeMem) {
            result = true;
        }
        return result;
    }


}
