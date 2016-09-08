package com.udbac.hadoop.util;

import org.junit.Test;

/**
 * Created by root on 2016/7/21.
 */
public class TimeUtilTest {
    @Test
    public void getYesterday() throws Exception {
        System.out.println(new TimeUtil().getYesterday());
    }

    @Test
    public void parseStringDate2Long() throws Exception {
        System.out.println(new TimeUtil().parseStringDate2Long("2011-01-02"));
    }

    @Test
    public void parseLong2StringDate() throws Exception {
        System.out.println(new TimeUtil().parseLong2StringDate(1000));
    }

    @Test
    public void longToTime() throws Exception {
        System.out.println(new TimeUtil().longToTime(1000));
    }

    @Test
    public void timeToLong() throws Exception {
        System.out.println(new TimeUtil().timeToLong("235959"));
        System.out.println(Integer.MAX_VALUE);
    }


}