package com.udbac.hadoop.common;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;

/**
 * Created by chaoslane@126.com on 2016/7/25.
 * 自定义排序类 根据自定义key的两个fields进行二次排序
 */
public class DefinedComparator extends WritableComparator {
    private static final Logger logger = Logger.getLogger(DefinedKey.class);

    public DefinedComparator() {
        super(DefinedKey.class, true);
    }

    public int compare(WritableComparable combinationKeyOne,
                       WritableComparable combinationKeyOther) {
        logger.debug("---------进入自定义排序---------");

        DefinedKey keyOne = (DefinedKey) combinationKeyOne;
        DefinedKey keyOther = (DefinedKey) combinationKeyOther;

        //二次
        if (!keyOne.getDeviceId().equals(keyOther.getDeviceId())) {
            logger.debug("---------退出自定义排序1---------");
            return keyOne.getDeviceId().compareTo(keyOther.getDeviceId());
        } else {
            logger.debug("---------退出自定义排序2---------");
            return keyOne.getTimeStr().compareTo(keyOther.getTimeStr());
        }

    }
}
