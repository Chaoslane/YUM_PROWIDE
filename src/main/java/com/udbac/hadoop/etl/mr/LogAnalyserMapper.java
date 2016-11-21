package com.udbac.hadoop.etl.mr;

import com.udbac.hadoop.common.DefinedKey;
import com.udbac.hadoop.util.SplitValueBuilder;
import com.udbac.hadoop.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by chaoslane@126.com on 2016/7/25.
 * 读入数据源的mapper
 */

public class LogAnalyserMapper extends Mapper<LongWritable, Text, DefinedKey, Text> {
    private final Logger logger = Logger.getLogger(LogAnalyserMapper.class);
    private Map<DefinedKey,Text> mapoutput = new HashMap<>();
    private String inputPath = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        //读取输入文件的文件夹名
        inputPath = configuration.get("inputPath_directorry_name");
        mapoutput.clear();
    }

    /**
     * 读入一行数据 根据分割符切割后 为自定义key赋值
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            if (null == value)
                return;
            //切割字符串 获取 date_time 进行+8的操作
            String[] tokens = value.toString().split("\\|");
            String date_time = TimeUtil.handleTime(tokens[1]).replace("-","");
            //判断输入文件夹名 和 输入数据的日期是否相等
//            if (!inputPath.contains(date_time.substring(0,9)))
//                return;
            DefinedKey definedKey = new DefinedKey();
            SplitValueBuilder svb = new SplitValueBuilder();

            if (tokens.length==12) {
                svb.add(tokens[0]).add(date_time);
                for (int i = 2; i < tokens.length; i++) {
                    svb.add(tokens[i]);
                }
                //给自定义key赋值 以进行 对id+time的二次排序
                definedKey.setDeviceId(tokens[0]);
                definedKey.setTimeStr(TimeUtil.parseStringDate2Long(date_time));
            }
            mapoutput.put(definedKey, new Text(svb.toString()));
        } catch (Exception e) {
            e.printStackTrace();
            this.logger.error("处理SDCLOG出现异常，数据:" + value + e);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<DefinedKey, Text> entry : mapoutput.entrySet()) {
            context.write(entry.getKey(), entry.getValue());
        }
    }

}
