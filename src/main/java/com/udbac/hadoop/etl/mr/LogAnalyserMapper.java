package com.udbac.hadoop.etl.mr;

import com.udbac.hadoop.common.DefinedKey;
import com.udbac.hadoop.util.SplitValueBuilder;
import com.udbac.hadoop.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by chaoslane@126.com on 2016/7/25.
 * 读入数据源的mapper
 */

public class LogAnalyserMapper extends Mapper<LongWritable, Text, DefinedKey, Text> {
    private final Logger logger = Logger.getLogger(LogAnalyserMapper.class);
    private Map<DefinedKey,Text> mapoutput = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
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
            if (value.getLength()==0)
                return;
            //切割字符串 获取 date_time 进行+8的操作
            String[] tokens = StringUtils.split(value.toString(),"\t");
            tokens[1] = TimeUtil.handleTime(tokens[1]).replace("-","");

            DefinedKey definedKey = new DefinedKey();
            SplitValueBuilder svb = new SplitValueBuilder();

            if (tokens.length==11) {
                for (int i =2; i<tokens.length;i++) {
                    svb.add(tokens[i]);
                }
                //给自定义key赋值 以进行 对id+time的二次排序
                definedKey.setDeviceId(tokens[0]);
                definedKey.setTimeStr(tokens[1]);
            }
            context.write(definedKey,new Text(svb.toString()));
        } catch (Exception e) {
            e.printStackTrace();
            this.logger.error("处理SDCLOG出现异常，数据:" + value + e);
        }
    }

}
