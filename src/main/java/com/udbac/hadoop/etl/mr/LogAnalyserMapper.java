package com.udbac.hadoop.etl.mr;

import com.udbac.hadoop.common.*;
import com.udbac.hadoop.util.SplitValueBuilder;
import com.udbac.hadoop.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
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
    private Map<DefinedKey, Text> mapoutput = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mapoutput.clear();
    }

    /**
     * 读入一行数据 根据分割符切割后 为自定义key赋值
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String[] lineSplits = StringUtils.split(value.toString(), LogConstants.SEPARTIOR_TAB);
            if (lineSplits.length != 11) {
                return;
            }
            //切割字符串 获取 date_time 进行+8的操作
            lineSplits[1] = TimeUtil.handleTime(lineSplits[1]);

            DefinedKey definedKey = new DefinedKey();
            SplitValueBuilder svb = new SplitValueBuilder();

            for (int i = 2; i < lineSplits.length; i++) {
                svb.add(lineSplits[i]);
            }
            //给自定义key赋值 以进行 对id+time的二次排序
            definedKey.setDeviceId(lineSplits[0]);
            definedKey.setTimeStr(lineSplits[1]);
            context.write(definedKey, new Text(svb.toString()));
        } catch (Exception e) {
            e.printStackTrace();
            this.logger.error("处理SDCLOG出现异常，数据:" + value + e);
        }
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "local");
            String inputArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (inputArgs.length != 2) {
                System.err.println("\"Usage:<inputPath> <outputPath>/n\"");
                System.exit(1);
            }
            String inputPath = inputArgs[0];
            String outputPath = inputArgs[1];

            Job job1 = Job.getInstance(conf, "SessionRebuildMR");
            TextInputFormat.addInputPath(job1, new Path(inputPath));
            TextOutputFormat.setOutputPath(job1, new Path(outputPath));

            job1.setJarByClass(LogAnalyserMapper.class);
            job1.setMapperClass(LogAnalyserMapper.class);
            job1.setReducerClass(LogAnalyserReducer.class);

            job1.setMapOutputKeyClass(DefinedKey.class);
            job1.setMapOutputValueClass(Text.class);
            job1.setSortComparatorClass(DefinedComparator.class);
            job1.setGroupingComparatorClass(DefinedGroupSort.class);
            job1.setPartitionerClass(DefinedPartition.class);
            job1.setNumReduceTasks(1);
            if (job1.waitForCompletion(true)) {
                System.out.println("-----SessionRebuildMR job succeed-----");
            }
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException("-----SessionRebuildMR job failed!!!-----");
        }
    }
}
