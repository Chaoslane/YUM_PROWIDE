package com.udbac.hadoop.etl.mr;

import com.udbac.hadoop.common.DefinedKey;
import com.udbac.hadoop.common.LogConstants;
import com.udbac.hadoop.entity.WideTable;
import com.udbac.hadoop.etl.util.IPSeekerExt;
import com.udbac.hadoop.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by chaoslane@126.com on 2016/7/25.
 * 根据有序的时间 进行session重建 把30分钟内的行为 合并为一个访次
 */
public class LogAnalyserReducer extends Reducer<DefinedKey, Text, NullWritable, Text> {
    private final Logger logger = Logger.getLogger(LogAnalyserReducer.class);

    @Override
    protected void reduce(DefinedKey key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        try {
            Map<String, WideTable> anVisit = getOneVisitMap(key,values);
            for (Map.Entry<String, WideTable> entry : anVisit.entrySet()) {
                 context.write(NullWritable.get(), new Text(entry.getKey() + "\t" + entry.getValue().toString()));
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("session重建出现异常");
        }
    }

    /**
     * session重建的方法
     * @param values 即一根据 用户ID 分组的 value list
     * @return 一个或多个访次的map集合
     */
    private static Map<String, WideTable> getOneVisitMap(DefinedKey key, Iterable<Text> values) {
        long cur,tmp,last = 0 ;
        BigDecimal duration = null;
        WideTable wideTable = null;
        //每一个 Map.Entry KEY为生成的javaUUID VALUE为多个行为合并之后的一个访次的信息
        Map<String, WideTable> oneVisit = new HashMap<>();
        for (Text text : values) {
            String log = text.toString();
            String[] logSplits = log.split("\\t");
            cur = TimeUtil.parseStringDate2Long(key.getTimeStr());

            //小于30分钟的 则进行时长叠加 && routeevent的合并（有为1，无为0）
            if (cur - last < LogConstants.HALFHOUR_OF_MILLISECONDS) {
                tmp = cur - last;
                duration = duration.add(BigDecimal.valueOf(tmp));
                wideTable.setDuration(duration);
                wideTable.setWt_login(wideTable.getWt_login() | Integer.valueOf(logSplits[3]));
                wideTable.setWt_menu(wideTable.getWt_menu() | Integer.valueOf(logSplits[4]));
                wideTable.setWt_user(wideTable.getWt_user() | Integer.valueOf(logSplits[5]));
                wideTable.setWt_cart(wideTable.getWt_cart() | Integer.valueOf(logSplits[6]));
                wideTable.setWt_suc(wideTable.getWt_suc() | Integer.valueOf(logSplits[7]));
                wideTable.setWt_pay(wideTable.getWt_pay() | Integer.valueOf(logSplits[8]));
            } else {
                //大于30分钟的 则new一个新的对象，并放入到访次集合中
                duration = BigDecimal.ZERO;
                wideTable = WideTable.parse(key.toString()+"\t"+log);
                handleTab(wideTable);
                oneVisit.put(UUID.randomUUID().toString().replace("-", ""), wideTable);
            }
            last = cur;
        }
        return oneVisit;
    }

    //操作WideTable 进行一些解析的操作
    private static void handleTab(WideTable wideTable) {
        //解析domain
        String domain = wideTable.getUser_domain();
        if (StringUtils.isNotBlank(domain)) {
            wideTable.setUser_domain(LogConstants.UserDomain.getDomainType(domain));
        }
    }

}
