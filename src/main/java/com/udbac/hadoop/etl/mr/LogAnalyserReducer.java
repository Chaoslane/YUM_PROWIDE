package com.udbac.hadoop.etl.mr;

import com.udbac.hadoop.common.DefinedKey;
import com.udbac.hadoop.common.LogConstants;
import com.udbac.hadoop.entity.WideTable;
import com.udbac.hadoop.etl.util.IPSeekerExt;
import com.udbac.hadoop.util.SplitValueBuilder;
import com.udbac.hadoop.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by chaoslane@126.com on 2016/7/25.
 * 根据有序的时间 进行session重建 把30分钟内的行为 合并为一个访次
 */
public class LogAnalyserReducer extends Reducer<DefinedKey, Text, NullWritable, Text> {
    private final Logger logger = Logger.getLogger(LogAnalyserReducer.class);
    private static IPSeekerExt ipSeekerExt = new IPSeekerExt();

    @Override
    protected void reduce(DefinedKey key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        try {
            Map<String, WideTable> anVisit = getOneVisitMap(values);
            for (Map.Entry<String, WideTable> entry : anVisit.entrySet()) {
                 context.write(NullWritable.get(), new Text(entry.getKey() + "|" + entry.getValue().toString()));
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
    private static Map<String, WideTable> getOneVisitMap(Iterable<Text> values) {
        long cur,tmp,last = 0 ;
        BigDecimal duration = null;
        WideTable wideTable = null;
        //每一个 Map.Entry KEY为生成的javaUUID VALUE为多个行为合并之后的一个访次的信息
        Map<String, WideTable> oneVisit = new HashMap<>();
        for (Text text : values) {
            String log = text.toString();
            String[] logSplits = log.split("\\|");
            cur = TimeUtil.parseStringDate2Long(logSplits[1]);

            //小于30分钟的 则进行时长叠加 && routeevent的合并（有为1，无为0）
            if (cur - last < LogConstants.HALFHOUR_OF_MILLISECONDS) {
                tmp = cur - last;
                duration = duration.add(BigDecimal.valueOf(tmp));
                wideTable.setDuration(duration);
                wideTable.setWt_login(wideTable.getWt_login() | Integer.valueOf(logSplits[6]));
                wideTable.setWt_menu(wideTable.getWt_menu() | Integer.valueOf(logSplits[7]));
                wideTable.setWt_user(wideTable.getWt_user() | Integer.valueOf(logSplits[8]));
                wideTable.setWt_cart(wideTable.getWt_cart() | Integer.valueOf(logSplits[9]));
                wideTable.setWt_suc(wideTable.getWt_suc() | Integer.valueOf(logSplits[10]));
                wideTable.setWt_pay(wideTable.getWt_pay() | Integer.valueOf(logSplits[11]));
            //大于30分钟的 则new一个新的对象，并放入到访次集合中
            } else {
                duration = BigDecimal.ZERO;
                wideTable = WideTable.parse(log);
                handleTab(wideTable);
                oneVisit.put(UUID.randomUUID().toString().replace("-", ""), wideTable);
            }
            last = cur;
        }
        return oneVisit;
    }

    //操作WideTable 进行一些解析的操作
    private static void handleTab(WideTable wideTable) {
        //解析IP
        String uip = wideTable.getDcssip();
        if (StringUtils.isNotBlank(uip)) {
            IPSeekerExt.RegionInfo info = ipSeekerExt.analyticIp(uip);
            if (null != info) {
                wideTable.setDcssip(info.getCountry() + "," + info.getProvince() + "," + info.getCity());
            }
        }
        //解析domain
        String domain = wideTable.getUser_domain();
        if (StringUtils.isNotBlank(domain)) {
            wideTable.setUser_domain(LogConstants.UserDomain.getDomainType(domain));
        }
    }

//    private static void handleUserAgent(AnalysedLog analysedLog) {
//        String csUserAgent = analysedLog.getCsUserAgent();
//        if (StringUtils.isNotBlank(csUserAgent)) {
//            UserAgentUtil.UserAgentInfo info = UserAgentUtil.analyticUserAgent(csUserAgent);
//            if (info != null) {
//                analysedLog.setCsUserAgent(info.toString());
//            }
//        }
//    }
}
