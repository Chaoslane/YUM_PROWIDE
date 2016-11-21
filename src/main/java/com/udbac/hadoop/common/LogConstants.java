package com.udbac.hadoop.common;

/**
 * Created by chaoslane@126.com on 2016/7/25.
 * 常量类
 */
public class LogConstants {
    /**
     * 默认值
     */
    public static final String DEFAULT_VALUE = "unknown";
    /**
     * 一天的毫秒数
     */
    public static final int DAY_OF_MILLISECONDS = 86400000;
    /**
     * 半小时的毫秒数
     */
    public static final int HALFHOUR_OF_MILLISECONDS = 1800000;
    /**
     * 定义的运行日期
     */
    public static final String RUNNING_DATE_PARAMES = "RUNNING_DATE";
    /**
     * 日志分隔符
     */
    public static final String LOG_SEPARTIOR = " ";

    /**
     * 枚举类
     * 从宽表中获取的用户类型 为domain域名 转换为alias 方便存入表中
     */
    public enum UserDomain {
        OUTTAKE("m.4008823823.com.cn", "takeout"),
        SELFTAKE("order.kfc.com.cn", "self"),
        MEMBERTAKE("mall.kfc.com.cn", "member");

        public String domainName;
        public String alias;

        UserDomain(String domainName, String alias) {
            this.domainName = domainName;
            this.alias = alias;
        }

        //遍历枚举值
        public static String getDomainType(String domainName) {
            for (UserDomain userDomain : values()) {
                domainName.trim();
                if (userDomain.getDomainName().equals(domainName)) {
                    return userDomain.getAlias();
                }
            }
            return DEFAULT_VALUE;
        }
        public String getDomainName() {
            return domainName;
        }
        public String getAlias() {
            return alias;
        }
    }

    /**
     * 枚举类型 业务4 点击事件
     */
    public enum ClickEvent {
        BANNER1("1","banner1"),
        FIRSTPAGESEND("2","firstsend"),
        SELFTAKEBUTTON("3","selfbutton"),
        MEMBERBUTTON("4","membutton"),
        HOMEPAGESEND("5","homesend");

        public String eventURI;
        public String alias;

        ClickEvent(String eventURI, String alias) {
            this.eventURI = eventURI;
            this.alias = alias;
        }

        public String getClickEventType(String eventURI) {
            for (ClickEvent clickEvent : values()) {
                if (clickEvent.getEventURI().equals(eventURI)) {
                    return clickEvent.getAlias();
                }
            }
            return DEFAULT_VALUE;
        }

        public String getEventURI() {
            return eventURI;
        }
        public String getAlias() {
            return alias;
        }
    }
}
