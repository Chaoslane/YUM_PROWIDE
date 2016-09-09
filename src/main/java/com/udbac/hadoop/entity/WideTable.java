package com.udbac.hadoop.entity;

import com.udbac.hadoop.util.SplitValueBuilder;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by chaoslane@126.com on 2016/7/25.
 */
public class WideTable {
    private String deviceId;
    private String date_time;
    private String dcssip;
    private String brand_new_user;//新老公户
    private String user_domain;//用户类型
    private String spreadid;//app下载渠道
    //用户转化路径
    private String wt_login;
    private String wt_menu;
    private String wt_user;
    private String wt_cart;
    private String wt_suc;
    private String wt_pay;
    private String wt_event;
    private BigDecimal duration;

    /**
     * 把map输出到reduce的一行字符串 转化为 WideTable对象
     *
     * @param mapinput
     * @return WideTable
     */
    public static WideTable parse(String mapinput) {
        WideTable wideTable = null;
        String[] tokens = mapinput.split("\\|");
        if (tokens.length == 13) {
            wideTable = new WideTable();
            Field[] field = wideTable.getClass().getDeclaredFields();
            for (int i = 0; i < 13; i++) {
                try {
                    field[i].set(wideTable, tokens[i]);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
            return wideTable;
        }
        return null;
    }


    public String getDeviceId() {
        return deviceId;
    }

    public String getDate_time() {
        return date_time;
    }

    public void setDate_time(String date_time) {
        this.date_time = date_time;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getDcssip() {
        return dcssip;
    }

    public void setDcssip(String dcssip) {
        this.dcssip = dcssip;
    }

    public String getBrand_new_user() {
        return brand_new_user;
    }

    public void setBrand_new_user(String brand_new_user) {
        this.brand_new_user = brand_new_user;
    }

    public String getUser_domain() {
        return user_domain;
    }

    public void setUser_domain(String user_domain) {
        this.user_domain = user_domain;
    }

    public String getSpreadid() {
        return spreadid;
    }

    public void setSpreadid(String spreadid) {
        this.spreadid = spreadid;
    }

    public Integer getWt_login() {
        return Integer.valueOf(wt_login);
    }

    public void setWt_login(Integer wt_login) {
        this.wt_login = String.valueOf(wt_login);
    }

    public Integer getWt_menu() {
        return Integer.valueOf(wt_menu);
    }

    public void setWt_menu(Integer wt_menu) {
        this.wt_menu = String.valueOf(wt_menu);
    }

    public Integer getWt_user() {
        return Integer.valueOf(wt_user);
    }

    public void setWt_user(Integer wt_user) {
        this.wt_user = String.valueOf(wt_user);
    }

    public Integer getWt_cart() {
        return Integer.valueOf(wt_cart);
    }

    public void setWt_cart(Integer wt_cart) {
        this.wt_cart = String.valueOf(wt_cart);
    }

    public Integer getWt_suc() {
        return Integer.valueOf(wt_suc);
    }

    public void setWt_suc(Integer wt_suc) {
        this.wt_suc = String.valueOf(wt_suc);
    }

    public Integer getWt_pay() {
        return Integer.valueOf(wt_pay);
    }

    public void setWt_pay(Integer wt_pay) {
        this.wt_pay = String.valueOf(wt_pay);
    }

    public String getWt_event() {
        return wt_event;
    }

    public void setWt_event(String wt_event) {
        this.wt_event = wt_event;
    }

    public BigDecimal getDuration() {
        return duration;
    }

    public void setDuration(BigDecimal duration) {
        this.duration = duration;
    }

    @Override
    public String toString() {
        SplitValueBuilder svb = new SplitValueBuilder();
        svb.add(deviceId).add(date_time.split(" ")[0])
                .add(date_time.split(" ")[1])
                .add(dcssip)
                .add(brand_new_user)
                .add(user_domain)
                .add(spreadid)
                .add(wt_login)
                .add(wt_menu)
                .add(wt_user)
                .add(wt_cart)
                .add(wt_suc)
                .add(wt_pay)
                .add(wt_event)
                .add(duration);
        return svb.build();
    }
}
