package com.atguigu.gmall2019.dw.publisher.service;

import org.springframework.stereotype.Service;

import java.util.Map;


public interface PublisherService {

    //获取Dau总数
    public Long getDauTotal(String date);
    //分时统计
    public Map<String, Long> getDauHourCount(String date);

    //获取交易总额
    public Double getOrderAmount(String date);
    //交易额分时统计
    public Map<String, Double> getOrderHourAmount(String date);
}
