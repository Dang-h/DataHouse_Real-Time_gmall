package com.atguigu.gmall2019.dw.publisher.service;

import org.springframework.stereotype.Service;

import java.util.Map;


public interface PublisherService {

    //获取Dau总数
    public Long getDauTotal(String date);

    //分时统计
    public Map<String, Long> getDauHourCount(String date);
}
