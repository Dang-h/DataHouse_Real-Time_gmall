package com.atguigu.gmall2019.dw.publisher.service;

import org.springframework.stereotype.Service;

import java.util.Map;


public interface PublisherService {
    public Long getDauTotal(String date);


    public Map<String, Long> getDayHourCount(String date);
}
