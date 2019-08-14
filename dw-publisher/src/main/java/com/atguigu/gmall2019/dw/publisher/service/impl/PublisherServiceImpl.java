package com.atguigu.gmall2019.dw.publisher.service.impl;

import com.atguigu.gmall2019.dw.publisher.mapper.DauMapper;
import com.atguigu.gmall2019.dw.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {


    DauMapper dauMapper;

    @Override
    public Long getDauTotal(String date) {
        return dauMapper.getDauTotal(date);
    }

    @Override
    public Map<String, Long> getDayHourCount(String date) {
        return null;
    }
}

//编写一个日期-1的方法
