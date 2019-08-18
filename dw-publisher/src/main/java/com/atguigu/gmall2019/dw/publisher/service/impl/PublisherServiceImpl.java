package com.atguigu.gmall2019.dw.publisher.service.impl;

import com.atguigu.gmall2019.dw.publisher.bean.OrderHourAmount;
import com.atguigu.gmall2019.dw.publisher.mapper.DauMapper;
import com.atguigu.gmall2019.dw.publisher.mapper.OrderMapper;
import com.atguigu.gmall2019.dw.publisher.service.PublisherService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

//业务层，处理业务逻辑
//publisherService的实现类
//Service：定义为容器的组件
@Service
public class PublisherServiceImpl implements PublisherService {

    //自动注入，直接使用DauMapper
    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Override
    public Long getDauTotal(String date) {

        return dauMapper.getDauTotal(date);
    }

    //日活
    @Override
    public Map<String, Long> getDauHourCount(String date) {

        //得到数据
        List<Map> dauHourCountList = dauMapper.getDauHourCount(date);

        //用一个Map来装数据
        HashMap<String, Long> hourMap = new HashMap<>();

        //把数据装入Map
        for (Map map : dauHourCountList) {
            hourMap.put(((String) map.get("LOGHOUR")), ((Long) map.get("CNT")));
        }

        return hourMap;
    }

    //交易额
    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.getOrderAmount(date);
    }

    @Override
    public Map<String, Double> getOrderHourAmount(String date) {

        //把list转换成map
        HashMap<String, Double> hourAmountMap = new HashMap<>();
        List<OrderHourAmount> orderHourAmount = orderMapper.getOrderHourAmount(date);

        //怼入数据
        for (OrderHourAmount hourAmount : orderHourAmount) {
            hourAmountMap.put(hourAmount.getCreateHour(), hourAmount.getSumOrderAmount());
        }
        return hourAmountMap;
    }
}