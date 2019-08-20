package com.atguigu.gmall2019.dw.publisher.controller;


import com.alibaba.fastjson.JSON;
import com.atguigu.gmall2019.dw.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

//控制层，对外发布外界接口；实现Web接口
@RestController
public class PubliherController {

    //controller调用service
    @Autowired
    PublisherService publisherService;

    //查询使用GetMapping，路径为：realtime-total
    //读入传入参数date
    @GetMapping("realtime-total")
    public String getTotal(@RequestParam("date") String date){
        //传入数据
        Long dauTotal = publisherService.getDauTotal(date);

        //List中装有一个个Map，Map里存有键值对
        List<Map> totalList = new ArrayList<>();

        //做一个Map
        //设计返回结果Json
        //{"id":"dau","name":"新增日活","value":1200}
        Map dauMap = new HashMap();
        dauMap.put("id","dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);
        totalList.add(dauMap);

        //"id":"new_mid","name":"新增设备","value":233}
        Map newMidMap = new HashMap();
        newMidMap.put("id","new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", 23333);
        totalList.add(newMidMap);

        //"id":"order_amount","name":"新增交易额","value":1000.2 }
        Map orderAmountMap = new HashMap();
        orderAmountMap.put("id", "order_amount");
        orderAmountMap.put("name", "新增交易额");
        Double orderAmount = publisherService.getOrderAmount(date);
        orderAmountMap.put("value", orderAmount);
        totalList.add(orderAmountMap);

        //将Map转换成Json结果集返回
        return JSON.toJSONString(totalList);
    }

    //统计分时数据
    @GetMapping("realtime-hour")
    public String getRealtimeHour(@RequestParam("id") String id, @RequestParam("date") String tdate){

        if ("dau".equals(id)){
            //获取今天分时数据
            Map<String, Long> dauHourCountTodayMap = publisherService.getDauHourCount(tdate);

            //获取昨天时间
            String ydate = getYesterdayString(tdate);

            //获取昨天分时数据
            Map<String, Long> dauHourCountYDayMap = publisherService.getDauHourCount(ydate);

            Map dauMap = new HashMap();

            dauMap.put("today", dauHourCountTodayMap);
            dauMap.put("yesterday", dauHourCountYDayMap);


            //转换成Json返回
            return JSON.toJSONString(dauMap);
        } else if ("order_amount".equals(id)){
            Map<String, Double> orderHourAmountTodayMap = publisherService.getOrderHourAmount(tdate);

            String ydate = getYesterdayString(tdate);
            Map<String, Double> orderHourAmountYDayMap = publisherService.getOrderHourAmount(ydate);
            Map orderAmountMap = new HashMap();

            orderAmountMap.put("today", orderHourAmountTodayMap);
            orderAmountMap.put("yesterday", orderHourAmountYDayMap);

            return JSON.toJSONString(orderAmountMap);
        }

        return null;
    }

    /**
     * 获取昨天日期
     * @param todayString
     * @return
     */
    private  String getYesterdayString(String todayString){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

        String yesterdayString = null;
        try {
            Date today = dateFormat.parse(todayString);
            Date yesterday = DateUtils.addDays(today, -1);
            yesterdayString = dateFormat.format(yesterday);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return yesterdayString;
    }
}

