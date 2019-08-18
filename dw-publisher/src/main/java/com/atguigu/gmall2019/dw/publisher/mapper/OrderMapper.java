package com.atguigu.gmall2019.dw.publisher.mapper;

import com.atguigu.gmall2019.dw.publisher.bean.OrderHourAmount;

import java.util.List;

public interface OrderMapper {

    //获取订单金额
    public Double getOrderAmount(String date);

    //获取分时统计金额
    public List<OrderHourAmount> getOrderHourAmount(String date);
}
