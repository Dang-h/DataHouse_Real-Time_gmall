package com.atguigu.gmall2019.dw.publisher.mapper;

import java.util.List;
import java.util.Map;

//MyBatis查询数据库映射成Java对象；通过SQL定义文件自动实现接口
//接口，只定义方法
public interface DauMapper {

    //获取数据库数据
    public Long getDauTotal(String date);

    public List<Map> getDauHourCount(String date);
}
