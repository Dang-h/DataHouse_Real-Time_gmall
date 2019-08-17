package com.atguigu.amall.dw.dwlogger.controller;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import constant.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

//对接外部请求。JSONMocker发送请求，http://logserver/log?logString={xxxxx}
@Slf4j
@RestController //Controller+responsebody
public class LoggerController {

    //声明kafka接口
    //自动实现接口方法
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    //处理请求/log?
    @PostMapping("log")
    public String doLog(@RequestParam("logString") String logString) {

        // 0 补充时间戳
        //转换成JSON对象
        JSONObject jsonObject = JSON.parseObject(logString);
        //补充时间戳ts
        jsonObject.put("ts", System.currentTimeMillis());

        // 1 写日志 （用于离线数据采集）
        //TODO 补充配置文件log4j.properties 到resources
        String jsonString = jsonObject.toJSONString();
        //通过注解@Slf4j自动生成声明
        //info：日志级别，对应log4j
        log.info(jsonObject.toJSONString());

        // 2 推送到kafka
        //“startup”写前可防止空指针
        if ("startup".equals(jsonObject.getString("type"))) {
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP, jsonString);
        } else {
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT, jsonString);
        }

        return "success";
    }

}

