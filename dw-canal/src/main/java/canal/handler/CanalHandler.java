package canal.handler;

import canal.utils.myKafkaSender;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import constant.GmallConstants;

import java.util.List;

//业务的具体处理
public class CanalHandler {

    //定义具体参数
    //表名
    String tableName;
    //操作类型
    CanalEntry.EventType eventType;
    //数据列表
    List<CanalEntry.RowData> rowDataList;


    public CanalHandler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDataList) {
        this.tableName = tableName;
        this.eventType = eventType;
        this.rowDataList = rowDataList;
    }


    public void handle() {
        //根据业务类型分类处理
        //将不同的表的变化切分成不同的主题保存到对应的topic里
        if (tableName.equals("order_info") && eventType.equals(CanalEntry.EventType.INSERT)) {
            //处理数据
            for (CanalEntry.RowData rowData : rowDataList) {
                //发送数据
                sendKafka(rowData, GmallConstants.KAFKA_TOPIC_NEW_ORDER);
            }
        }
    }

    /**
     * 发送数据到Kafka对应的Topic中
     * @param rowData
     * @param topic
     */
    private void sendKafka(CanalEntry.RowData rowData, String topic) {

        List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();

        JSONObject jsonObject = new JSONObject();

        for (CanalEntry.Column column : afterColumnsList) {
            System.out.println(column.getName() + " ---> " + column.getValue());

            //把每一个字段变成一个JSON字符串,并发送至Kafka对应的topic中
            jsonObject.put(column.getName(), column.getValue());
        }
        String rowJSON = jsonObject.toJSONString();
        myKafkaSender.send(topic, rowJSON);
    }
}
