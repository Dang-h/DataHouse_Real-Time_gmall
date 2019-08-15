package canal.client;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.List;

public class CanalHandler {

    String tableName;
    CanalEntry.EventType eventType;
    List<CanalEntry.RowData> rowDataList;

    public CanalHandler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDataList) {
        this.tableName = tableName;
        this.eventType = eventType;
        this.rowDataList = rowDataList;
    }


    //发送数据到kafka

}
