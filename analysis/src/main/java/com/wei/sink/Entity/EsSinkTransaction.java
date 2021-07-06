package com.wei.sink.Entity;

import com.wei.pojo.LoginFailWarning;
import com.wei.pojo.OrderEvent;
import com.wei.pojo.OrderResult;
import com.wei.pojo.ReceiptEvent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import scala.Tuple2;

/**
 * 待提交事务对象
 */
@Data
@NoArgsConstructor
public class EsSinkTransaction {

    //待提交缓存
    private final Map<String,Tuple2<OrderEvent, ReceiptEvent>> commitBuffer = new LinkedHashMap<>();

    //当前事务缓存中待提交数据条数
    private int dataCount = 0 ;

    //缓存中是否有数据等待提交
    private boolean bufferIsEmpty = false;

    //当前事务ID
    private String transactionId;

    //信号量锁
    private boolean hasLock;

    public void addDataToCommitBuffer(Tuple2<OrderEvent, ReceiptEvent> data,int parallelId){
        //这里可以加上去重逻辑
        String esDeduplicateId = StringUtils.deleteWhitespace(
                String.valueOf(parallelId) + "_" + System.currentTimeMillis() + "_" + dataCount)
                .replace(":", "").replace("/", "|");
        commitBuffer.put(esDeduplicateId,data);
        dataCount++;
    }
}
