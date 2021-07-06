package com.wei.sink.Entity;

/**
 * Es sink 自定义上下文环境
 */
public class EsSinkContext {

    //事务编号
    private long transactionCount = 0;
    //epoch编号
    private long epochCount = 0;

    //生成事务ID
    public String genAndIncreaseTransactionId(int parallelId){
        String epoch = "epoch-";
        String transactionId = "-transaction-";
        String lastValue = epoch + epochCount + transactionId + transactionCount;
        if (transactionCount == Long.MAX_VALUE){
            transactionCount = 0;
            epochCount++;
        }else {
            transactionCount++;
        }
        return "parallel-" + parallelId + lastValue;
    }

    @Override
    public String toString(){
        return "EsSinkContext{"
                + "semaphore="
                + ", transactionCount="
                + transactionCount
                + ", epochCount="
                + epochCount
                + '}';
    }
}
