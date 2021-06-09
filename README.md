# Data_com v1.0
# 基于flink的电商用户行为分析
## 项目模块
    主要分为两个模块：
    
## 代码结构
   1. feature-算子
   2. pojo-数据结构
   3. sink-数据sink
   4. source-数据源
   5. util-工具
## 数据源：
    -1. kafka-支持多流，通过设置多个topic，默认按传入顺序构建source。通过topic获取对应source；
## 算子：
   ### -实时统计分析
      -1.实时热门商品统计-getHotItem ProcessFunction+sql两种实现
      -2.实时热门页面流量统计-getHotPage 
      -3.实时访问流量统计-uvCount 实现了简易版（无去重）、set去重、BloomFliter、HyperLogLog
      -4.APP市场推广统计 -appMarketing 全流量+分渠道
      -5.页面广告点击量统计 -adStatisticsByProvince 
   ### -业务流程以及风险控制
      -1.恶意登录监控 -loginFail 自定义ProcessFunction+CEP
      -2.订单支付实时监控 -orderPayTimeout 自定义ProcessFunction+CEP
      -3.订单实时支付对账 -txPayMatch connect、join
## sink：
      -1.
## 所需环境
    -kafka，flink，redis，es
