package com.msb.stream.cep

/**
 * @description: CEP实现订单监控.
 * @author: zhaotf
 * @create: 2020-06-21 17:32
 */
object OrderMonitorByStateLocal {

}

/**
 * 订单信息类.
 * @param oid 订单编号
 * @param status 状态
 * @param payId 支付编号
 * @param actionTime 操作时间
 */
case class OrderInfo(oid:String,status:String,payId:String,actionTime:Long)

/**
 * 提示信息的样例类.
 * @param oid 订单编号
 * @param msg 消息内容
 * @param createTime 操作时间
 * @param payTime 支付时间
 */
case class OrderMessage(oid:String,msg:String,createTime:Long,payTime:Long)
