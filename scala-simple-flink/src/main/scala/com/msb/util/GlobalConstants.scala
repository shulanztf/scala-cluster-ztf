package com.msb.util

/**
 * 从Kafka中读取的数据，车辆经过卡口的信息.
 * actionTime 监控时间
 * monitorId 卡口id
 * cameraId cameraId
 * car 车牌号
 * speed 车速
 * roadId roadId
 * areaId areaId
 */
case class TrafficInfo(actionTime: Long, monitorId: String, cameraId: String, car: String, speed: Double, roadId: String, areaId: String)

/**
 * 某个时间范围内卡口的平均车速和通过的车辆数量.
 * start 开始时间
 * end 结束时间
 * monitorId 卡口id
 * avgSpeed 平均车速
 * carCount 车辆总数
 */
case class AvgSpeedInfo(start: Long, end: Long, monitorId: String, avgSpeed: Double, carCount: Int)

/**
 * 车辆超速的信息.
 * car 车牌号
 * monitorId 卡口id
 * roadId roadId
 * realSpeed realSpeed
 * limitSpeed 最小车速
 * actionTime 动作时间
 */
case class OutOfLimitSpeedInfo(car: String, monitorId: String, roadId: String, realSpeed: Double, limitSpeed: Int, actionTime: Long)

/**
 * 套牌车辆告警信息对象.
 * car 车牌号
 * firstMonitor 第一个卡口id
 * secondMonitor 第二个卡口id
 * msg 提示信息
 * actionTime 动作时间
 */
case class RepetitionCarWarning(car: String, firstMonitor: String, secondMonitor: String, msg: String, actionTime: Long)

/**
 * @description: TODO.
 * @author: zhaotf
 * @create: 2020-07-05 19:38
 */
object GlobalConstants {

}
