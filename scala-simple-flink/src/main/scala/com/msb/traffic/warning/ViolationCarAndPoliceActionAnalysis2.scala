package com.msb.traffic.warning

/**
 * @description: 违法车辆和交警出警分析
 *               * 第一种，当前的违法车辆（在5分钟内）如果已经出警了。（最后输出道主流中做删除处理）。
 *               * 第二种，当前违法车辆（在5分钟后）交警没有出警（发出出警的提示，在侧流中发出）。
 *               * 第三种，有交警的出警记录，但是不是由监控平台报的警。
 *               * 需要两种数据流：
 *               * 1、系统的实时违法车辆的数据流
 *               * 2、交警实时出警记录数据.
 * @author: zhaotf
 * @create: 2020-07-12 20:18
 */
object ViolationCarAndPoliceActionAnalysis2 {

}
