package mackchao

/**
  * https://blog.csdn.net/oMaFei/article/details/113186486
  *  @description:
  * @author Mackchao.Sun
  * @date:2022/10/28
  *       实现目标： 从目标csv中读取模拟登录的数据，实时检测，如果5秒钟之内连续登录的次数超过2次，则马上告警
  * */
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * 定义一个输入数据的样例类
  *
  * @param userId   用户id
  * @param ip       客户端的ip
  * @param loginState    登录状态，目前只有success/fail，后期可以做扩展，所以定义为string
  * @param ts       事件的时间戳，单位秒
  */
case class userLogin(userId: Long,ip: String,loginState: String,ts: Long)

/**
  * 定义一个输出的样例类
  * @param userId   用户id
  * @param startTs   开始登录时间
  * @param endTs     触发事件的最后一次时间
  * @param loginCount   时间段内总共登录的次数
                  1234,10.0.1.1,fail,1611373940
                  1235,10.0.1.2,fail,1611373941
                  1234,10.0.1.3,fail,1611373942
                  1234,10.0.1.3,success,1611373943
                  1234,10.0.1.3,fail,1611373943
                  1234,10.0.1.3,fail,1611373944
                  1236,10.0.1.4,fail,1611373945
                  1234,10.0.1.4,fail,1611373957
                  1234,10.0.1.5,fail,1611373958
                  1234,10.0.11.55,fail,1611373959
                  1236,2.2.2.2,fail,1611373960
  */
case class userLoginWarning(userId: Long, startTs: Long, endTs:Long, loginCount: Long)

object maliceLoginDetect {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //指定事件时间为窗口和watermark的时间
    env.setParallelism(1)

    //从文件中读取数据
    val resource = getClass.getResource("/userLogin.csv")
    val inputStream = env.readTextFile(resource.getPath)

    // 转换成样例类，并提取时间戳watermark
    val loginEventStream = inputStream
      .map(d => {
        val arr = d.split(",")
        // 分别对应  userId        ip      登录状态  时间戳
        userLogin(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignAscendingTimestamps(_.ts * 1000L) //把秒转为毫秒

    val loginWarningStream = loginEventStream
      .keyBy(_.userId)
      .process(new loginMaliceDetect(2))

    loginWarningStream.print()
    env.execute()
  }
}

class loginMaliceDetect(warningCount: Long) extends KeyedProcessFunction[Long,userLogin,userLoginWarning]{

  //定义状态，保存当前所有的登录事件为list，方便后边做数据统计
  lazy val loginFailListState: ListState[userLogin] = getRuntimeContext.getListState(new ListStateDescriptor[userLogin]("loginFail-list", classOf[userLogin]))

  //定义定时器的时间戳状态，否则没法删定时器

  lazy val  timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timerState", classOf[Long]))

  override def processElement(i: userLogin, context: KeyedProcessFunction[Long, userLogin, userLoginWarning]#Context, collector: Collector[userLoginWarning]): Unit = {
    //判断，如果当前事件是登录失败事件，那再继续操作
    if(i.loginState == "fail"){
      loginFailListState.add(i)
      //如果没有注册定时器，那就注册一个定时器，5秒之后触发
      if(timerTsState.value()== 0){
        val timerTs = i.ts * 1000L + 5000L
        context.timerService().registerEventTimeTimer(timerTs)
        timerTsState.update(timerTs)
      }
    }
    else if(i.loginState == "success"){
      context.timerService().deleteEventTimeTimer(timerTsState.value())
      timerTsState.clear()
      loginFailListState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, userLogin, userLoginWarning]#OnTimerContext, out: Collector[userLoginWarning]): Unit = {
    // 判断下如果登录失败次数超过了设置的阈值，则告警
    val loginFailList: ListBuffer[userLogin] = new ListBuffer[userLogin]
    val iterable = loginFailListState.get().iterator()
    while (iterable.hasNext){
      loginFailList += iterable.next()
    }
    if (loginFailList.size > warningCount){
      out.collect(userLoginWarning(userId = ctx.getCurrentKey, startTs = loginFailList.head.ts, endTs = loginFailList.last.ts, loginCount = loginFailList.size))
    }
    loginFailList.clear()
    loginFailListState.clear()
    timerTsState.clear()
  }
}
//上面代码栗子是可以实现基本的登录异常检测了，但是如果碰到数据乱序等情况，
//有3个失败事件在时间范围内，但是有个乱序的数据插在中间，这时候按照逻辑中间就会情况重新计算。。这时候就需要用到flink提供的cep(复杂事件检测)的功能了