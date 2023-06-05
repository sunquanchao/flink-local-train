package mackchao

/**
  * @description:
  * @author Mackchao.Sun
  * @date:2022/10/28
  * */
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import java.util

object maliceLoginDetectWithCep {
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

    // 1、先定义匹配的模式，需求为一个登录失败事件后，紧接着出现另一个失败事件
    val loginFailPattern = Pattern
      .begin[userLogin]("firstFail")
      .where(_.loginState == "fail")
      .next("secondFail")
      .where(_.loginState == "fail")
      .within(Time.seconds(5))

    //2、将匹配的规则应用在数据流中，得到一个PatternStream
    val patternStream = CEP.pattern(loginEventStream.keyBy(_.userId), loginFailPattern)

    // 3、匹配中符合模式要求的数据流，需要调用select
    val loginFailWarningStream = patternStream.select(new LoginFailEventMatch())
    loginFailWarningStream.print()
    env.execute("login fail detect with cep")

  }
}
class LoginFailEventMatch() extends PatternSelectFunction[userLogin,userLoginWarning]{
  override def select(map: util.Map[String, util.List[userLogin]]): userLoginWarning = {

    //前边定义的所有pattern，都在Map里头,因为map的value里面只定义了一个事件，所以只会有一条，取第一个就可以，如果定义了多个，需要按实际情况来
    val firstFailEvent = map.get("firstFail").get(0)
    val secondFailEvent = map.get("secondFail").iterator().next()
    userLoginWarning(firstFailEvent.userId,firstFailEvent.ts,secondFailEvent.ts,2)
  }
}
