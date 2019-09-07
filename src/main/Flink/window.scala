import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object window {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.socketTextStream("localhost", 11111)
    val streamKeyby = stream.map((_, 1)).keyBy(0)
    //窗口类型1：countWindow:滚动、滑动
    //countWindow 滚动窗口:单个key的数量达到count才会触发CountWindow的执行
    //    val streamWindow = streamKeyby.countWindow(3).reduce(
    //        (v1,v2)=>(v1._1,v1._2+v2._2)
    //      )

    //countWindow 滑动窗口:接收到的数据达到滑动步长，执行一次
    //    val streamWindow = streamKeyby.countWindow(3, 2).reduce(
    //      (v1, v2) => (v1._1, v1._2 + v2._2)
    //    )

    //窗口类型2：timeWindow:滚动、滑动、会话
    //timeWindow 滚动窗口:每次达到时间间隔执行一次
    //    val streamWindow= streamKeyby.timeWindow(Time.seconds(5)).reduce(
    //      (v1, v2) => (v1._1, v1._2 + v2._2)
    //    )

    //timeWindow 滑动窗口:每次达到滑动步长时间执行一次，每次内容长度为窗口时间长度
        val streamWindow= streamKeyby.timeWindow(Time.seconds(5),Time.seconds(2)).reduce(
          (v1, v2) => (v1._1, v1._2 + v2._2)
        )



    streamWindow.print()
    env.execute("windowJob")
  }
}
