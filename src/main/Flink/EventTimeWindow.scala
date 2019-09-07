import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object EventTimeWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("localhost", 11111).assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[String](Time.milliseconds(0)) {
        override def extractTimestamp(t: String): Long = {
          val eventTime = t.split(" ")(0).toLong
          println(eventTime)
          eventTime
        }
      }
    ).map(item => (item.split(" ")(1), 1)).keyBy(0)
    //滚动窗口
    //    val streamWindow = stream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
    //滑动窗口
    //    val streamWindow = stream.window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
    //会话窗口:间隔时长超过执行时间，触发一次执行
    val streamWindow = stream.window(EventTimeSessionWindows.withGap(Time.seconds(5)))

    val streamReduce = streamWindow.reduce(
      (v1, v2) => (v1._1, v1._2 + v2._2)
    )
    streamReduce.print()


    env.execute("EventTimeJob")
  }

}
