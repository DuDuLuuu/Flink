import org.apache.flink.streaming.api.scala._

/**
  * Flink、的读写数据操作
  *
  */
object ResourceAndSink {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //获取数据
    //    方式一：文件读取
    //    val stream=env.readTextFile("in/1.txt")
    //    方式二：socket读取
    //    val stream= env.socketTextStream("localhost", 11111)
    //方式三：内存创建数据
    val list = List(1, 2, 3, 4)
    val iter = Iterator(5, 6, 7, 8)
    //    val stream= env.fromCollection(list)
    //    val stream= env.fromCollection(iter)
    val stream = env.generateSequence(1, 5)
    //打印数据
    stream.print()



    //    写数据
    //    1、writeAsText
    //    2、writeAsCvs
    //    3、print/printToErr
    //    4、writeUsingOutputFormat
    //    5、writetoSocket
    //执行任务
    env.execute("testJob")

  }
}
