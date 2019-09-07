
import org.apache.flink.streaming.api.scala._

object Transformation {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //Map
    //    val stream = env.generateSequence(1, 10)
    //    val streamMap = stream.map(item => item * 2)
    //    streamMap.print()

    //FlatMap
    //    val stream=env.readTextFile("in/1.txt")
    //    val streamFlat: DataStream[String] = stream.flatMap(item=>item.split(" "))
    //    streamFlat.print()

    //Filter
    //    val stream = env.generateSequence(1, 10)
    //    val streamFilter: DataStream[Long] = stream.filter(item => item == 1)
    //    streamFilter.print()

    //Connet:将两个stream合并成一个ConnectedStreams，但内部还是各自独立的，调用算子需要两个函数
    //    val stream=env.generateSequence(1,10)
    //    val streamMap=env.readTextFile("in/1.txt").flatMap(item=>item.split(" ")).filter(item=>item.equals("hello"))
    //    val streamConnect: ConnectedStreams[Long, String] = stream.connect(streamMap)
    //    val streamCoMap: DataStream[io.Serializable] = streamConnect.map(item=>item*2,item=>(item,1))
    //    streamCoMap.print()

    //CoMap/CoFlatMap:将ConnectedStreams内两个stream各自执行函数，返回1个DataStream

    //Split:将一个DataStream按需拆成多个集合放在SplitStream中并返回
    //Select：从SplitStream取出指定集合
    //    val stream: DataStream[String] = env.readTextFile("in/1.txt").flatMap(item => item.split(" "))
    //    val streamSplit: SplitStream[String] = stream.split(word => {
    //      ("hello".equals(word)) match {
    //        case true => List("hello")
    //        case false => List("ohter")
    //      }
    //    })
    //    val streamSelect1: DataStream[String] = streamSplit.select("hello")
    //    val streamSelect2: DataStream[String] = streamSplit.select("ohter")
    //
    //    //    streamSelect1.print()
    //    streamSelect2.print()

    //Union:将两个DataStream合并成1个DataStream
    //    val stream1: DataStream[String] = env.readTextFile("in/1.txt")
    //    val stream2: DataStream[String] = env.readTextFile("in/2.txt")
    //    val streamUnion: DataStream[String] = stream1.union(stream2)
    //    streamUnion.print()


    //KeyBy
    // Reduce
    //    val stream: DataStream[(String, Int)] = env.readTextFile("in/1.txt").flatMap(_.split(" ")).map((_, 1))
    //    val streamKeyBy: KeyedStream[(String, Int), Tuple] = stream.keyBy(0)
    //    val streamReduce: DataStream[(String, Int)] = streamKeyBy.reduce(
    //      (item1, item2) => (item1._1, item1._2 + item2._2)
    //    )
    //    streamReduce.print()

    //Fold：折叠，合并当前元素和前一次折叠操作的结果
    //    val stream=env.readTextFile("in/1.txt").flatMap(_.split(" ")).map((_,1)).keyBy(0)
    //    val streamFold=stream.fold(100)((begin,item)=>(begin+item._2))
    //    streamFold.print()
    //Aggregations：聚合 sum、sumBy、min、minBy、max、maxBy
    //    val stream = env.readTextFile("in/1.txt").flatMap(_.split(" ")).map((_, 1)).keyBy(0)
    //    val streamReduce=stream.sum(1)
    //    streamReduce.print()

    env.execute("TransformationJob")
  }
}
