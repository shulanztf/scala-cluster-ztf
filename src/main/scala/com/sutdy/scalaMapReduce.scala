import java.io.IOException
import java.lang

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

/**
  * https://blog.csdn.net/legotime/article/details/52832328?locationNum=10&fps=1 用scala来写mapreduce做数据去重
  */
object scalaMapReduce {
  class myScalaMapper extends Mapper[Object, Text, Text, Text] {
    private var line: Text = new Text
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context) {
      try {
        line = value
        context.write(line, new Text(""))
      }
      catch {
        case e: InterruptedException => e.printStackTrace()
        case e: IOException => e.printStackTrace()
      }
    }
  }
  class myScalaReducer extends Reducer[Text, Text, Text, Text]{
    override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
      context.write(key, new Text(""))
    }

//    override def reduce(key: Text, value: Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context) {
//      try {
//        context.write(key, new Text(""))
//      }
//      catch {
//        case e: InterruptedException => e.printStackTrace()
//        case e: IOException => e.printStackTrace()
//      }
//    }
  }



  def main(args: Array[String]) {
    val conf = new Configuration()
    val job = new Job(conf,"Data Deduplication by scala")

    //如果是打包成jar文件，那么下面就需要
    //job.setJarByClass(this.getClass)

    //设置Map,Combine,Reduce处理类
    job.setMapperClass(classOf[scalaMapReduce.myScalaMapper])
    job.setCombinerClass(classOf[scalaMapReduce.myScalaReducer])
    job.setReducerClass(classOf[scalaMapReduce.myScalaReducer])

    //设置输出类型
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])

    //设置输入和输出目录
    FileInputFormat.addInputPath(job, new Path("/hadoop/mapreduce/DedupData/"))
    FileOutputFormat.setOutputPath(job, new Path("/hadoop/mapreduce/DedupOutDatabyScala/"))

    System.exit(if (job.waitForCompletion(true)) 0 else 1)



  }
}
