package MapReduce;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountAverage
{
  public static class AverageMapper
    extends Mapper<LongWritable, Text, Text, Text>
  {
    public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
      throws IOException, InterruptedException
    {
      String inputline = value.toString();
      StringTokenizer itr = new StringTokenizer(inputline);
      String mapkey = "";
      String mapvalue = "";
      int count = 0;
      while (itr.hasMoreTokens()) {
        if (count > 0)
        {
          mapvalue = itr.nextToken();
        }
        else
        {
          mapkey = itr.nextToken();
          count++;
        }
      }
      context.write(new Text(mapkey), new Text(mapvalue));
    }
  }
  
  public static class AverageReducer
    extends Reducer<Text, Text, Text, DoubleWritable>
  {
    public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, DoubleWritable>.Context context)
      throws IOException, InterruptedException
    {
      Double sum = Double.valueOf(0.0D);
      int count = 0;
      for (Text t : values)
      {
        sum = Double.valueOf(sum.doubleValue() + Double.parseDouble(t.toString()));
        count++;
      }
      context.write(new Text(key), new DoubleWritable(sum.doubleValue() / count));
    }
  }
  
  public static void main(String[] args)
    throws IOException, InterruptedException, ClassNotFoundException
  {
    Configuration conf = new Configuration();
    if (args.length != 2)
    {
      System.err.println("please input two args<in> <out>\n");
      System.exit(2);
    }
    Job job = new Job(conf, "count average");
    job.setJarByClass(CountAverage.class);
    job.setMapperClass(AverageMapper.class);
    job.setReducerClass(AverageReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
