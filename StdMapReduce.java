package MapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class StdMapReduce
{
  public static class AverageMapper
    extends Mapper<Object, Text, Text, Text>
  {
    public void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
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
    extends Reducer<Text, Text, Text, Text>
  {
    public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
      throws IOException, InterruptedException
    {
      Double sum = Double.valueOf(0.0D);
      int count = 0;
      ArrayList<Double> ts = new ArrayList<Double>();
      for (Text t : values)
      {
        double newSum = sum.doubleValue() + Double.parseDouble(t.toString());
        count++;
        ts.add(Double.valueOf(newSum - sum.doubleValue()));
        sum = Double.valueOf(newSum);
      }
      for (int i = 0; i < ts.size(); i++) {
        context.write(new Text(key), new Text(ts.get(i) + " " + sum.doubleValue() / count));
      }
    }
  }
  
  public static class SDMapper
    extends Mapper<LongWritable, Text, Text, Text>
  {
    public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
      throws IOException, InterruptedException
    {
      String inputline = value.toString();
      StringTokenizer itr = new StringTokenizer(inputline);
      


      String mapkey = itr.nextToken().trim();
      Double mapvalue = Double.valueOf(Double.parseDouble(itr.nextToken().trim()));
      Double mapmean = Double.valueOf(Double.parseDouble(itr.nextToken().trim()));
      Double sd = Double.valueOf(0.0D);
      sd = Double.valueOf(Math.pow(mapvalue.doubleValue() - mapmean.doubleValue(), 2.0D));
      context.write(new Text(mapkey), new Text(""+sd));
    }
  }
  
  public static class SDReducer
    extends Reducer<Text, Text, Text, DoubleWritable>
  {
    public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, DoubleWritable>.Context context)
      throws IOException, InterruptedException
    {
      Double sum = Double.valueOf(0.0D);
      int n = 0;
      for (Text t : values)
      {
        sum = Double.valueOf(sum.doubleValue() + Double.parseDouble(t.toString()));
        n++;
      }
      context.write(new Text(key), new DoubleWritable(Math.sqrt(sum.doubleValue() / n)));
    }
  }
  
  public static void main(String[] args)
    throws IOException, InterruptedException, ClassNotFoundException
  {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2)
    {
      System.err.println("please input two args<in> <out>\n");
      System.exit(2);
    }
    Job job = new Job(conf, "count average");
    job.setJarByClass(StdMapReduce.class);
    job.setMapperClass(StdMapReduce.AverageMapper.class);
    job.setReducerClass(StdMapReduce.AverageReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path("temp"));
    job.waitForCompletion(true);
    if (job.isSuccessful())
    {
      Job job2 = new Job(conf, "calculate standard deviation");
      job2.setJarByClass(StdMapReduce.class);
      job2.setMapperClass(StdMapReduce.SDMapper.class);
      job2.setReducerClass(StdMapReduce.SDReducer.class);
      job2.setOutputKeyClass(Text.class);
      job2.setOutputValueClass(Text.class);
      FileInputFormat.setInputPaths(job2, new Path[] { new Path("temp") });
      FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
      job2.waitForCompletion(true);
      
      FileSystem fs = FileSystem.get(conf);
      fs.delete(new Path("temp"), true);
      fs.close();
      if (job2.isSuccessful()) {
        System.exit(0);
      } else {
        System.exit(1);
      }
    }
    else
    {
      System.exit(1);
    }
  }
}
