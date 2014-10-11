package MapReduce;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HigherTemp
{
  public static class AverageMapper
    extends Mapper<Object, Text, Text, Text>
  {
    public void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
      throws IOException, InterruptedException
    {
      String inputline = value.toString();
      StringTokenizer itr = new StringTokenizer(inputline);
      String mapkey = itr.nextToken().trim();
      String mapvalue = itr.nextToken().trim();
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
      for (Text t : values)
      {
        sum = Double.valueOf(sum.doubleValue() + Double.parseDouble(t.toString()));
        count++;
      }
      int intKey = Integer.parseInt(key.toString());
      int topNeighbor = 0;
      int bottomNeighbor = 0;
      if (intKey == 0)
      {
        topNeighbor = 999;
        bottomNeighbor = 1;
      }
      else if (intKey == 999)
      {
        topNeighbor = 998;
        bottomNeighbor = 0;
      }
      else
      {
        topNeighbor = intKey - 1;
        bottomNeighbor = intKey + 1;
      }
      NumberFormat nf = NumberFormat.getInstance();
      nf.setMinimumIntegerDigits(3);
      
      context.write(new Text(key), new Text(sum.doubleValue() / count + " 1"));
      context.write(new Text(nf.format(topNeighbor)), new Text(sum.doubleValue() / count + " 0"));
      context.write(new Text(nf.format(bottomNeighbor)), new Text(sum.doubleValue() / count + " 0"));
    }
  }
  
  public static class HTMapper
    extends Mapper<Object, Text, Text, Text>
  {
    public void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
      throws IOException, InterruptedException
    {
      String inputline = value.toString();
      StringTokenizer itr = new StringTokenizer(inputline);
      String mapkey = itr.nextToken().trim();
      String mapvalue = itr.nextToken().trim();
      String flag = itr.nextToken().trim();
      context.write(new Text(mapkey), new Text(mapvalue + " " + flag));
    }
  }
  
  public static class HTReducer
    extends Reducer<Text, Text, Text, Text>
  {
    public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
      throws IOException, InterruptedException
    {
      Double ownTemp = Double.valueOf(0.0D);
      ArrayList<Double> nb = new ArrayList<Double>();
      String[] fireDragon;
      for (Text t : values)
      {
        fireDragon = t.toString().trim().split(" ");
        int flag = Integer.parseInt(fireDragon[1]);
        if (flag == 1) {
          ownTemp = Double.valueOf(Double.parseDouble(fireDragon[0]));
        } else {
          nb.add(Double.valueOf(Double.parseDouble(fireDragon[0])));
        }
      }
      boolean success = true;
      for (Double n : nb) {
        if (n.doubleValue() >= ownTemp.doubleValue())
        {
          success = false;
          break;
        }
      }
      if (success) {
        context.write(new Text(key), new Text(""));
      }
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
    job.setJarByClass(HigherTemp.class);
    job.setMapperClass(HigherTemp.AverageMapper.class);
    job.setReducerClass(HigherTemp.AverageReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path("temp"));
    job.waitForCompletion(true);
    if (job.isSuccessful())
    {
      Job job2 = new Job(conf, "calculate higher temperature");
      job2.setJarByClass(HigherTemp.class);
      job2.setMapperClass(HigherTemp.HTMapper.class);
      job2.setReducerClass(HigherTemp.HTReducer.class);
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
