package meteorological;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

// 根据每天平均风速的统计结果，统计各级风力的天数
public class three_demo {
    public static String wind_speed_lev(float x){
        String level = "";
        if (x>=0 && x<=0.2){level = "无风";}
        else if (x>0.2 && x<=3.3){level = "清风";}
        else if (x>3.3 && x<=5.4){level = "微风";}
        else if (x>5.4 && x<=7.9){level = "和风";}
        else if (x>7.9 && x<=13.8){level = "强风";}
        else if (x>13.8 && x<=17.1){level = "疾风";}
        else if (x>17.1 && x<=21.7){level = "大风";}
        else if (x>21.7 && x<=24.4){level = "烈风";}
        else if (x>24.4 && x<=28.4){level = "狂风";}
        else if (x>28.4 && x<=32.6){level = "暴风";}
        else if (x>32.6){level = "暴风以上";}
        return level;
    }
    public static class Q3Mapper extends Mapper<LongWritable, Text,Text, IntWritable>{
        public void map(LongWritable k1 , Text v1,Context context) throws IOException, InterruptedException {
            // 01月02日	2.360176
            Text text = new Text();
            String data = v1.toString();
            String words[] = data.split("\\s+");

            text.set(wind_speed_lev(Float.parseFloat(words[1])));
            // (清风	1)
            context.write(text, new IntWritable(1));
        }
    }

    public static class Q3Reducer extends Reducer<Text , IntWritable ,Text,IntWritable>{
        public void reduce(Text k3, Iterable<IntWritable> v3 ,Context context) throws IOException, InterruptedException {
            int count = 0;
            for(IntWritable writable : v3){
                count += writable.get();
            }
            context.write(k3,new IntWritable(count));
        }
    }

    public static void main(String[] args) throws Exception {
        //1. 创建一个job和任务入口(指定主类)
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(three_demo.class);

        //2. 指定job的mapper和输出的类型<k2 v2>
        job.setMapperClass(three_demo.Q3Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //3. 指定job的reducer和输出的类型<k4  v4>
        job.setReducerClass(three_demo.Q3Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 本地测试
        Path inputpath = new Path("src/outputdata/meteorological/two_ques/part-r-00000");
        Path outputPath = new Path("src/outputdata/meteorological/three_ques");
        FileInputFormat.setInputPaths(job, inputpath);
        FileOutputFormat.setOutputPath(job, outputPath);

        //如果输出路径存在，则删除此路径
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath))
        {
            fs.delete(outputPath,true);
        }

        //执行job
        boolean b = job.waitForCompletion(true);
        System.exit(b? 0:1);
    }
}
