package score;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

// 统计每个学生的平均成绩（为了防止学生重名，输出平均成绩时同时输出学号，姓名和平均成绩)
public class one_demo {
    public static class Q1mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            // 001 Jerry 81 70
            // Num name math english
            // 学号 姓名 课程“math”的分数 课程“English”的分数
            String data = v1.toString();
            String[] words = data.split("\\s+");

            // ([学号，姓名]， [数学成绩， 英语成绩])
            // [Num name], [...]
            context.write(new Text(words[0]+" "+words[1]), new IntWritable(Integer.parseInt(words[2])+Integer.parseInt(words[3])));
        }
    }

    public static class Q1Reduce extends Reducer<Text, IntWritable, Text, FloatWritable> {
        protected void reduce(Text k3, Iterable<IntWritable> v3, Context context) throws IOException, InterruptedException {
            float avg_score = 0;
            for (IntWritable writable : v3) {
                avg_score = writable.get()/(float)2;
            }
            // ([学号，姓名]， (数学成绩 + 英语成绩) // 2 )
            context.write(k3, new FloatWritable(avg_score));
        }
    }


    public static void main(String[] args) throws Exception {
        //1. 创建一个job和任务入口(指定主类)
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(one_demo.class);
        //2. 指定job的mapper和输出的类型<k2 v2>
        job.setMapperClass(one_demo.Q1mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //3. 指定job的reducer和输出的类型<k4  v4>
        job.setReducerClass(one_demo.Q1Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        // 本地测试
        Path inputpath = new Path("src/inputdata/score.txt");
        Path outputPath = new Path("src/outputdata/Score/one_ques");
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
