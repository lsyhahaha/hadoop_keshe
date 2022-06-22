package score;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

// 统计每门课程的最低成绩（输出课程名和最低成绩）
public class four_demo {
    public static class mapper extends Mapper<LongWritable, Text,Text, FloatWritable> {
        @Override
        public void map(LongWritable k1,Text v1,Context context) throws IOException, InterruptedException {
            // 001 Jerry 81 70
            // 学号 姓名 课程“math”的分数 课程“English”的分数
            String line = v1.toString();
            String[] strings = line.split("\\s+");
            context.write(new Text("mathmin"),new FloatWritable(Float.parseFloat(strings[2])));
            context.write(new Text("englishmin"),new FloatWritable(Float.parseFloat(strings[3])));
        }
    }

    public static class reducer extends Reducer<Text,FloatWritable,Text,FloatWritable> {
        @Override
        public void reduce(Text k3,Iterable<FloatWritable> v3,Context context) throws IOException, InterruptedException {
            float min_score = 100;
            for (FloatWritable writable : v3){
                if (min_score >= writable.get()){min_score = writable.get();}
            }
            context.write(k3,new FloatWritable(min_score));
        }
    }

    public static void main(String[] args) throws Exception {
        //1. 创建一个job和任务入口(指定主类)
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(four_demo.class);

        //2. 指定job的mapper和输出的类型<k2 v2>
        job.setMapperClass(four_demo.mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        //3. 指定job的reducer和输出的类型<k4  v4>
        job.setReducerClass(four_demo.reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        // 本地测试
        Path inputpath = new Path("src/inputdata/score.txt");
        Path outputPath = new Path("src/outputdata/Score/four_ques");
        FileInputFormat.setInputPaths(job, inputpath);
        FileOutputFormat.setOutputPath(job, outputPath);

        //如果输出路径存在，则删除此路径
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath))
        {
            fs.delete(outputPath,true);
        }

        //5. 执行job
        boolean b = job.waitForCompletion(true);
        System.exit(b? 0:1);
    }
}
