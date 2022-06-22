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

// 统计每门课程的平均成绩（输出课程名和平均成绩）
public class two_demo {
    public static class Q2mapper extends Mapper<LongWritable, Text,Text, FloatWritable>{
        private Text title_engli;
        private Text title_math;


        @Override
        public void map(LongWritable k1,Text v1,Context context) throws IOException, InterruptedException {
            // 001 Jerry 81 70
            // xuehao name math english
            // 学号 姓名 课程“math”的分数 课程“English”的分数
            String line = v1.toString();
            String[] strings = line.split("\\s+");

            // .matches("[0-9]+")
            // checkIfNotInt(strings[2]) == false
            System.out.println(line);
            if (strings[2].matches("[0-9]+") == false) {
                // 判断取值后是否是整型
                title_math = new Text(strings[2]);
                title_engli = new Text(strings[3]);
                return;
            }

            // (math, [数学成绩])
            context.write(title_math, new FloatWritable(Float.parseFloat(strings[2])));
            // (math, [英语成绩])
            context.write(title_engli, new FloatWritable(Float.parseFloat(strings[3])));
        }
    }

    public static class Q2reducer extends Reducer<Text,FloatWritable,Text,FloatWritable>{
        @Override
        public void reduce(Text k3,Iterable<FloatWritable> v3,Context context) throws IOException, InterruptedException {
            float score_tol = 0;
            float count = 0;
            for (FloatWritable writable : v3){
                score_tol = score_tol+writable.get();
                count++;
            }
            float avg = score_tol/count;
            // 数学或者英语， 成绩
            context.write(k3,new FloatWritable(avg));
        }
    }

    public static void main(String[] args) throws Exception {
        //1. 创建一个job和任务入口(指定主类)
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(two_demo.class);

        //2. 指定job的mapper和输出的类型<k2 v2>
        job.setMapperClass(Q2mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        //3. 指定job的reducer和输出的类型<k4  v4>
        job.setReducerClass(Q2reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        // 本地测试
        Path inputpath = new Path("src/inputdata/score.txt");
        Path outputPath = new Path("src/outputdata/Score/two_ques");
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
