package score;

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

// 统计课程成绩分布情况（包括该课程考试的总人数、90分以上的人数80-89分的人数、60-79分的人数、60分以下的人数）
public class five_demo {
    static int total_num = 0;
    public static class mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        public static String score_group(float x) {
            String group = "";
            if (x >= 90.0) {
                group = "90分以上";
            } else if (x <= 89 && x >= 80) {
                group = "80-89分";
            } else if (x <= 79 && x >= 60) {
                group = "60-79分";
            } else {
                group = "60分以下";
            }
            return group;
        }

        @Override
        public void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            Text math = new Text();
            Text eng = new Text();
            Text math_num = new Text("math的总人数为：");
            Text eng_num = new Text("english的总人数为");

            // 001 Jerry 81 70
            // 学号 姓名 课程“math”的分数 课程“English”的分数
            String line = v1.toString();
            String[] strings = line.split("\\s+");
            // if (strings.length != 4){return;}

            // 数据赋值
            math.set("math:" + score_group(Float.parseFloat(strings[2])));
            eng.set("english:" + score_group(Float.parseFloat(strings[3])));

            // （[math: 60分以下]， 1） 或者...
            context.write(math, new IntWritable(1));
            // （[english: 60分以下]， 1） 或者...
            context.write(eng, new IntWritable(1));
            // ([math的总人数为：, 1])
            context.write(math_num,new IntWritable(1));
            // ([english的总人数为：, 1])
            context.write(eng_num,new IntWritable(1));
        }
    }

    public static class reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text k3, Iterable<IntWritable> v3, Context context) throws IOException, InterruptedException {
            int nums = 0;
            for (IntWritable writable : v3) {
                nums = nums + writable.get();
            }
            context.write(k3, new IntWritable(nums));
        }
    }

    public static void main(String[] args) throws Exception {
        //1. 创建一个job和任务入口(指定主类)
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(five_demo.class);

        //2. 指定job的mapper和输出的类型<k2 v2>
        job.setMapperClass(five_demo.mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //3. 指定job的reducer和输出的类型<k4  v4>
        job.setReducerClass(five_demo.reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 本地测试
        Path inputpath = new Path("src/inputdata/score.txt");
        Path outputPath = new Path("src/outputdata/Score/five_ques");
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
        System.exit(b ? 0 : 1);
    }
}
