package meteorological;

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

// 统计月度平均气温
public class one_demo {
    public static class QixiangMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            // 2021 01 01 00    80   -94 10285    50    60     1 -9999 -9999
            // 年 月 日 小时 气温(/10) 露点温度(/10) 海平面气压(/10) 风向 风速(/10)  天空条件总覆盖代码 液体沉淀深度维度-一小时间隔(/10) 液体沉淀深度维度-六小时间隔(/10)
            String data = v1.toString();
            String[] words = data.split("\\s+");
            if (words[4].equals("-9999")){return;}
            if (words[5].equals("-9999")){return;}
            if (words[6].equals("-9999")){return;}
            if (words[7].equals("-9999")){return;}
            if (words[8].equals("-9999")){return;}
            if(words.length != 12){return;}
            // （月， 气温）
            context.write(new Text(words[1]), new FloatWritable(Float.valueOf(words[4])));//
        }
    }

    public static class QixiangReduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        protected void reduce(Text k3, Iterable<FloatWritable> v3, Context context) throws IOException, InterruptedException {
            float total1 = 0;
            float count = 0;
            float avg_airtem ;
            for (FloatWritable writable : v3) {
                    total1 = total1 + writable.get();
                    count++;
            }
            avg_airtem = (total1/count)/(float)10;
            // （月，平均气温）
            context.write(k3, new FloatWritable(avg_airtem));
        }
    }


    public static void main(String[] args) throws Exception {
        //1. 创建一个job和任务入口(指定主类)
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(one_demo.class);

        //2. 指定job的mapper和输出的类型<k2 v2>
        job.setMapperClass(one_demo.QixiangMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        //3. 指定job的reducer和输出的类型<k4  v4>
        job.setReducerClass(one_demo.QixiangReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        // 本地测试
        Path inputpath = new Path("src/inputdata/china_isd_lite_2021/*");
        Path outputPath = new Path("src/outputdata/meteorological/one_ques");
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
