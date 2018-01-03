/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package pack.sum;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author milind
 */
public class AddNum {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private Text word = new Text();
        int sum = 0;
        int count = 0;
        String mapkey = "";

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String str = itr.nextToken();
                sum = sum + Integer.parseInt(str);
                count += 1;
                mapkey = str + "," + mapkey;
            }
        }

        public void cleanup(Context context
        ) throws IOException, InterruptedException {
            mapkey = mapkey.substring(0, mapkey.lastIndexOf(","));
            word.set(mapkey);
            context.write(word, new IntWritable(sum / count));
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();
        int mean = 0, count = 0, differenceSquare = 0, sd = 0;

        public void reduce(Text key, Iterable<IntWritable> values,
                Context context
        ) throws IOException, InterruptedException {
            for (IntWritable val : values) {
                mean = val.get();
            }
            String keySplitter[] = key.toString().split(",");
            for (String str : keySplitter) {
                count += 1;
                differenceSquare += Math.pow(Integer.parseInt(str) - mean, 2);
            }
            sd = (int) Math.sqrt(differenceSquare / count);
            result.set(sd);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Standard Deviation");
        job.setJarByClass(AddNum.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
