/**
 * Created by leo on 13/10/16.
 *
 * M/R 3: Proportion (in%) of male or female
 *
 * In this one I tried to do 2 jobs, the first output being the input for the second one
 * Unfortunately, I could not get it to work
 *
 */
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

public class Proportion2 {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            //First we need to get the third column which are all the origins
            //We also need need to split these origins if there are more than one
            String[] sexes = value.toString().split(";")[1].split(",");

            //For each sex
            for(String sex: sexes)
            {
                //We don't care about the blank spaces
                if(sex == "" || sex == " ")
                    continue;
                word.set(sex.replaceAll("\\s+",""));
                context.write(word, one);
            }
        }
    }

    public static class TokenizerMapper2
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word,  new IntWritable(Integer.parseInt(itr.nextToken())));
            }
        }
    }


    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {


            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            //long a = context.getCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_OUTPUT_RECORDS").getValue();

            result.set(sum);
            context.write(key, result);
        }
    }

    public static class IntSumReducer2
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {


            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            int tot = context.getConfiguration().getInt("TOT",1);

            result.set(tot);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        //job1
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Proportion");
        job.setJarByClass(Proportion.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("first_job_output"));
        //System.exit(job.waitForCompletion(true) ? 0 : 1);
        job.waitForCompletion(true);

        int tot = (int)job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_OUTPUT_RECORDS").getValue();

        //Job2
        Configuration conf2 = new Configuration(conf);
        conf2.setInt("TOT", tot);
        Job job2 = Job.getInstance(conf2, "Proportion");
        job.setJarByClass(Proportion.class);
        job.setMapperClass(TokenizerMapper2.class);
        job.setCombinerClass(IntSumReducer2.class);
        job.setReducerClass(IntSumReducer2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("first_job_output"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}