package hadoop.mapreduce.tp1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static void main(String[] args) throws Exception {
        // التحقق من صحة الوسائط
        if (args.length != 2) {
            System.err.println("Usage: WordCount <input path> <output path>");
            System.exit(-1);
        }

        // تهيئة الإعدادات
        Configuration conf = new Configuration();
        
        // إنشاء Job جديد مع اسم واضح
        Job job = Job.getInstance(conf, "Word Count v1.0");
        job.setJarByClass(WordCount.class);

        // تعيين Mapper و Reducer و Combiner
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        // تعيين أنواع المخرجات
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // تحديد مسارات الإدخال والإخراج
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // إضافة خيارات متقدمة (اختياري)
        configureJobOptions(job);

        // تنفيذ Job وإرجاع حالة الخروج
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }

    // (اختياري) إعدادات إضافية للـ Job
    private static void configureJobOptions(Job job) {
        // مثال: تحديد عدد الـ Reducers
        job.setNumReduceTasks(2);  // يمكن تغيير الرقم حسب الحاجة
        
        // (اختياري) ضغط المخرجات لتوفير المساحة
        job.getConfiguration().set("mapreduce.output.fileoutputformat.compress", "true");
        job.getConfiguration().set("mapreduce.output.fileoutputformat.compress.codec", 
                                  "org.apache.hadoop.io.compress.GzipCodec");
    }
}