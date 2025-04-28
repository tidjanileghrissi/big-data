package hadoop.mapreduce.tp1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    // استخدام final لأن القيمة ثابتة (لا تتغير)
    private static final IntWritable one = new IntWritable(1);
    private Text word = new Text();  // يمكن جعله final إذا لم يتم إعادة تعيينه

    @Override  // للتأكيد على أننا نجاوز الدالة الأصلية
    public void map(Object key, Text value, Context context) 
            throws IOException, InterruptedException {
        
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            
            // (اختياري) تنظيف النص: تحويل إلى أحرف صغيرة، إزالة علامات الترقيم
            token = token.toLowerCase().replaceAll("[^a-zA-Z0-9]", "");
            
            if (!token.isEmpty()) {  // تجاهل الكلمات الفارغة بعد التنظيف
                word.set(token);
                context.write(word, one);
                
                // (اختياري) طباعة الكلمات لتصحيح الأخطاء
                System.out.println("Emitting: (" + token + ", 1)");
            }
        }
    }
}