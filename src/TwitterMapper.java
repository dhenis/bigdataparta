//Deni Setiawan Msc Software Engineering

import java.lang.Math;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class TwitterMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final IntWritable one = new IntWritable(1);
    private Text data = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] itr = value.toString().split(";"); // exploed and parsed to array and data type is string fix

        if(itr.length >= 4){ // manage if failed to split because lack of array (uncomplete data)

          if(itr[2].length() > 0 && itr[2].length() <= 140){ // only execute while tweet length between 1 -140

            int leng = (int) Math.ceil((double)itr[2].length() / 5); // using ceil function because to deal with 1-5, 6-10 requirement.

            int bin = leng * 5 ; // define length to a bin

            int min = (leng*5)-4; // to get floor value / minimum value

            data.set("range"+ min +"-"+bin); // so key will group by length for histogram

            context.write(data, one); // write data <key, value>

          } // end of if length of content


        }//end of check length

    }
}
