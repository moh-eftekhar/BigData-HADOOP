package it.polito.bigdata.hadoop.exercise3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 3 - Mapper
 */
class MapperBigData extends Mapper<
                    Text, 		  // Input key type
                    Text, 		  // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    private static final Double PM10_THRESHOLD = Double.valueOf(50);

	@Override
    protected void map(
            Text key,   		// Input key type, the key is a pair (sensor_id, date)
            Text value,         // Input value type, the value is the PM10 level
            Context context) throws IOException, InterruptedException {

            // Extract sensor and date from the key
            String[] fields = key.toString().split(",");
                        
            String sensor_id=fields[0];
            // String day=fields[1];

            Double PM10Level=Double.valueOf(value.toString());
            
            // Compare the value of PM10 with the threshold value
            if (PM10Level.compareTo(PM10_THRESHOLD)>0)
            {
                // emit the pair (sensor_id, 1)
                context.write(new Text(sensor_id), new IntWritable(1));
            }
    }
}
