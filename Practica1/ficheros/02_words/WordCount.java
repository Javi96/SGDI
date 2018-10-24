import java.io.IOException;
import java.util.StringTokenizer;
import java.nio.file.Files;
import java.util.stream.*;
import java.nio.file.Paths;
import java.util.List;
import java.util.HashMap;  
import java.util.ArrayList;
import java.util.regex.*;
import java.io.DataOutput;
import java.io.DataInput;

import java.util.Map;
import java.util.Iterator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapred.JobConf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static class PairWritable implements Writable{

        private Text key;
        private IntWritable value;

        public PairWritable(){
            this.key = new Text();
            this.value = new IntWritable();
        }

        public PairWritable(Text key, IntWritable value){
            this.key = key;
            this.value = value;
        }

        public void write(DataOutput out) throws IOException {
            key.write(out);
            value.write(out);
        }

        public void readFields(DataInput in) throws IOException {
            key.readFields(in);
            value.readFields(in);
        }

        public void set(Text key, IntWritable value){
            this.key = key;
            this.value = value;
        }

        public String getKey(){
            return key.toString();
        }

        public int getValue(){
            return value.get();
        }

        @Override
        public String toString(){
            return "<" + key.toString() + " " + value.toString() + ">";
        }
    }

    static final String FOLDER_NAME = "test";

    public static class MyMap extends Mapper<Object, Text, Text, PairWritable>{

        private Text file = new Text();

        protected void setup(Context context) throws IOException, InterruptedException{
            file.set(((FileSplit) context.getInputSplit()).getPath().getName().toString());
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String wordAux = itr.nextToken();
                if(!Pattern.compile("[A-Za-z]*").matcher(wordAux).matches()){ // no es una palabra
                    wordAux = wordAux.replaceAll("[^A-Za-z]", " ");
                    for(String w : wordAux.split(" ")){
                        if(w.length()!=0){ // controlamos cadenas vacías tras el split
                            context.write(new Text( w.toLowerCase()), new PairWritable(new Text(file), new IntWritable(1)));
                        }
                    }
                }else{ // es una palabra
                    context.write(new Text(wordAux.toLowerCase()), new PairWritable(new Text(file), new IntWritable(1)));
                }                
            }
        }
    }

    public static HashMap<String, Integer> fillHashMap(final Iterable<PairWritable> values){
        HashMap<String, Integer> hashMap = new HashMap<String,Integer>();
        for (PairWritable value : values) {
            if(hashMap.containsKey(value.getKey())==false){
                hashMap.put(value.getKey(), value.getValue());
            }else{
                Integer total = value.getValue() + hashMap.get(value.getKey());
                hashMap.put(value.getKey(), value.getValue() + hashMap.get(value.getKey()));
            }
        }
        return hashMap;
    }

    public static class Combiner extends Reducer<Text, PairWritable, Text, PairWritable>{
        
        public void reduce(final Text key, final Iterable<PairWritable> values, final Context context) throws IOException, InterruptedException{
            HashMap<String, Integer> hashMap = new HashMap<String,Integer>();
            for (PairWritable value : values) {
                if(hashMap.containsKey(value.getKey())==false){
                    hashMap.put(value.getKey(), value.getValue());
                }else{
                    Integer total = value.getValue() + hashMap.get(value.getKey());
                    hashMap.put(value.getKey(), value.getValue() + hashMap.get(value.getKey()));
                }
            }
            Iterator it = hashMap.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry)it.next();
                context.write(key, new PairWritable(new Text((String)pair.getKey()), new IntWritable((Integer)pair.getValue())));
                it.remove();
            }
        }

    }

    public static class Reduce extends Reducer<Text,PairWritable,Text,PairWritable> {
    
        public void reduce(Text key, Iterable<PairWritable> values, Context context) throws IOException, InterruptedException {
            Iterator it = fillHashMap(values).entrySet().iterator();
            Map.Entry pair = (Map.Entry)it.next();
            String goodFile = (String)pair.getKey();
            Integer goodCount = (Integer)pair.getValue();
            while (it.hasNext()) {
                pair = (Map.Entry)it.next();
                if(goodCount < (Integer)pair.getValue()){
                    goodCount = (Integer)pair.getValue();
                    goodFile = (String)pair.getKey();
                }
                it.remove(); 
            }
            context.write(key, new PairWritable(new Text(goodFile), new IntWritable(goodCount)));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCount.class);
        job.setMapperClass(MyMap.class);

        job.setCombinerClass(Combiner.class);
        job.setReducerClass(Reduce.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PairWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PairWritable.class);

        FileSystem fs = FileSystem.get(new JobConf());
        List<String> fileName = new ArrayList<String>();
        RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(new Path(FOLDER_NAME), true);
        while(fileStatusListIterator.hasNext()){
            LocatedFileStatus fileStatus = fileStatusListIterator.next();
            fileName.add(FOLDER_NAME + "/" + fileStatus.getPath().getName());

        }

        FileInputFormat.addInputPaths(job, String.join(",", fileName));
        FileOutputFormat.setOutputPath(job, new Path( "salida" ));

        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(false) ? 0 : 1);
    }
}

