/**
 * Adaptación del ejemplo original en http://wiki.apache.org/hadoop/WordCount
 */

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

/**
 * <p>Este ejemplo cuenta el número de veces que aparece cada palabra en el 
 * archivo de entrada usando MapReduce. El código tiene 3 partes: mapper, 
 * reduce y programa principal</p>
 */
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
            return key.toString() + " " + value.toString();
        }
    }

    static final String FOLDER_NAME = "test";

  /**
   * <p>
   * El mapper extiende de la interfaz org.apache.hadoop.mapreduce.Mapper. Cuando
   * se ejecuta Hadoop, el mapper recibe cada linea del archivo de entrada como
   * argumento. La función "map" parte cada línea y para cada palabra emite la
   * pareja (word,1) como salida.</p>
   */
  public static class MyMap extends Mapper<Object, Text, Text, PairWritable>{

    //private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text file = new Text();
    private PairWritable pair = new PairWritable();
    private IntWritable one = new IntWritable(1);
    private Text fileName = new Text();

    protected void setup(Context context) throws IOException, InterruptedException{
        file.set(((FileSplit) context.getInputSplit()).getPath().getName().toString());
    }

    public boolean isNumeric(String s) {  
        return s != null && s.matches("[-+]?\\d*\\.?\\d+");  
    }  

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        /*System.out.println("----------------------------------------------");
        System.out.println(fileName);*/
        while (itr.hasMoreTokens()) {
            String word_aux = itr.nextToken();
            Pattern pattern = Pattern.compile(".*[\\.\\,\\;\\:\\'\"\\(\\)\\?\\¿\\¡\\!\\{\\}\\[\\]].*");
            pattern = Pattern.compile("[A-Za-z]*");
            Matcher match = pattern.matcher(word_aux);
            if(!match.matches()){
                System.out.println("no MATCH ------------------");
                System.out.println(word_aux);
                System.out.println(fileName);
            }else{
                word.set( word_aux );
                pair.set(file, one);
                System.out.println( "nombre del fichero: " + file.toString());
                context.write(word, pair);
            }
        }
    }
}

    public static class Combiner extends Reducer<Text, PairWritable, Text, PairWritable>{

        private IntWritable result = new IntWritable();
        private PairWritable pairWritable = new PairWritable();
        private Text file = new Text();

        public void reduce(final Text key, final Iterable<PairWritable> values, final Context context) throws IOException, InterruptedException{
            
            HashMap<String, Integer> hashMap = new HashMap<String,Integer>();
            System.out.println("--------------------------------COMBINER INPUT-------------------------------");
            for (PairWritable value : values) {
                //sum += val.get();
                if(hashMap.containsKey(value.getKey())==false){
                    hashMap.put(value.getKey(), value.getValue());
                }else{
                    Integer total = value.getValue() + hashMap.get(value.getKey());
                    hashMap.put(value.getKey(), total);
                }
                System.out.println(value);
            }

            Iterator it = hashMap.entrySet().iterator();
            Map.Entry pair = (Map.Entry)it.next();
            String goodFile = (String)pair.getKey();
            Integer goodCount = (Integer)pair.getValue();
            String goodWord = key.toString();
            while (it.hasNext()) {
                pair = (Map.Entry)it.next();
                if(goodCount < (Integer)pair.getValue()){
                    goodCount = (Integer)pair.getValue();
                    goodFile = (String)pair.getKey();
                }
                it.remove(); // avoids a ConcurrentModificationException
            }
            result.set(goodCount);
            file.set(goodFile);
            pairWritable.set(file, result);
            System.out.println("--------------------------------COMBINER OUTPUT-------------------------------");
            System.out.println(pairWritable);
            context.write(key, pairWritable);
        }

    }
  /**
   * <p>La función "reduce" recibe los valores (apariciones) asociados a la misma
   * clave (palabra) como entrada y produce una pareja con la palabra y el número
   * total de apariciones. 
   * Ojo: como las parejas generadas por la función "map" siempre tienen como 
   * valor 1 se podría evitar la suma y devolver directamente el número de 
   * elementos.</p>  
   */
    public static class Reduce extends Reducer<Text,PairWritable,Text,PairWritable> {
    
        private IntWritable result = new IntWritable();
        private PairWritable pairWritable = new PairWritable();
        private Text file = new Text();

        public void reduce(Text key, Iterable<PairWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            System.out.println("--------------------------------REDUCER INPUT-------------------------------");
            HashMap<String, Integer> hashMap = new HashMap<String,Integer>();
            for (PairWritable value : values) {
                //sum += val.get();
                if(hashMap.containsKey(value.getKey())==false){
                    hashMap.put(value.getKey(), value.getValue());
                }else{
                    Integer total = value.getValue() + hashMap.get(value.getKey());
                    hashMap.put(value.getKey(), total);
                }
                System.out.println(value);
                sum++;
            }

            Iterator it = hashMap.entrySet().iterator();
            Map.Entry pair = (Map.Entry)it.next();
            String goodFile = (String)pair.getKey();
            Integer goodCount = (Integer)pair.getValue();
            String goodWord = key.toString();
            
            //System.out.println("---------------------------HASH MAP-------------------------");
            while (it.hasNext()) {
                pair = (Map.Entry)it.next();
                if(goodCount < (Integer)pair.getValue()){
                    goodCount = (Integer)pair.getValue();
                    goodFile = (String)pair.getKey();
                }
                //System.out.println(pair.getKey().getClass() + " = " + pair.getValue().getClass());
                it.remove(); // avoids a ConcurrentModificationException
            }
            //System.out.println("---------------------------HASH MAP-------------------------");
            result.set(goodCount);
            file.set(goodFile);
            pairWritable.set(file, result);
            //System.out.println("FICHERO: " + goodFile);
            //System.out.println("PALABRA: " + goodWord);
            //System.out.println("REPETIC: " + goodCount);
            System.out.println("--------------------------------REDUCER OUTPUT-------------------------------");
            System.out.println(file  + " " + result);
            context.write(key, pairWritable);
    }
}



  /**
   * <p>Clase principal con método main que iniciará la ejecución de la tarea</p>
   */
  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf();
    Job job = Job.getInstance(conf);
    job.setJarByClass(WordCount.class);
    job.setMapperClass(MyMap.class);
    //Si existe combinador
    job.setCombinerClass(Combiner.class);
    job.setReducerClass(Reduce.class);

    // Declaración de tipos de salida para el mapper
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(PairWritable.class);
    // Declaración de tipos de salida para el reducer
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(PairWritable.class);

    // Archivos de entrada y directorio de salida
    /*FileSystem fileSystem = FileSystems.getDefault();
    if(fileSystem.isDirectory(new Path("sci.space"))){
        RemoteIterator<LocatedFileStatus> files = listFiles("sci.space", false);
        while(files.hasNext()){
            LocatedFileStatus fileStatus = files.next();
            String fileName = fileStatus.getPath().getName();
            System.out.println("----------------------------------------------");
            System.out.println(fileName);
            System.out.println("----------------------------------------------");
        }
    }*/

    //Configuration fileConf = getConf(); 
    StringBuilder paths = new StringBuilder("");

    FileSystem fs = FileSystem.get(new JobConf());
    int count = 0;
    List<String> fileName = new ArrayList<String>();
    RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(new Path(FOLDER_NAME), true);
    while(fileStatusListIterator.hasNext()){
        LocatedFileStatus fileStatus = fileStatusListIterator.next();
        //do stuff with the file like ...
        System.out.println("----------------------------------------------");
        System.out.println(fileStatus.getPath().getName());      
        fileName.add(FOLDER_NAME + "/" + fileStatus.getPath().getName());
        count++;    
        System.out.println("----------------------------------------------");

        //job.addFileToClassPath(fileStatus.getPath());
    }

    System.out.println("----------------------------------------------");
    System.out.println(count); 
    System.out.println(String.join(",", fileName));        
    System.out.println("----------------------------------------------");


    FileInputFormat.addInputPaths(job, String.join(",", fileName));
    FileOutputFormat.setOutputPath(job, new Path( "salida" ));
    
    // Aquí podemos elegir el numero de nodos Reduce
    // Cada reducer genera un fichero  'part-r-XXXXX'
    job.setNumReduceTasks(1);

		// Ejecuta la tarea y espera a que termine. El argumento boolean es para 
    // indicar si se quiere información sobre de progreso (verbosity)
    System.exit(job.waitForCompletion(false) ? 0 : 1);
}
}

