#! /bin/bash

HADOOP_DIR=/usr/local/apache-hadoop # Directorio donde está Hadoop
CLASS_NAME=WordCount # Nombre de la clase principal




# Limpiar compilaciones anteriores
rm -rf *.class

# Compilar la clase que contiene el Map y Reduce (opcionalmente Combiner)
javac ${CLASS_NAME}.java -cp ${HADOOP_DIR}/share/hadoop/common/hadoop-common-3.1.1.jar:${HADOOP_DIR}/share/hadoop/mapreduce/hadoop-mapreduce-client-core-3.1.1.jar:${HADOOP_DIR}/share/hadoop/common/lib/commons-cli-1.2.jar

# Según la documentación, también se podría compilar con:
# ${HADOOP_DIR}/bin/hadoop com.sun.tools.javac.Main WordCount.java
# pero hay que establecer HADOOP_CLASSPATH para que contenga /usr/lib/jvm/<JAVA FOLDER>/lib/tools.jar

# Crear un fichero JAR con todos las clases
jar cf ${CLASS_NAME}.jar *.class
	
# Borrar el directorio de salida para evitar errores al lanzar la tarea
rm -rf salida/

# Lanzar la tarea MapReduce 
$HADOOP_DIR/bin/hadoop jar ${CLASS_NAME}.jar ${CLASS_NAME}
