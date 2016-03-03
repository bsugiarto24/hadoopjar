.PHONY: clean all dine test
	
repeat:
	javac -cp hadoop-core-1.2.1.jar repeatLetters.java 
	jar cvfm repeat.jar manifest.txt *.class
	
invert:
	javac -cp hadoop-core-1.2.1.jar invert.java 
	jar cvfm invert.jar manifest2.txt *.class
	
scores:
	javac -cp hadoop-core-1.2.1.jar scores.java 
	jar cvfm scores.jar manifest3.txt *.class
	
index:
	javac -cp hadoop-core-1.2.1.jar invertedIndex.java 
	jar cvfm index.jar manifest4.txt *.class

mixture:
	javac -cp hadoop-core-1.2.1.jar mixture.java 
	jar cvfm mixture.jar manifest5.txt *.class
	
histogram:
	javac -cp \* histogram.java
	jar cvfm histogram.jar manifest6.txt *.class
	
summaries:
	javac -cp \* summaries.java
	jar cvfm summaries.jar manifest7.txt *.class

activity:
	javac -cp \* activity.java
	jar cvfm activity.jar manifest8.txt *.class

example:
	javac -cp \* example.java
	jar cvfm example.jar manifest9.txt *.class
		
run:
	echo "hadoop jar histogram.jar MultilineJsonJob -libjars org.json-20120521.jar,json-mapreduce-1.0.jar test/test.json test/output/"

get:
	hadoop fs -get /user/bsugiart/test/output output

clean:
	hadoop fs -rm -r /user/bsugiart/test/output
	rm -r output
	
replace:
	hadoop fs -rm thot.txt
	hadoop fs -put thot.txt

seq:
	javac -cp \* *.java
	java -cp .:\* beFuddledGen
	java -cp .:\* histogramSeq
	hadoop fs -rm out.txt
	hadoop fs -put out.txt
	hadoop jar histogram.jar -libjars org.json-20120521.jar,json-mapreduce-1.0.jar test/out.txt test/output/

show:
	cat output/part-r-00000 
	
pull:
	git pull
	
push:
	git add *
	git commit -m "auto push"
	git push
