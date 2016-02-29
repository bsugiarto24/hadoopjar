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
	javac -cp hadoop-core-1.2.1.jar histogram.java 
	jar cvfm histogram.jar manifest6.txt *.class
		
make runhistogram:
	hadoop jar job.jar MultilineJsonJob -libjars org.json-20120521.jar,json-mapreduce-1.0.jar test/test.json test/output/

get:
	hadoop fs -get /user/bsugiart/test/output output

clean:
	hadoop fs -rm -r /user/bsugiart/test/output
	rm -r output
	

show:
	cat output/part-r-00000 
	
pull:
	git pull
	
push:
	git add *
	git commit -m "auto push"
	git push
