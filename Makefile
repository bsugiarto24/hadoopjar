.PHONY: clean all dine test
	
all: dine

dine:
	javac -cp hadoop-core-1.2.1.jar repeatLetters.java 
	jar cvfm repeat.jar manifest.txt *.class

get:
	hadoop fs -get /user/bsugiart/test/output output

clean:
	rm -r output
	hadoop fs -rm -r /user/bsugiart/test/output

show:
	cat output/part-r-00000 
