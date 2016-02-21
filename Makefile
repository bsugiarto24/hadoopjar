.PHONY: clean all dine test
	
all: dine

repeat:
	javac -cp hadoop-core-1.2.1.jar repeatLetters.java 
	jar cvfm repeat.jar manifest.txt *.class
	
invert:
	javac -cp hadoop-core-1.2.1.jar invert.java 
	jar cvfm invert.jar manifest2.txt *.class
	
scores:
	javac -cp hadoop-core-1.2.1.jar scores.java 
	jar cvfm scores.jar manifest3.txt *.class

get:
	hadoop fs -get /user/bsugiart/test/output output

clean:
	rm -r output
	hadoop fs -rm -r /user/bsugiart/test/output
	rm *.class

show:
	cat output/part-r-00000 
	
pull:
	git pull
	
push:
	git add *
	git commit -m "auto push"
	git push
