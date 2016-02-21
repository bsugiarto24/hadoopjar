
.PHONY: clean all dine test
	
all: dine

dine:
	gcc -Wall -g -lpthread -o dine dine.c

test:
	~pn-cs453/demos/tryAsgn3

clean:
	rm dine
