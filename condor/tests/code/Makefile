all: spin sleepjob sigjob timegap memuser

timegap: timegap.c
	$(CC) -Wall -O2 -march=core2 -o timegap timegap.c

sigjob: sigjob.c
	$(CC) -g -o sigjob sigjob.c

sleepjob: sleepjob.c
	$(CC) -g -o sleepjob -std=c99 -lm sleepjob.c

spin: spin.c rc4.c
	$(CC) -O2 -o spin -lm $^

memuser: memuser.c
	$(CC) -O2 -o memuser -lm $^

clean:
	rm -f spin sleepjob sigjob timegap memuser
