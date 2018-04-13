/* Needs to be compiled with -std=c99 */
#define _DEFAULT_SOURCE

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <time.h>
#include <math.h>
#include <sys/sysinfo.h>
#include <sys/time.h>

int sigterm_ctr = 0;

const char *timestamp(void)
{
	time_t tm;
	char *str;

	time(&tm);
	str = ctime(&tm);
	*(str + strlen(str) - 1) = '\0';
	return str;
}

static void sigterm_catcher(int sig)
{
	printf("%s: Caught signal (%d) (number %d)\n", 
			timestamp(), sig, sigterm_ctr++);
}

static float to_flt(char *arg)
{
	char *end;
	float ans;

	ans = strtof(arg, &end);
	if(ans < 0)	{
		return 0.0f;
	}
	return ans;
}
/*static void printbin(unsigned int data)
{
	printf("%d\n", data);
	for(int n = 32; n >= 0; n--)	{
		printf("%d", (data >> n) & 1);
	}
	printf("\n");
}*/

int main(int argc, char *argv[])
{
	struct sysinfo info;
	float tm, splay;
	float this;
	unsigned long data;
	struct timeval tv;

	if(argc < 3)	{
		fputs("Usage: sleepjob TIME SPLAY\n", stderr);
		exit(1);
	}

	tm = to_flt(argv[1]);
	splay = to_flt(argv[2]);
	if(splay >= tm * 2.0f)	{
		printf("Splay must be < 2xtime\n");
		exit(1);
	}

	signal(SIGTERM, sigterm_catcher);
	signal(SIGINT, sigterm_catcher);
	if(sysinfo(&info) != 0)	{
		perror("sysinfo");
		return 1;
	}


	gettimeofday(&tv, NULL);
#define rol32(x, n) ((x) << (n) | (x) >> (32 - (n)))
	data = info.totalram ^ info.loads[0] ^ info.procs ^ rol32(info.freeram, info.uptime % 24);
	data ^= (unsigned int)time(NULL) | rol32(getpid() * 3, info.freeram % 29); 

	srand(data ^ tv.tv_usec);
	this = (splay * (float)(rand() % RAND_MAX))/RAND_MAX - (splay / 2.0f);

	printf("%s: Sleep for %f\n", timestamp(), this + tm);

	usleep(lroundf((this + tm) * 1000000.0f));

	printf("%s: Done!\n", timestamp());

	return 0;
}
