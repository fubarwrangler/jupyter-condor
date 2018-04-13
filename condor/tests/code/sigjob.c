#include <stdio.h>
#include <string.h>
#include <time.h>
#include <signal.h>
#include <unistd.h>

int sigterm_ctr = 0;
FILE *fp;
int done = 0;

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
	fprintf(fp, "%s: Caught signal (%d) (number %d)\n", 
			timestamp(), sig, sigterm_ctr++);
	done = 1;
}

char *fnames[] = { "output_parent", "output_child1", "output_child2" };

int main(int argc, char *argv[])
{
	int i = 0;

	signal(SIGTERM, sigterm_catcher);

	if(fork() == 0)	{
		i = 1;
	} else {
		if(fork() == 0)	{
			i = 2;
		}
	}

	if((fp = fopen(fnames[i], "w")) == NULL)	{
		fprintf(stderr, "Error opening %s for writing", argv[1]);
		return 4;
	}

	setlinebuf(fp);

	for(i = 0; i < 600 && !done;i++)	{
		sleep(2);
		fprintf(fp, "%s (%d): Hello %d\n", timestamp(), getpid(), i);
	}

	fprintf(stdout, "Exit normally");

	return 0;
}
