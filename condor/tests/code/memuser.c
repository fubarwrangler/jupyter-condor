/****************************************************************************
 * Use up to a configured RAM amount, and take time getting there in steps
 ***************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <stdbool.h>
#include <signal.h>
#include <math.h>
#include <sys/time.h>


double total_time = -1.0;
size_t total_ram = (1 << 30);
bool keep_going = true;
int chunks = 5;
int steps = 5;
int delay = 3;

int stop_count = 0;

/* If sigint, set done=1 and break out of main loop cleanly */
static void sigint_handler(int signum)
{
	if(signum == SIGVTALRM || signum == SIGALRM)	{
		fprintf(stderr, "Timer expired, exit now\n");
		exit(0);
	} else if(signum == SIGINT || signum == SIGTERM) {
		if(stop_count > 0)
			exit(1);
		keep_going = false;
		fprintf(stderr, "Signal caught, terminating\n");
		stop_count++;
	}
}

/* Setup signal handler */
static void setup_signals(void)
{
	struct sigaction new_action;
	sigset_t self;

	sigemptyset(&self);
	sigaddset(&self, SIGALRM);
	sigaddset(&self, SIGINT);
	sigaddset(&self, SIGTERM);
	new_action.sa_handler = sigint_handler;
	new_action.sa_mask = self;
	new_action.sa_flags = 0;

	sigaction(SIGINT, &new_action, NULL);
	sigaction(SIGTERM, &new_action, NULL);
	sigaction(SIGALRM, &new_action, NULL);
}


unsigned long int ipow(unsigned int a, unsigned int b)
{
	unsigned long int ans = 1;

	while(b--)
		ans *= (unsigned long int)a;

	return ans;
}

/* Get a positive integer out of the current optarg (option @c) or exit */
static unsigned long int parse_num(int c)
{
	char *p;
	long int n;
	unsigned long int mult;

	errno = 0;
	n = strtol(optarg, &p, 10);
	if(errno != 0 || n < 0)	{
		fprintf(stderr, "Error -%c requires a positive integer\n", c);
		exit(EXIT_FAILURE);
	}

	switch(*p)	{
		case '\0':
			return n;
		case 'K':
			mult = ipow(10, 3); break;
		case 'k':
			mult = ipow(2, 10); break;
		case 'M':
			mult = ipow(10, 6); break;
		case 'm':
			mult = ipow(2, 20); break;
		case 'G':
			mult = ipow(10, 9); break;
		case 'g':
			mult = ipow(2, 30); break;
			break;
		default:
			fprintf(stderr,
				"Invalid multiplier character found for -%c: %c, "
				"must be K/k/M/m/G/g\n", c, *p);
			exit(EXIT_FAILURE);
	}

	if(*(p + 1) == '\0' || (*(p + 1) == 'b' && *(p + 2) == '\0'))
		return n * mult;

	fprintf(stderr, "Invalid character found after multiplier for"
		" -%c: %c\n", c, *(p+1));

	exit(EXIT_FAILURE);
}

static double parse_dbl(int c)
{
	char *p;
	double d;

	errno = 0;
	d = strtod(optarg, &p);
	if(errno != 0 || d < 0)	{
		fprintf(stderr, "Error -%c requires a (possibly float) num of seconds\n", c);
		exit(EXIT_FAILURE);
	}
	return d;
}

/* Set the configuration options above from cmdline */
static void initialize_options(int argc, char *argv[])
{
	int c;

	while((c=getopt(argc, argv, "+hd:s:m:t:")) != -1)	{
		switch(c)	{
			case 'd':
				delay = parse_num(c);
				break;
			case 's':
				chunks = parse_num(c);
				break;
			case 'm':
				total_ram = parse_num(c);
				break;
			case 't':
				total_time = parse_dbl(c);
				break;
			case 'h':
				fprintf(stderr,
"Usage: %s [OPTION] [DESTINATION]\n\
  Options:\n\
    -s  steps to reach usage (defaults 5)\n\
    -d  delay between step (defaults 3s)\n\
    -m  memory usage to reach to (defaults to 1Gb)\n\
    -t  how long to run in seconds after reaching max memory, default forever\n\
  Notes:\n\
    Integer values can be postfixed with a multiplier, one of the\n\
    following letters:\n\
      k/K m/M g/G\n\
    for kilo, mega, or giga-byte. The lower-case versions return the power\n\
    of two nearest (1k = 1024), and the upper-case returns an exact power of\n\
    ten (1K = 1000).\n\
", argv[0]);
				exit(EXIT_SUCCESS);
			case '?':
				if(strchr("nt", optopt) == NULL)
					fprintf(stderr,
						"Unknown option -%c encountered\n", optopt);
				else
					fprintf(stderr,
						"Option -%c requires an argument\n", optopt);
				exit(EXIT_FAILURE);
			default:
				abort();
		}
	}
}

static void set_timer(double secs)
{
	struct itimerval newt;
	struct timeval t;

	t.tv_sec = floor(secs);
	t.tv_usec = (secs - floor(secs)) * pow(10, 6);
	newt.it_interval = t;
	newt.it_value = t;

	printf("Set timer for %.2fs\n", secs);

	setitimer(ITIMER_REAL, &newt, NULL);
}

int main(int argc, char *argv[])
{
	unsigned char **buf = NULL;
	unsigned char **bufs = NULL;
	size_t each_chunk;
	int i, ctr;
	void *nullbytes;

	initialize_options(argc, argv);


	setup_signals();

	printf("Ram: %ld in %d steps, delay of %d\n", total_ram, chunks, delay);

	bufs = calloc((chunks + 2), sizeof(unsigned char *));
	if(bufs == NULL)	{
		fprintf(stderr, "Error allocating RAM for chunks!?\n");
		return EXIT_FAILURE;
	}

	each_chunk = total_ram / chunks;

	ctr = 0;
	buf = bufs;
	while(keep_going)	{
		if(ctr < chunks) {
			if((*buf = malloc(each_chunk)) == NULL)	{
				printf("Error allocating chunk\n");
				return EXIT_FAILURE;
			}
			memset(*buf, 0xaf, each_chunk);
			printf("Allocating %ld bytes (/ %ld)\n", (ctr+1) * each_chunk, total_ram);
			*buf++; ctr++;
		}
		if (ctr == chunks && total_time > 0.0) {
			set_timer(total_time);
			ctr++;
		}
		sleep(delay);
	}
	return EXIT_SUCCESS;
}
/* vim: set ts=4 noexpandtab sw=4: */
