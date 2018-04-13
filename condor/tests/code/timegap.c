/******************************************************************************
 * Stupid program to read a fixed timestamp from the condor logs and output
 * warnings when the gap between two consecutive lines is greater than a
 * given number of seconds.
 *
 * Should probably use a configurable strptime() parser, but we do it
 * ourselves via strtok()!!
 *
 * William Strecker-Kellogg <willsk@bnl.gov>
 *
 *
 * Usage: ./program FILE TIME
 *
 *****************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

long int get_int(const char *s)
{
	char *p;
	long int n;
	if(s == NULL)
		return -1;
	errno = 0;
	n = strtol(s, &p, 10);
	if(errno || n < 0 || *p != '\0')	{
		//fprintf(stderr, "invalid int found %d chars into %s\n", (p - s), p);
		return -1;
	}
	return (int)n;
}

int main(int argc, char *argv[])
{
	FILE *fp;
	char buf[1024];
	char clean[1024], last_line[1024];
	char *p;

    /* (last)hour, (last)minute, (last)second */
	int h, m, s;
	int lh = 0, lm = 0, ls = 0;

	int diff, bad;
    int lineno = 0;

	int first = 1;

	if(argc < 3)	{
		printf("Usage: %s FILE seconds\n", argv[0]);
		return 1;
	}

	if(strcmp(argv[1], "-") == 0)	{
		fp = stdin;
	} else if((fp = fopen(argv[1], "r")) == NULL)  {
		perror("Cannot open file");
		return 1;
	}

	bad = get_int(argv[2]);
	if(bad < 0)	{
		printf("Error, valid # needed for seconds\n");
		return 1;
	}

	/* condor line: "04/26/16 16:08:24"
	 *                       ^
	 *                       |
	 *   pos 9       --------/
	 */
	while(fgets(buf, sizeof(buf), fp) != NULL)   {
        lineno++;

		if(strlen(buf) < 9) {
			//fprintf(stderr, "Malformed line found: %s\n", buf);
			continue;
		}
		strcpy(clean, buf);

		p = buf + 9;

		p = strtok(p, ":");
		if((h = get_int(p)) < 0)	{ continue; }
		p = strtok(NULL, ":");
		if((m = get_int(p)) < 0)	{ continue; }
		p = strtok(NULL, " ");
		if((s = get_int(p)) < 0)	{ continue; }

		diff = (h - lh) * 3600  + (m - lm) * 60 + (s - ls);

		if(diff < 0)
			continue;
		if(diff > bad && !first)	{
			printf("Gap of %ds found at line %d:\n *** %s *** %s", diff, lineno, clean, last_line);
		}
        strcpy(last_line, clean);

		ls = s, lm = m, lh = h;
		first = 0;
	}


	fclose(fp);
	return 0;
}



