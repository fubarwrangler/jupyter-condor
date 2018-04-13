#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <grp.h>

int main(int argc, char *argv[])
{
	int errval;
	gid_t gid;
    char *groupname;
	size_t buflen = 1024;
	struct group gr;
	struct group *grptr;
	char *buf = NULL;

    if(argc == 2)
        groupname = argv[1];
    else
        exit(2);


	while(1) {
		buf = realloc(buf, buflen);
        if(buf == NULL) {
            perror("Error realloc for buf\n");
            exit(1);
        }
		errno = 0;
		errval = getgrnam_r(groupname, &gr, buf, buflen, &grptr);
		if(grptr != NULL)   {
			gid = grptr->gr_gid;
		} else if (errval == 0) {
			printf("required-group %s not found\n", groupname);
            exit(1);
		} else if (errval == ERANGE)	{
			buflen *= 2;
			continue;
		} else {
		    printf("Error looking up group %s: %s",
				 groupname, strerror(errval));
            exit(1);
		}
		break;
	}
    if(grptr != NULL)   {
        char **p = grptr->gr_mem;
        printf("Group %s (%d) found, members:\n", grptr->gr_name,grptr->gr_gid);
        while(*p)   {
            puts(*p);
            p++;
        }
    }

	free(buf);
	return 0;
}
