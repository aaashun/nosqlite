#include "nosqlite.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/time.h>

int
main(int argc, char **argv)
{
    int rv;
    struct nosqlite *db = NULL;

    FILE *file = NULL;
    int i, n = 300000;
    char (*keys)[128] = NULL, *p = NULL;
    char value[65535];
    int klen, vlen;

    struct timeval starttime, endtime;

    if (argc <=2 ) {
        fprintf(stderr, "usage: %s keyset nosqlitedb\n", argv[0]);
        goto end;
    }

    keys = (char (*)[128])malloc(128*n);
    file = fopen(argv[1], "r");
    if (!file) {
        fprintf(stderr, "failed to open: %s\n", argv[1]);
        goto end;
    }

    for (i = 0; i < n; ++i) {
        p = fgets(keys[i], sizeof(keys[i]), file);
        if (!p) {
            break;
        }

        p = strchr(keys[i], '\n');
        if (p) {
            *p = '\0';
        }
    }

    n = i;

    gettimeofday(&starttime, NULL);
    db = nosqlite_open(argv[2], 100000);
    if (!db) {
        fprintf(stderr, "failed to open: %s\n", argv[2]);
        goto end;
    }
    gettimeofday(&endtime, NULL);
    printf("load: %dms\n", (int)((endtime.tv_sec - starttime.tv_sec) * 1000 + (endtime.tv_usec - starttime.tv_usec) / 1000));

    gettimeofday(&starttime, NULL);
    for (i = 0; i < n; ++i) {
        klen = (int)strlen(keys[i]);
        vlen = (int)sizeof(value);
        rv = nosqlite_get(db, keys[i], klen, value, &vlen);
        if (rv) {
            fprintf(stderr, "%s not found\n", keys[i]);
            goto end;
        }
        /* printf("%.*s\n", vlen, value); */
    }
    gettimeofday(&endtime, NULL);
    printf("qps: %f\n", n/((double)(endtime.tv_sec - starttime.tv_sec) + (endtime.tv_usec - starttime.tv_usec) / 1000000.0));

end:
    if (file) {
        fclose(file);
    }

    if (db) {
        nosqlite_close(db);
    }

    return 0;
}
