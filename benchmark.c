#include "nosqlite.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/time.h>

#define MAX_KLEN 32
#define MAX_VLEN 512

int
main(int argc, char **argv)
{
    int rv;
    struct nosqlite *db = NULL;

    FILE *file = NULL;
    int i, c, n, capacity, checkvalue;
    char (*keys)[MAX_KLEN] = NULL, *p = NULL, *k = NULL, *v = NULL;
    char (*values)[MAX_VLEN] = NULL, value[MAX_VLEN];
    char line[MAX_KLEN + MAX_VLEN];
    int klen, vlen;

    struct timeval starttime, endtime;

    if (argc <= 4) {
        fprintf(stderr, "usage: %s sample.dict sample.db capacity checkvalue\n", argv[0]);
        fprintf(stderr, "\n");
        fprintf(stderr, "    1. the default max key length is %dB\n", MAX_KLEN);
        fprintf(stderr, "    2. the default max value length is %dB\n", MAX_VLEN);
        fprintf(stderr, "    3. capacity: larger capacity need more memory, smaller capacity cause lower performance\n");
        fprintf(stderr, "    4. checkvalue: 0 or 1, means check value or not\n");
        goto end;
    }

    checkvalue = atoi(argv[4]);
    file = fopen(argv[1], "r");
    if (!file) {
        fprintf(stderr, "failed to open: %s\n", argv[1]);
        goto end;
    }

    for (n = 0; c != EOF;) {
        c = fgetc(file);
        if (c == '\n') {
            n++;
        }
    }

    keys = (char (*)[MAX_KLEN])malloc(MAX_KLEN*n);
    values = (char (*)[MAX_VLEN])malloc(MAX_VLEN*n);

    fseek(file, 0, SEEK_SET);
    for (i = 0; i < n; ++i) {
        p = fgets(line, sizeof(line), file);
        if (!p) {
            break;
        }
    
        p = strchr(line, '\r');
        if (p) {
            *p = '\0';
        }

        p = strchr(line, '\n');
        if (p) {
            *p = '\0';
        }

        p = strchr(line, '\t');
        if (!p) {
            p = strchr(line, ' ');
        }

        if (p == line) {
            fprintf(stderr, "error: line %d starts with whitespace\n", i);
            goto end;
        }

        if (!p) {
            fprintf(stderr, "error: line %d has no key value pair separated by whitespace\n", i);
            goto end;
        }

        k = line;
        *p = '\0';
        v = p + 1;

        while ((*v == ' ' || *v == '\t') && *v != '\0') {
            v++;
        }

        if (*v == '\0') {
            fprintf(stderr, "line %d has no value\n", i);
        }

        strcpy(keys[i], k);
        strcpy(values[i], v);
    }

    n = i;

    gettimeofday(&starttime, NULL);
    db = nosqlite_open(argv[2], atoi(argv[3]));
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

        if (checkvalue == 1) {
            value[vlen] = '\0';
            if (strncmp(values[i], value, vlen) != 0) {
                fprintf(stderr, "warning: line %d, key: %s, expected value: \"%s\", actually: \"%s\"\n", i, keys[i], values[i], value);
            }
        }
    }
    gettimeofday(&endtime, NULL);
    printf("memory: %.1fMB\n", ((double)(nosqlite_size(db)))/1024/1024);
    printf("qps: %.1f\n", n/((double)(endtime.tv_sec - starttime.tv_sec) + (endtime.tv_usec - starttime.tv_usec) / 1000000.0));

end:
    if (file) {
        fclose(file);
    }

    if (db) {
        nosqlite_close(db);
    }

    return 0;
}
