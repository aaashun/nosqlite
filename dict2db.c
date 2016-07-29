#include "nosqlite.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/time.h>

#define MAX_VLEN 512

int
main(int argc, char **argv)
{
    int rv;
    struct nosqlite *db = NULL;

    FILE *file = NULL;
    int i = 0;
    char line[MAX_VLEN], *p = NULL, *k = NULL, *v = NULL;
    int klen, vlen;

    if (argc <=2 ) {
        fprintf(stderr, "usage: %s sample.dict sample.db (the default max line length is %dB)\n", argv[0], MAX_VLEN);
        goto end;
    }

    file = fopen(argv[2], "r");
    if (file) {
        fprintf(stderr, "error: %s exists, please remove it manuallay\n", argv[2]);
        goto end;
    }

    file = fopen(argv[1], "r");
    if (!file) {
        fprintf(stderr, "failed to open: %s\n", argv[1]);
        goto end;
    }

    db = nosqlite_open(argv[2], 300000);
    if (!db) {
        fprintf(stderr, "failed to open: %s\n", argv[2]);
        goto end;
    }

    for (i = 0; ;i++) {
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

        rv = nosqlite_set(db, k, strlen(k), v, strlen(v));
        if (rv == -1) {
            goto end;
        }
    }

    printf("convert %s to %s ok\n", argv[1], argv[2]);
    printf("please do run './benchmark %s %s N 1' to check it, and select a right capacity value(N)\n", argv[1], argv[2]);

end:
    if (file) {
        fclose(file);
    }

    if (db) {
        nosqlite_close(db);
    }

    return 0;
}
