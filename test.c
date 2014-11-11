#include "nosqlite.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int
main(int argc, char **argv)
{
    int rv;
    char value[128];
    int vlen;

    struct nosqlite *db = NULL;

    remove("test.db");
    db = nosqlite_open("test.db", 10);

    if (!db) {
        printf("failed to open test.db\n");
        goto end;
    }

    printf("\ntestcase 001: set key1, key2, key3\n");
    rv = nosqlite_set(db, "key1", 4, "value1", 6);
    rv |= nosqlite_set(db, "key2", 4, "value2", 6);
    rv |= nosqlite_set(db, "key3", 4, "value3", 6);
    if (rv) {
        printf("\tfail - failed to set keys\n");
    } else {
        printf("\tpass - set key1, key2, key3 ok\n");
    }


    printf("\ntestcase 002: get key1\n");
    vlen = (int)sizeof(value);
    rv = nosqlite_get(db, "key1", 4, value, &vlen);
    if (!rv && vlen == 6 && !strncmp(value, "value1", 6)) {
        printf("\tpass - key1: %.*s\n", vlen, value);
    } else {
        printf("\tfail - key1 not found\n");
    }

    printf("\ntestcase 003: remove key1\n");
    rv = nosqlite_remove(db, "key1", 4);
    if (rv) {
        printf("\tfail - failed to remove key1\n");
    } else {
        printf("\tpass - removed key1\n");
    }

    printf("\ntestcase 004: find removed key1\n");
    vlen = (int)sizeof(value);
    rv = nosqlite_get(db, "key1", 4, value, &vlen);
    if (rv) {
        printf("\tpass - key1 is removed, can not be found\n");
    } else {
        printf("\tfail - key1 is not removed, can be found, key1: %.*s\n", vlen, value);
    }

    nosqlite_close(db);

    printf("\ntestcase 005: open db again\n");
    db = nosqlite_open("test.db", 10);
    if (db) {
        printf("\tpass - open\n");
    } else {
        printf("\tfail - can not open test.db again\n");
        goto end;
    }

    printf("\ntestcase 006: find key2 from reopened db\n");
    vlen = (int)sizeof(value);
    rv = nosqlite_get(db, "key2", 4, value, &vlen);
    if (!rv && vlen == 6 && !strncmp(value, "value2", 6)) {
        printf("\tpass - key2: %.*s\n", vlen, value);
    } else {
        printf("\tfail - key2 not found\n");
    }

    if (sizeof(NOSQLITE_VINT) == 4) {
        char *bigvalue;
        int   bigvlen, i;

        printf("\ntestcase 007: set key4 with 1MB value\n");

        bigvalue = (char *)malloc(1024000);
        for (i = 0; i < 1024000; i ++) {
            *(bigvalue + i) = (char)('0' + i % 10);
        }

        nosqlite_set(db, "key4", 4, bigvalue, 1024000);

        bigvlen = 1024000;
        rv = nosqlite_get(db, "key4", 4, bigvalue, &bigvlen);
        if (!rv && bigvlen == 1024000) {
            printf("\tpass - key4 is found with 1MB value\n");
        } else {
            printf("\tfail - key4 not found\n");
        }

        free(bigvalue);
    }

end:
    if (db) {
        nosqlite_close(db);
    }
    return 0;
}
