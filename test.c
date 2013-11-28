#include "nosqlite.h"

#include <stdio.h>

int
main(int argc, char **argv)
{
    int rv;
    char value[128];
    int vlen;

    struct nosqlite *db = NULL;

    db = nosqlite_open("test.db", 10);

    if (!db) {
        fprintf(stderr, "failed to open test.db\n");
        goto end;
    }

    nosqlite_set(db, "key1", 4, "value1", 6);
    nosqlite_set(db, "key2", 4, "value2", 6);
    nosqlite_set(db, "key3", 4, "value3", 6);

    vlen = (int)sizeof(value);
    rv = nosqlite_get(db, "key1", 4, value, &vlen);
    if (rv) {
        fprintf(stderr, "key1 not found\n");
    } else {
        printf("key1: %.*s\n", vlen, value);
    }

    rv = nosqlite_remove(db, "key1", 4);
    if (rv) {
        fprintf(stderr, "failed to remove key1\n");
    } else {
        printf("removed key1\n");
    }

    vlen = (int)sizeof(value);
    rv = nosqlite_get(db, "key1", 4, value, &vlen);
    if (rv) {
        printf("key1 is removed, can not be found\n");
    } else {
        fprintf(stdout, "key1 is not removed, can be found, key1: %.*s\n", vlen, value);
    }

end:
    if (db) {
        nosqlite_close(db);
    }
    return 0;
}
