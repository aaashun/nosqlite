#ifndef NOSQLITE_H_
#define NOSQLITE_H_

#ifdef __cplusplus
extern "C" {
#endif

struct nosqlite;

struct nosqlite *nosqlite_open(const char *path, int capacity);
int nosqlite_get(struct nosqlite *db, const void *key, int klen, const void *value, int *vlen);
int nosqlite_set(struct nosqlite *db, const void *key, int klen, const void *value, int vlen);
int nosqlite_remove(struct nosqlite *db, const void *key, int klen);
int nosqlite_close(struct nosqlite *db);

#ifdef __cplusplus
}
#endif
#endif
