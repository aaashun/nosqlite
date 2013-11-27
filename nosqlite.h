#ifndef NOSQLITE_H_
#define NOSQLITE_H_

#ifdef __cplusplus
extern "C" {
#endif

struct nosqlite;

struct nosqlite *nosqlite_open(const char *path, int capacity);
int nosqlite_get(struct nosqlite *db, const unsigned char *key, unsigned char klen, const unsigned char *value, unsigned short *vlen);
int nosqlite_set(struct nosqlite *db, const unsigned char *key, unsigned char klen, const unsigned char *value, unsigned short vlen);
int nosqlite_remove(struct nosqlite *db, const unsigned char *key, unsigned char klen);
int nosqlite_close(struct nosqlite *db);

#ifdef __cplusplus
}
#endif
#endif
