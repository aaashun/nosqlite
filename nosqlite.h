#ifndef NOSQLITE_H_
#define NOSQLITE_H_

#define NOSQLITE_VERSION "nosqlite 0.2"
#ifndef NOSQLITE_VINT
#   define NOSQLITE_VINT unsigned short  /* max value size 64KB, 2^16 - 1 */
/* #define NOSQLITE_VINT unsigned int */ /* max value size 2GB, 2^31 - 1 */
#endif

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
