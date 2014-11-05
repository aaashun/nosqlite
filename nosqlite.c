#include "nosqlite.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define NOSQLITE_PAGESIZE 65536

struct node {
    unsigned int hash2;
    unsigned int pos;
    struct node *next;
};

struct page { /* page size: 64k, node number: */
    struct node *nodes;
    struct page *next;
    unsigned short available;
};

struct nosqlite {
    FILE *file;
    int capacity;
    struct node *nodes;
    struct page *pages;
    struct node *freelist;
    unsigned w:1; /* writeable */
};


#define _le(i) ((sizeof(NOSQLITE_VINT) == 4) ? _le4(i) : _le2(i))

static unsigned short
_le2(unsigned short i) {
    static short d = 0x1122;
    return  *((char *)&d) == 0x22 ? i : (i << 8 & 0xff00) | (i >> 8 & 0x00ff);
}


static unsigned int
_le4(unsigned int i) {
    static short d = 0x1122;
    return  *((char *)&d) == 0x22 ? i : (i << 24 & 0xff000000)
        | (i << 8  & 0x00ff0000)       
        | (i >> 8  & 0x0000ff00)       
        | (i >> 24 & 0x000000ff);      
}


/* DJB hash */
static unsigned int
_hash(const unsigned char *key, int klen)
{
    unsigned int hash = 5381;
    while (klen--) {
        hash = ((hash << 5) + hash) + *key++;
    }
    return hash;
}


/* BKDR hash */
static unsigned int
_hash2(const unsigned char *key, int klen)
{
    unsigned int hash = 0;
    while (klen--) {  
        hash = (hash * 131) + *key++;
    }
    return hash;
}


static struct node *
_nalloc(struct nosqlite *db)
{
    struct node *node;
    struct page *page, *p;

    static unsigned short max_available = 0;
    if (max_available == 0) {
        max_available = (NOSQLITE_PAGESIZE - sizeof(struct page))/sizeof(struct node);
    }

    if (db->freelist) {
        node = db->freelist;
        db->freelist = node->next;
        return node;
    }

    if (db->pages == NULL || db->pages->available >= max_available) {
        page = (struct page *)malloc(NOSQLITE_PAGESIZE);
        page->next = NULL;
        page->available = 0;
        page->nodes = (struct node *)((char *)page + sizeof(struct page));
        p = db->pages;
        db->pages = page;
        page->next = p;
    } else {
        page = db->pages;
    }

    node = &page->nodes[page->available++];
    return node;
}


static void 
_nfree(struct nosqlite *db, struct node *node)
{
    struct node *p;
    p = db->freelist;
    db->freelist = node;
    node->next = p;
    return;
}


static int
_append(struct nosqlite *db, const void *key, int klen, unsigned int pos)
{
    unsigned int index, hash2;
    struct node *newnode, *node;

    index = _hash((const unsigned char *)key, klen) % db->capacity;
    hash2 = _hash2((const unsigned char *)key, klen);

    if (db->nodes[index].hash2 == 0 && db->nodes[index].pos == 0) {
        newnode = &db->nodes[index];
    } else {
        for (node = &db->nodes[index]; node->next; node = node->next) {
            continue;
        }
        newnode = _nalloc(db);
        node->next = newnode;
    }

    newnode->hash2 = hash2;
    newnode->pos = pos;
    newnode->next = NULL;

    return 0;
}


struct nosqlite *
nosqlite_open(const char *path, int capacity)
{
    int rv = -1;
    struct nosqlite *db;
    unsigned char klen, key[256];
    NOSQLITE_VINT vlen;
    unsigned int pos, size;

    if (capacity <= 0) {
        capacity = 1000;
    }

    db = (struct nosqlite *)calloc(1, sizeof(struct nosqlite) + sizeof(struct node) * capacity);
    db->capacity = capacity;
    db->nodes = (struct node *)((char *)db + sizeof(struct nosqlite));

    db->file = fopen(path, "rb");
    if (!db->file) {
        db->file = fopen(path, "a"); /* create it */
        if (db->file) {
            fwrite(NOSQLITE_VERSION, 1, 12, db->file);
            fclose(db->file);
        }
    } else {
        /* mode "r" for fopen has high performance for the following code */
        char version[12];
        size = (unsigned int)fread(version, 1, 12, db->file);
        if (size != 12 || strncmp(version, NOSQLITE_VERSION, 12)) { /* verify version */
            fprintf(stderr, "invalid %s db: %s\n", NOSQLITE_VERSION, path);
        } else {
            while (1) {
                pos = (unsigned int)ftell(db->file);
                size = (unsigned int)fread(&klen, 1, 1, db->file);
                if (size == 0) { /* eof */
                    rv = 0;
                    break;
                }

                if (klen > 127) { /* skip erased data */
                    fseek(db->file, klen - 128, SEEK_CUR);
                    size = (unsigned int)fread(&vlen, 1, sizeof(vlen), db->file);
                    if (size != 2) {
                        fprintf(stderr, "failed to read erased vlen\n");
                        break;
                    }

                    vlen = _le(vlen);
                    fseek(db->file, vlen, SEEK_CUR);
                    continue;
                }

                size = (unsigned int)fread(&key, 1, (size_t)klen, db->file);
                if (size != (unsigned int)klen) {
                    fprintf(stderr, "failed to read key\n");
                    break;
                }

                _append(db, key, klen, pos);

                size = (unsigned int)fread(&vlen, 1, 2, db->file);
                if (size != 2) {
                    fprintf(stderr, "failed to read vlen\n");
                    break;
                }

                vlen = _le(vlen);
                fseek(db->file, vlen, SEEK_CUR);
            }
        }

        fclose(db->file);
    }

    db->w = 1;
    db->file = fopen(path, "rb+"); /* open for random read/write */
    if (!db->file) {
        db->w = 0;
        db->file = fopen(path, "rb"); /* open for random read only */
    }

    if (!db->file) {
        fprintf(stderr, "failed to open: %s\n", path);
    } else {
        rv = 0;
    }


    if (rv && db) {
        nosqlite_close(db);
        db = NULL;
    }

    return db;
}


int
nosqlite_set(struct nosqlite *db, const void *key, int _klen, const void *value, int _vlen)
{
    int rv = -1;
    unsigned int size = 0, pos;

    unsigned char klen = (unsigned char)_klen;
    NOSQLITE_VINT vlen = (NOSQLITE_VINT)_vlen;

    if (sizeof(NOSQLITE_VINT) == 2 && _vlen > 65535) {
        fprintf(stderr, "too large value, the max is 65535\n");
    } else if (sizeof(NOSQLITE_VINT) == 4 && _vlen > 2147483647) {
        fprintf(stderr, "too large value, the max is 2147483647\n");
    } else if (_vlen < 0) {
        fprintf(stderr, "value length can not be negative");
    }

    if (!db->w) {
        fprintf(stderr, "this db is readonly\n");
        return -1;
    }

    nosqlite_remove(db, key, klen);

    fseek(db->file, 0, SEEK_END);
    pos = (unsigned int)ftell(db->file);
    size += (unsigned int)fwrite(&klen, 1, 1, db->file);
    size += (unsigned int)fwrite(key, 1, klen, db->file);

    vlen = _le(vlen);
    size += (unsigned int)fwrite(&vlen, 1, 2, db->file);
    vlen = _le(vlen);

    size += (unsigned int)fwrite(value, 1, vlen, db->file);
    if (size != (1 + klen + 2 + vlen)) {
        fprintf(stderr, "failed to write\n");
    } else {
        rv = _append(db, key, klen, pos);
    }

    return rv;
}


/*
 * -1: normal error
 * -2: need larger buffer for value
 */
int
nosqlite_get(struct nosqlite *db, const void *key, int _klen, const void *value, int *_vlen)
{
    int rv = -1;
    unsigned int size;

    struct node *node;
    unsigned char klen = (unsigned char)_klen;
    NOSQLITE_VINT vlen = (NOSQLITE_VINT)*_vlen;
    unsigned int index, hash2;

    index = _hash((const unsigned char *)key, klen) % db->capacity;
    hash2 = _hash2((const unsigned char *)key, klen);

    for (node = &db->nodes[index]; node; node = node->next) {
        if (node->hash2 == hash2) {
            fseek(db->file, node->pos, SEEK_SET);

            size = 0;
            size += (unsigned int)fread(&klen, 1, 1, db->file);
            fseek(db->file, klen, SEEK_CUR);
            size += (unsigned int)fread(&vlen, 1, 2, db->file);
            vlen = _le(vlen);

            if (size != 3) {
                fprintf(stderr, "failed to read klen or vlen while get\n");
            } else {
                if (vlen > (NOSQLITE_VINT)*_vlen) {
                    vlen = (NOSQLITE_VINT)*_vlen;
                    rv = -2;
                } else {
                    *_vlen = (int)vlen;
                    rv = 0;
                }

                size = (unsigned int)fread((void *)value, 1, vlen, db->file);
                if (size != (unsigned int)vlen) {
                    rv = -1;
                    fprintf(stderr, "failed to read value while get\n");
                }
            }

            break;
        }
    }

    return rv;
}


int
nosqlite_remove(struct nosqlite *db, const void *key, int _klen)
{
    int rv = -1;

    struct node *node, *prenode;
    unsigned char klen = (unsigned char)_klen;
    unsigned int index, hash2;

    if (!db->w) {
        fprintf(stderr, "this db is readonly\n");
        return -1;
    }

    index = _hash((const unsigned char *)key, klen) % db->capacity;
    hash2 = _hash2((const unsigned char *)key, klen);

    for (node = &db->nodes[index], prenode = NULL; node; prenode = node, node = node->next) {
        if (node->hash2 == hash2) {
            klen = klen + 128;

            fseek(db->file, node->pos, SEEK_SET);
            fwrite(&klen, 1, 1, db->file); /* set erase flag */

            if (!prenode) { /* node == db->nodes[index]) */
                memset(&db->nodes[index], 0, sizeof(struct node));
            } else {
                prenode->next = node->next;
                _nfree(db, node);
            }
            rv = 0;
        }
    }

    return rv;
}


int
nosqlite_close(struct nosqlite *db)
{
    struct page *page, *p;

    for (page = db->pages; page;) {
        p = page;
        page = page->next;
        free(p);
    }

    if (db->file) {
        fclose(db->file);
    }

    free(db);

    return 0;
}
