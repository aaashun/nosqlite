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
};


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

/*
static unsigned int
_hash3(const unsigned char *key, int klen)
{
    unsigned int hash = 0;
    while (klen--) {
        hash = ((hash << 5) ^ (hash >> 27)) ^ *key++;
    }
    return hash;
}
*/


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
_append(struct nosqlite *db, const unsigned char *key, unsigned char klen, unsigned int pos)
{
    unsigned int index, hash2;
    struct node *newnode, *node;

    index = _hash(key, klen) % db->capacity;
    hash2 = _hash2(key, klen);

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
    unsigned short vlen;
    unsigned int pos, size;

    if (capacity <= 0) {
        capacity = 1000;
    }

    db = (struct nosqlite *)calloc(1, sizeof(struct nosqlite) + sizeof(struct node) * capacity);
    db->capacity = capacity;
    db->nodes = (struct node *)((char *)db + sizeof(struct nosqlite));

    db->file = fopen(path, "a"); /* create if not exist */
    if (db->file) {
        fclose(db->file);
    }

    db->file = fopen(path, "rb+"); /* open for random read/write */
    if (!db->file) {
        fprintf(stderr, "failed to open: %s", path);
        goto end;
    }

    while (1) {
        pos = (unsigned int)ftell(db->file);

        size = (unsigned int)fread(&klen, 1, 1, db->file);
        if (size == 0) { /* eof */
            break;
        }

        if (klen > 127) { /* skip erased data */
            fseek(db->file, klen - 128, SEEK_CUR);
            fread(&vlen, 1, 2, db->file);
            fseek(db->file, vlen, SEEK_CUR);
            continue;
        }

        size = (unsigned int)fread(&key, 1, (size_t)klen, db->file);
        if (size != (size_t)klen) {
            fprintf(stderr, "failed to read key\n");
            goto end;
        }

        _append(db, key, klen, pos);

        size = (int)fread(&vlen, 1, 2, db->file);
        if (size != 2) {
            fprintf(stderr, "failed to read vlen\n");
            goto end;
        }

        fseek(db->file, vlen, SEEK_CUR);
    }
    rv = 0;

end:
    if (rv && db) {
        nosqlite_close(db);
        db = NULL;
    }
    return db;
}


int
nosqlite_set(struct nosqlite *db, const unsigned char *key, unsigned char klen, const unsigned char *value, unsigned short vlen)
{
    int rv = -1;
    unsigned int size = 0, pos;

    nosqlite_remove(db, key, klen);

    fseek(db->file, 0, SEEK_END);
    pos = (unsigned int)ftell(db->file);
    size += (unsigned int)fwrite(&klen, 1, 1, db->file);
    size += fwrite(key, 1, klen, db->file);
    size += fwrite(&vlen, 1, 2, db->file);
    size += fwrite(value, 1, vlen, db->file);

    if (size != (1 + klen + 2 + vlen)) {
        fprintf(stderr, "failed to write\n");
        goto end;
    }

    _append(db, key, klen, pos);

    rv = 0;
end:
    return rv;
}


/*
 * -1: normal error
 * -2: need larger buffer for value
 */
int
nosqlite_get(struct nosqlite *db, const unsigned char *key, unsigned char klen, const unsigned char *value, unsigned short *vlen)
{
    int rv = -1, size;

    unsigned int index, hash2;

    struct node *node;

    index = _hash(key, klen) % db->capacity;
    hash2 = _hash2(key, klen);

    for (node = &db->nodes[index]; node; node = node->next) {
        if (node->hash2 == hash2) {
            unsigned char klen2;
            unsigned short vlen2;

            fseek(db->file, node->pos, SEEK_SET);

            size = 0;
            size += (unsigned int)fread(&klen2, 1, 1, db->file);
            fseek(db->file, klen2, SEEK_CUR);
            size += (unsigned int)fread(&vlen2, 1, 2, db->file);

            if (size != 3) {
                fprintf(stderr, "failed to read klen or vlen while get\n");
                break;
            }

            if (*vlen < vlen2) {
                rv = -2;
            } else {
                rv = 0;
            }

            *vlen = *vlen >= vlen2 ? vlen2 : *vlen;

            size = fread((void *)value, 1, *vlen, db->file);
            if (size != *vlen) {
                rv = -1;
                fprintf(stderr, "failed to read value while get\n");
                break;
            }

            break;
        }
    }

    return rv;
}


int
nosqlite_remove(struct nosqlite *db, const unsigned char *key, unsigned char klen)
{
    int rv = -1;
    unsigned int index, hash2;

    struct node *node, *prenode;

    index = _hash(key, klen) % db->capacity;
    hash2 = _hash2(key, klen);

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
