#ifndef BTREE_H
#define BTREE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

struct btree;

struct btree *btree_new(size_t elsize, size_t max_items,
int (*compare)(const void *a, const void *b, void *udata), void *udata);

void btree_free(struct btree *btree);

void btree_clear(struct btree *btree);

bool btree_nomemory(const struct btree *btree);

size_t btree_height(const struct btree *btree);

size_t btree_count(const struct btree *btree);

struct btree *btree_copy(struct btree *btree);

const void *btree_set(struct btree *btree, const void *item);

const void *btree_delete(struct btree *btree, const void *key);

const void *btree_load(struct btree *btree, const void *item);

const void *btree_pop_min(struct btree *btree);

const void *btree_pop_max(struct btree *btree);

const void *btree_min(const struct btree *btree);

const void *btree_max(const struct btree *btree);

const void *btree_get(const struct btree *btree, const void *key);
