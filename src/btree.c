#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stddef.h>
#include <stdint.h>

#include "btree.h"

static void *(*_btree_malloc)(size_t) = NULL;
static void (*_btree_free)(void *) = NULL;


