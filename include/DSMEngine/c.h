/* Copyright (c) 2011 The DSMEngine Authors. All rights reserved.
  Use of this source code is governed by a BSD-style license that can be
  found in the LICENSE file. See the AUTHORS file for names of contributors.

  C bindings for DSMEngine.  May be useful as a stable ABI that can be
  used by programs that keep DSMEngine in a shared library, or for
  a JNI api.

  Does not support:
  . getters for the option types
  . custom comparators that implement key shortening
  . custom iter, db, env, table_cache implementations using just the C bindings

  Some conventions:

  (1) We expose just opaque struct pointers and functions to clients.
  This allows us to change internal representations without having to
  recompile clients.

  (2) For simplicity, there is no equivalent to the Slice type.  Instead,
  the caller has to pass the pointer and length as separate
  arguments.

  (3) Errors are represented by a null-terminated c string.  NULL
  means no error.  All operations that can raise an error are passed
  a "char** errptr" as the last argument.  One of the following must
  be true on entry:
     *errptr == NULL
     *errptr points to a malloc()ed null-terminated error message
       (On Windows, *errptr must have been malloc()-ed by this library.)
  On success, a DSMEngine routine leaves *errptr unchanged.
  On failure, DSMEngine frees the old value of *errptr and
  set *errptr to a malloc()ed error message.

  (4) Bools have the type uint8_t (0 == false; rest == true)

  (5) All of the pointer arguments must be non-NULL.
*/

#ifndef STORAGE_DSMEngine_INCLUDE_C_H_
#define STORAGE_DSMEngine_INCLUDE_C_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>

#include "DSMEngine/export.h"

/* Exported types */

typedef struct DSMEngine_t DSMEngine_t;
typedef struct DSMEngine_cache_t DSMEngine_cache_t;
typedef struct DSMEngine_comparator_t DSMEngine_comparator_t;
typedef struct DSMEngine_env_t DSMEngine_env_t;
typedef struct DSMEngine_filelock_t DSMEngine_filelock_t;
typedef struct DSMEngine_filterpolicy_t DSMEngine_filterpolicy_t;
typedef struct DSMEngine_iterator_t DSMEngine_iterator_t;
typedef struct DSMEngine_logger_t DSMEngine_logger_t;
typedef struct DSMEngine_options_t DSMEngine_options_t;
typedef struct DSMEngine_randomfile_t DSMEngine_randomfile_t;
typedef struct DSMEngine_readoptions_t DSMEngine_readoptions_t;
typedef struct DSMEngine_seqfile_t DSMEngine_seqfile_t;
typedef struct DSMEngine_snapshot_t DSMEngine_snapshot_t;
typedef struct DSMEngine_writablefile_t DSMEngine_writablefile_t;
typedef struct DSMEngine_writebatch_t DSMEngine_writebatch_t;
typedef struct DSMEngine_writeoptions_t DSMEngine_writeoptions_t;

/* DB operations */

DSMEngine_EXPORT DSMEngine_t* DSMEngine_open(const DSMEngine_options_t* options,
                                       const char* name, char** errptr);

DSMEngine_EXPORT void DSMEngine_close(DSMEngine_t* db);

DSMEngine_EXPORT void DSMEngine_put(DSMEngine_t* db,
                                const DSMEngine_writeoptions_t* options,
                                const char* key, size_t keylen, const char* val,
                                size_t vallen, char** errptr);

DSMEngine_EXPORT void DSMEngine_delete(DSMEngine_t* db,
                                   const DSMEngine_writeoptions_t* options,
                                   const char* key, size_t keylen,
                                   char** errptr);

DSMEngine_EXPORT void DSMEngine_write(DSMEngine_t* db,
                                  const DSMEngine_writeoptions_t* options,
                                  DSMEngine_writebatch_t* batch, char** errptr);

/* Returns NULL if not found.  A malloc()ed array otherwise.
   Stores the length of the array in *vallen. */
DSMEngine_EXPORT char* DSMEngine_get(DSMEngine_t* db,
                                 const DSMEngine_readoptions_t* options,
                                 const char* key, size_t keylen, size_t* vallen,
                                 char** errptr);

DSMEngine_EXPORT DSMEngine_iterator_t* DSMEngine_create_iterator(
    DSMEngine_t* db, const DSMEngine_readoptions_t* options);

DSMEngine_EXPORT const DSMEngine_snapshot_t* DSMEngine_create_snapshot(DSMEngine_t* db);

DSMEngine_EXPORT void DSMEngine_release_snapshot(
    DSMEngine_t* db, const DSMEngine_snapshot_t* snapshot);

/* Returns NULL if property name is unknown.
   Else returns a pointer to a malloc()-ed null-terminated value. */
DSMEngine_EXPORT char* DSMEngine_property_value(DSMEngine_t* db,
                                            const char* propname);

DSMEngine_EXPORT void DSMEngine_approximate_sizes(
    DSMEngine_t* db, int num_ranges, const char* const* range_start_key,
    const size_t* range_start_key_len, const char* const* range_limit_key,
    const size_t* range_limit_key_len, uint64_t* sizes);

DSMEngine_EXPORT void DSMEngine_compact_range(DSMEngine_t* db, const char* start_key,
                                          size_t start_key_len,
                                          const char* limit_key,
                                          size_t limit_key_len);

/* Management operations */

DSMEngine_EXPORT void DSMEngine_destroy_db(const DSMEngine_options_t* options,
                                       const char* name, char** errptr);

DSMEngine_EXPORT void DSMEngine_repair_db(const DSMEngine_options_t* options,
                                      const char* name, char** errptr);

/* Iterator */

DSMEngine_EXPORT void DSMEngine_iter_destroy(DSMEngine_iterator_t*);
DSMEngine_EXPORT uint8_t DSMEngine_iter_valid(const DSMEngine_iterator_t*);
DSMEngine_EXPORT void DSMEngine_iter_seek_to_first(DSMEngine_iterator_t*);
DSMEngine_EXPORT void DSMEngine_iter_seek_to_last(DSMEngine_iterator_t*);
DSMEngine_EXPORT void DSMEngine_iter_seek(DSMEngine_iterator_t*, const char* k,
                                      size_t klen);
DSMEngine_EXPORT void DSMEngine_iter_next(DSMEngine_iterator_t*);
DSMEngine_EXPORT void DSMEngine_iter_prev(DSMEngine_iterator_t*);
DSMEngine_EXPORT const char* DSMEngine_iter_key(const DSMEngine_iterator_t*,
                                            size_t* klen);
DSMEngine_EXPORT const char* DSMEngine_iter_value(const DSMEngine_iterator_t*,
                                              size_t* vlen);
DSMEngine_EXPORT void DSMEngine_iter_get_error(const DSMEngine_iterator_t*,
                                           char** errptr);

/* Write batch */

DSMEngine_EXPORT DSMEngine_writebatch_t* DSMEngine_writebatch_create(void);
DSMEngine_EXPORT void DSMEngine_writebatch_destroy(DSMEngine_writebatch_t*);
DSMEngine_EXPORT void DSMEngine_writebatch_clear(DSMEngine_writebatch_t*);
DSMEngine_EXPORT void DSMEngine_writebatch_put(DSMEngine_writebatch_t*,
                                           const char* key, size_t klen,
                                           const char* val, size_t vlen);
DSMEngine_EXPORT void DSMEngine_writebatch_delete(DSMEngine_writebatch_t*,
                                              const char* key, size_t klen);
DSMEngine_EXPORT void DSMEngine_writebatch_iterate(
    const DSMEngine_writebatch_t*, void* state,
    void (*put)(void*, const char* k, size_t klen, const char* v, size_t vlen),
    void (*deleted)(void*, const char* k, size_t klen));
DSMEngine_EXPORT void DSMEngine_writebatch_append(
    DSMEngine_writebatch_t* destination, const DSMEngine_writebatch_t* source);

/* Options */

DSMEngine_EXPORT DSMEngine_options_t* DSMEngine_options_create(void);
DSMEngine_EXPORT void DSMEngine_options_destroy(DSMEngine_options_t*);
DSMEngine_EXPORT void DSMEngine_options_set_comparator(DSMEngine_options_t*,
                                                   DSMEngine_comparator_t*);
DSMEngine_EXPORT void DSMEngine_options_set_filter_policy(DSMEngine_options_t*,
                                                      DSMEngine_filterpolicy_t*);
DSMEngine_EXPORT void DSMEngine_options_set_create_if_missing(DSMEngine_options_t*,
                                                          uint8_t);
DSMEngine_EXPORT void DSMEngine_options_set_error_if_exists(DSMEngine_options_t*,
                                                        uint8_t);
DSMEngine_EXPORT void DSMEngine_options_set_paranoid_checks(DSMEngine_options_t*,
                                                        uint8_t);
DSMEngine_EXPORT void DSMEngine_options_set_env(DSMEngine_options_t*, DSMEngine_env_t*);
DSMEngine_EXPORT void DSMEngine_options_set_info_log(DSMEngine_options_t*,
                                                 DSMEngine_logger_t*);
DSMEngine_EXPORT void DSMEngine_options_set_write_buffer_size(DSMEngine_options_t*,
                                                          size_t);
DSMEngine_EXPORT void DSMEngine_options_set_max_open_files(DSMEngine_options_t*, int);
DSMEngine_EXPORT void DSMEngine_options_set_cache(DSMEngine_options_t*,
                                              DSMEngine_cache_t*);
DSMEngine_EXPORT void DSMEngine_options_set_block_size(DSMEngine_options_t*, size_t);
DSMEngine_EXPORT void DSMEngine_options_set_block_restart_interval(
    DSMEngine_options_t*, int);
DSMEngine_EXPORT void DSMEngine_options_set_max_file_size(DSMEngine_options_t*,
                                                      size_t);

enum { DSMEngine_no_compression = 0, DSMEngine_snappy_compression = 1 };
DSMEngine_EXPORT void DSMEngine_options_set_compression(DSMEngine_options_t*, int);

/* Comparator */

DSMEngine_EXPORT DSMEngine_comparator_t* DSMEngine_comparator_create(
    void* state, void (*destructor)(void*),
    int (*compare)(void*, const char* a, size_t alen, const char* b,
                   size_t blen),
    const char* (*name)(void*));
DSMEngine_EXPORT void DSMEngine_comparator_destroy(DSMEngine_comparator_t*);

/* Filter policy */

DSMEngine_EXPORT DSMEngine_filterpolicy_t* DSMEngine_filterpolicy_create(
    void* state, void (*destructor)(void*),
    char* (*create_filter)(void*, const char* const* key_array,
                           const size_t* key_length_array, int num_keys,
                           size_t* filter_length),
    uint8_t (*key_may_match)(void*, const char* key, size_t length,
                             const char* filter, size_t filter_length),
    const char* (*name)(void*));
DSMEngine_EXPORT void DSMEngine_filterpolicy_destroy(DSMEngine_filterpolicy_t*);

DSMEngine_EXPORT DSMEngine_filterpolicy_t* DSMEngine_filterpolicy_create_bloom(
    int bits_per_key);

/* Read options */

DSMEngine_EXPORT DSMEngine_readoptions_t* DSMEngine_readoptions_create(void);
DSMEngine_EXPORT void DSMEngine_readoptions_destroy(DSMEngine_readoptions_t*);
DSMEngine_EXPORT void DSMEngine_readoptions_set_verify_checksums(
    DSMEngine_readoptions_t*, uint8_t);
DSMEngine_EXPORT void DSMEngine_readoptions_set_fill_cache(DSMEngine_readoptions_t*,
                                                       uint8_t);
DSMEngine_EXPORT void DSMEngine_readoptions_set_snapshot(DSMEngine_readoptions_t*,
                                                     const DSMEngine_snapshot_t*);

/* Write options */

DSMEngine_EXPORT DSMEngine_writeoptions_t* DSMEngine_writeoptions_create(void);
DSMEngine_EXPORT void DSMEngine_writeoptions_destroy(DSMEngine_writeoptions_t*);
DSMEngine_EXPORT void DSMEngine_writeoptions_set_sync(DSMEngine_writeoptions_t*,
                                                  uint8_t);

/* Cache */

DSMEngine_EXPORT DSMEngine_cache_t* DSMEngine_cache_create_lru(size_t capacity);
DSMEngine_EXPORT void DSMEngine_cache_destroy(DSMEngine_cache_t* cache);

/* Env */

DSMEngine_EXPORT DSMEngine_env_t* DSMEngine_create_default_env(void);
DSMEngine_EXPORT void DSMEngine_env_destroy(DSMEngine_env_t*);

/* If not NULL, the returned buffer must be released using DSMEngine_free(). */
DSMEngine_EXPORT char* DSMEngine_env_get_test_directory(DSMEngine_env_t*);

/* Utility */

/* Calls free(ptr).
   REQUIRES: ptr was malloc()-ed and returned by one of the routines
   in this file.  Note that in certain cases (typically on Windows), you
   may need to call this routine instead of free(ptr) to dispose of
   malloc()-ed memory returned by this library. */
DSMEngine_EXPORT void DSMEngine_free(void* ptr);

/* Return the major version number for this release. */
DSMEngine_EXPORT int DSMEngine_major_version(void);

/* Return the minor version number for this release. */
DSMEngine_EXPORT int DSMEngine_minor_version(void);

#ifdef __cplusplus
} /* end extern "C" */
#endif

#endif /* STORAGE_DSMEngine_INCLUDE_C_H_ */
