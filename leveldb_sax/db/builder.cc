// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"

#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"



namespace leveldb {

Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, MemTable* mem, FileMetaData* meta) {
  Status s;
  meta->file_size = 0;


  std::string fname = TableFileName(dbname, meta->number);

  WritableFile* file;
  s = env->NewWritableFile(fname, &file);
  if (!s.ok()) {
    return s;
  }

  TableBuilder* builder = new TableBuilder(options, file);
  //要改的
  meta->smallest.Set(Slice((char*)mem->Getlsaxt().asaxt, sizeof(saxt_only)), 0,
                     static_cast<ValueType>(0));
  meta->largest.Set(Slice((char*)mem->Getrsaxt().asaxt, sizeof(saxt_only)), 0,
                     static_cast<ValueType>(0));
  meta->startTime = mem->startTime;
  meta->endTime = mem->endTime;
  builder->Add(mem);


  // Finish and check for builder errors
  s = builder->Finish();
  if (s.ok()) {
    meta->file_size = builder->FileSize();
    assert(meta->file_size > 0);
  }
  delete builder;

  // Finish and check for file errors
  if (s.ok()) {
    s = file->Sync();
  }
  if (s.ok()) {
    s = file->Close();
  }
  delete file;
  file = nullptr;

  //这里是验证，写完查再说
//  if (s.ok()) {
//    // Verify that the table is usable
//    out("===============");
//    out("验证：");
//    out(meta->number);
//    out(meta->file_size);
//    Cache::Handle* handle = nullptr;
//    s = table_cache->FindTable(meta->number, meta->file_size, &handle);
//    if (!s.ok()) exit(1);
//  }


  // Check for input iterator errors
//  if (!iter->status().ok()) {
//    s = iter->status();
//  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->RemoveFile(fname);
  }
  return s;
}

}  // namespace leveldb
