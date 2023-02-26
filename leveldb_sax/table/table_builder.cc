// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct TableBuilder::Rep {
  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        file(f),
        offset(0),
        data_block(&options),
//        num_entries(0),
        closed(false) {

  }
  Options options;
  WritableFile* file;
  uint64_t offset;
  Status status;
  BlockBuilder data_block;
//  int64_t num_entries;
  bool closed;  // Either Finish() or Abandon() has been called.


  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.

  STpos pending_handle;
  std::string compressed_output;
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  snap_add_finish.store(0, memory_order_release);
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  return Status::OK();
}


#if qiehuan
void TableBuilder::AddLeaf(NonLeafKey* nonLeafKey, void* tocopy) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  r->data_block.AddLeaf(nonLeafKey, tocopy);
  Addsnap(nonLeafKey);
}
#else
void TableBuilder::AddLeaf(NonLeafKey* nonLeafKey, void* tocopy, size_t size_tocopy) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  r->data_block.AddLeaf_(tocopy, size_tocopy);
//  r->data_block.AddLeaf(nonLeafKey);
  Addsnap(nonLeafKey);
}
#endif

void TableBuilder::AddNonLeaf(NonLeafKey* nonLeafKey, bool isleaf) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  r->data_block.AddNonLeaf(nonLeafKey, isleaf);
  Addsnap(nonLeafKey);
}

void TableBuilder::Addsnap(NonLeafKey* nonLeafKey) {
  Rep* r = rep_;
  Flush();
  nonLeafKey->p = r->pending_handle.Get();
  snap_add_finish.fetch_add(1, memory_order_release);
}


void TableBuilder::AddRootKey(NonLeafKey* nonLeafKey) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  //一个sstable的起点,就是
  std::string root_str;
  root_str.append((char*)nonLeafKey, sizeof(NonLeafKey));
  r->status = r->file->Append(Slice(root_str));
  if (r->status.ok()) {
    //flush剩余的
    r->file->Flush();
    r->offset += root_str.size();
  }
}

void TableBuilder::Add(MemTable* mem) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;

  NonLeaf* root = mem->table_.root;
  Add_dfs(root);
  if (ok()) {
    //一个sstable的起点,就是
    std::string root_str;
//      out("rootKey");
//      out(r->pending_handle.GetSize());
//      out(r->pending_handle.GetOffset());
    NonLeafKey rootKey(root->num, root->co_d, root->lsaxt, root->rsaxt, r->pending_handle.Get());
    root_str.append((char*)&rootKey, sizeof(NonLeafKey));
    r->status = r->file->Append(Slice(root_str));
    if (r->status.ok()) {
      //flush剩余的
      r->file->Flush();
      r->offset += root_str.size();
    }
  }

  //num_entries没用
//  if (r->num_entries > 0) {
//    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
//  }

  // 表示一个block开头
//  if (r->pending_index_entry) {
//    assert(r->data_block.empty());
//    r->options.comparator->FindShortestSeparator(&r->last_key, key);
//    std::string handle_encoding;
//    r->pending_handle.EncodeTo(&handle_encoding);
//    r->index_block.Add(r->last_key, Slice(handle_encoding));
//    r->pending_index_entry = false;
//  }
//
//  if (r->filter_block != nullptr) {
//    r->filter_block->AddKey(key);
//  }
//
//  r->last_key.assign(key.data(), key.size());
//  r->num_entries++;
//  r->data_block.Add(key, value);
//
//  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
//  //一个block4k，根本存不了几个结点。
//  if (estimated_block_size >= r->options.block_size) {
//    Flush();
//  }
}

void TableBuilder::Add_dfs(NonLeaf* nonLeaf) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;

  //地址从内存改成了偏移量
  vector<void*> new_p;
  new_p.reserve(nonLeaf->num);

  if (nonLeaf->isleaf) {
    Leaf* tmpleaf = (Leaf*)malloc(sizeof(Leaf)*nonLeaf->num);
    for(int i=0;i<nonLeaf->num;i++){
      Leaf* aleaf = (Leaf*)nonLeaf->nonLeafKeys[i].p;
      if (aleaf->num>0) {
        LeafKey* copyleaf = (LeafKey*)(tmpleaf + i);
        aleaf->sort(copyleaf);
        r->data_block.Add(aleaf, copyleaf);
        //取消了手动flush，改成每4kb写入了
        Flush();
      }
//      out("handle");
//          out(r->pending_handle.GetSize());
//          out(r->pending_handle.GetOffset());
      new_p.push_back(r->pending_handle.Get());

    }
    free(tmpleaf);
  } else {
    for(int i=0;i<nonLeaf->num;i++) {
      NonLeaf* anonLeaf = (NonLeaf*)nonLeaf->nonLeafKeys[i].p;
      Add_dfs(anonLeaf);
//      out("handle");
//      out(r->pending_handle.GetSize());
//      out(r->pending_handle.GetOffset());
      new_p.push_back(r->pending_handle.Get());
    }
  }
  //存这个结点
//  out("jin");
  r->data_block.Add(nonLeaf, new_p);
  Flush();
}

void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  WriteBlock(&r->data_block, &r->pending_handle);
//  if (ok()) {
//    r->status = r->file->Flush();
//  }
}

//外压缩
void TableBuilder::WriteBlock(BlockBuilder* block, STpos* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  Slice raw = block->Finish();
  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  block->Reset();
}

//写入文件和5字节
void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type, STpos* handle) {
  Rep* r = rep_;
  handle->Set(block_contents.size(), r->offset);
//  out("yaru");
//  out(r->offset);
//  out(block_contents.size());
//  out(handle->GetOffset());
//  out(handle->GetSize());
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    //crc
//    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
//    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
//    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::status() const { return rep_->status; }

Status TableBuilder::Finish() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

//uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder::FileSize() const { return rep_->offset; }



}  // namespace leveldb
