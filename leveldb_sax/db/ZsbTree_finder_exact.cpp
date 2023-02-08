//
// Created by hh on 2023/1/17.
//

#include "ZsbTree_finder_exact.h"


namespace leveldb {
void ZsbTree_finder_exact::start() {

  heap->Lock();
  need = heap->need();
  topdist = heap->top();
  heap->Unlock();
  nonLeaf_dfs(*root);

}

void ZsbTree_finder_exact::nonLeaf_dfs(NonLeaf& nonLeaf) {

  if (nonLeaf.isleaf) {
    NonLeafKey* nonLeafKeys = nonLeaf.nonLeafKeys;
    for(int i=0;i<nonLeaf.num;i++) {
      NonLeafKey& nonLeafKey = nonLeafKeys[i];
      if (need || !nonLeafKey.co_d || minidist_paa_to_saxt(aquery1.paa, nonLeafKey.lsaxt.asaxt + Bit_cardinality - nonLeafKey.co_d, nonLeafKey.co_d) <= topdist) {
        leaf_todo(*(Leaf*)nonLeafKey.p);
      }
    }
  } else {
    NonLeafKey* nonLeafKeys = nonLeaf.nonLeafKeys;
    for(int i=0;i<nonLeaf.num;i++) {
      NonLeafKey& nonLeafKey = nonLeafKeys[i];
      if (need || !nonLeafKey.co_d || minidist_paa_to_saxt(aquery1.paa, nonLeafKey.lsaxt.asaxt + Bit_cardinality - nonLeafKey.co_d, nonLeafKey.co_d) <= topdist) {
        nonLeaf_dfs(*(NonLeaf*)nonLeafKey.p);
      }
    }
  }
}

void ZsbTree_finder_exact::leaf_todo(Leaf& leaf) {
  // 每次查leaf前更新一下need和topdist
  if (leaf.num) {

    int res_size = 0;
    LeafKey* leafKeys = leaf.leafKeys;
    if (need) {
      //全看
      char* tmpinfo = add_info;
      charcpy(tmpinfo, &need, sizeof(int));
      charcpy(tmpinfo, &topdist, sizeof(float));
      res_size = leaf.num;
      if (!res_size) return;

#if iscount_saxt_for_exact
      saxt_num += res_size;
#endif

#if istime == 2
      char* sizeinfo = tmpinfo;
      tmpinfo += sizeof(int);
      int new_res_size = 0;
      for(int i=0;i<res_size;i++) {
        if (aquery1.rep.startTime <= leafKeys[i].keytime_ && leafKeys[i].keytime_ <= aquery1.rep.endTime){
          charcpy(tmpinfo, &leafKeys[i].p, sizeof(void*));
          new_res_size ++;
        }
      }
      res_size = new_res_size;
      charcpy(sizeinfo, &res_size, sizeof(int));
#else
      charcpy(tmpinfo, &res_size, sizeof(int));
      for(int i=0;i<res_size;i++) {
        charcpy(tmpinfo, leafKeys + i, sizeof(void*));
      }
#endif
      char* out;
      size_t out_size;
      find_tskey_exact(info, to_find_size_leafkey + res_size * sizeof(void*), out, out_size, gs_jvm);
      ares_exact* ares_out = (ares_exact*) out;
      
      heap->Lock();
      //push
      for (int j=0;j<out_size;j++) {
        if (!heap->push(ares_out[j])) break;
      }
      topdist = heap->top();
      need = heap->need();
      heap->Unlock();
      free(out);
    }
    else {

      int num = leaf.num;
#if iscount_saxt_for_exact
      saxt_num += num;
#endif
#if istime == 2
      for(int i=0;i<num;i++) {
        if (aquery1.rep.startTime <= leafKeys[i].keytime_ && leafKeys[i].keytime_ <= aquery1.rep.endTime) {
          float dist = minidist_paa_to_saxt(aquery1.paa, leafKeys[i].asaxt.asaxt, Bit_cardinality);
          if(dist <= topdist) tmp_distp[res_size++] = {dist, leafKeys[i].p};
        }
      }
#else
      for(int i=0;i<num;i++) {
        float dist = minidist_paa_to_saxt(aquery1.paa, leafKeys[i].asaxt.asaxt, Bit_cardinality);
        if(dist <= topdist) tmp_distp[res_size++] = {dist, leafKeys[i].p};
      }
#endif
      heap->Lock();
      for(int i=0;i<res_size;i++) {
        heap->to_sort_dist_p.push_back(tmp_distp[i]);
      }
      heap->Unlock();

    }

  }
}

void ZsbTree_finder_exact_appro::leaf_todo(Leaf& leaf) {

  // 每次查leaf前更新一下need和topdist
  if (leaf.num) {

    int res_size = 0;
    LeafKey* leafKeys = leaf.leafKeys;
    if (need) {
      //全看
      char* tmpinfo = add_info;
      charcpy(tmpinfo, &need, sizeof(int));
      charcpy(tmpinfo, &topdist, sizeof(float));
      res_size = leaf.num;
      if (!res_size) return;

#if iscount_saxt_for_exact
      saxt_num += res_size;
#endif

#if istime == 2
      char* sizeinfo = tmpinfo;
      tmpinfo += sizeof(int);
      int new_res_size = 0;
      for(int i=0;i<res_size;i++) {
        if (!heap->is_in_set(leafKeys[i].p) && aquery1.rep.startTime <= leafKeys[i].keytime_ && leafKeys[i].keytime_ <= aquery1.rep.endTime){
          charcpy(tmpinfo, &leafKeys[i].p, sizeof(void*));
          new_res_size ++;
        }
      }
#else
      char* sizeinfo = tmpinfo;
      tmpinfo += sizeof(int);
      int new_res_size = 0;
      for(int i=0;i<res_size;i++) {
        if (!heap->is_in_set(leafKeys[i].p)) {
          charcpy(tmpinfo, leafKeys + i, sizeof(void*));
          new_res_size++;
        }
      }
#endif
      if (!new_res_size) return;
      res_size = new_res_size;
      charcpy(sizeinfo, &res_size, sizeof(int));
      char* out;
      size_t out_size;
      find_tskey_exact(info, to_find_size_leafkey + res_size * sizeof(void*), out, out_size, gs_jvm);
      ares_exact* ares_out = (ares_exact*) out;

      heap->Lock();
      //push
      for (int j=0;j<out_size;j++) {
        if (!heap->push(ares_out[j])) break;
      }
      topdist = heap->top();
      need = heap->need();
      heap->Unlock();
      free(out);
    } else {


      int num = leaf.num;
#if iscount_saxt_for_exact
      saxt_num += num;
#endif
#if istime == 2
      for(int i=0;i<num;i++) {
        if (!heap->is_in_set(leafKeys[i].p) && aquery1.rep.startTime <= leafKeys[i].keytime_ && leafKeys[i].keytime_ <= aquery1.rep.endTime) {
          float dist = minidist_paa_to_saxt(aquery1.paa, leafKeys[i].asaxt.asaxt, Bit_cardinality);
          if(dist <= topdist) tmp_distp[res_size++] = {dist, leafKeys[i].p};
        }
      }
#else
      for(int i=0;i<num;i++) {
        if (!heap->is_in_set(leafKeys[i].p)) {
          float dist = minidist_paa_to_saxt(aquery1.paa, leafKeys[i].asaxt.asaxt, Bit_cardinality);
          if(dist <= topdist) tmp_distp[res_size++] = {dist, leafKeys[i].p};
        }
      }
#endif
      heap->Lock();
      for(int i=0;i<res_size;i++) {
        heap->to_sort_dist_p.push_back(tmp_distp[i]);
      }
      heap->Unlock();


    }

  }
}
}