//
// Created by hh on 2022/12/11.
//

#include "zsbtree_finder.h"

void ZsbTree_finder::root_Get(NonLeaf &nonLeaf) {
  if (nonLeaf.isleaf) {
    to_find_nonleaf = &nonLeaf;
    return;
  }
  int pos = zsbtreee_insert::whereofKey(nonLeaf.lsaxt, nonLeaf.rsaxt, leafkey);
  if (pos==0) nonLeaf_Get(nonLeaf);
  else if (pos==-1) {
    l_Get_NonLeaf(nonLeaf.nonLeafKeys[0]);
  } else {
    r_Get_NonLeaf(nonLeaf.nonLeafKeys[nonLeaf.num-1]);
  }
}


void ZsbTree_finder::l_Get_NonLeaf(NonLeafKey &nonLeafKey) {
  NonLeaf &nonLeaf = *((NonLeaf *)nonLeafKey.p);
  if (nonLeaf.isleaf) {
    to_find_nonleaf = &nonLeaf;
    return;
  }
  l_Get_NonLeaf(nonLeaf.nonLeafKeys[0]);

//  if (isleaf) {
//    Leaf &leaf = *((Leaf *)nonLeafKey.p);
//    simi_leakKeys.reserve(leaf.num);
//    simi_leakKeys.resize(leaf.num);
//    memcpy(simi_leakKeys.data(), leaf.leafKeys, sizeof(LeafKey) * leaf.num);
//  } else {
//    NonLeaf &nonLeaf = *((NonLeaf *)nonLeafKey.p);
//    l_Get_NonLeaf(nonLeaf.nonLeafKeys[0]);
//  }
}

void ZsbTree_finder::r_Get_NonLeaf(NonLeafKey &nonLeafKey) {
  NonLeaf &nonLeaf = *((NonLeaf *)nonLeafKey.p);
  if (nonLeaf.isleaf) {
    to_find_nonleaf = &nonLeaf;
    return;
  }
  r_Get_NonLeaf(nonLeaf.nonLeafKeys[nonLeaf.num-1]);
}


void ZsbTree_finder::leaf_Get(Leaf *leaf, int id, LeafKey* res, int& res_num) {
  res_num = leaf->num;
#if istime == 2
  int newnum = 0;
  LeafKey* returnleafkeys = leaf->leafKeys;
  for(int i=0;i<res_num;i++) {
    ts_time thistime = returnleafkeys[i].keytime_;
    if (startTime <= thistime && thistime <= endTime) res[newnum++] = returnleafkeys[i];
  }
  res_num = newnum;
#else
  memcpy(res, leaf->leafKeys, sizeof(LeafKey) * res_num);
#endif
  oneId = id;

  // 根据id和cod,判断是否该查找左右两个点
#if ischaone
  return ;
#endif
  NonLeaf& nonLeaf = *to_find_nonleaf;
  if (id > 0) {
#if ischalr
    leaf_Get1((Leaf*)nonLeaf.nonLeafKeys[id - 1].p, res, res_num);
#else
    if (nonLeaf.nonLeafKeys[id - 1].co_d >= leaf->co_d)
      leaf_Get1((Leaf*)nonLeaf.nonLeafKeys[id - 1].p, res, res_num);
#endif
  }

  if (id + 1 < nonLeaf.num) {
#if ischalr
    leaf_Get1((Leaf*)nonLeaf.nonLeafKeys[id + 1].p, res, res_num);
#else
    if (nonLeaf.nonLeafKeys[id + 1].co_d >= leaf->co_d)
      leaf_Get1((Leaf*)nonLeaf.nonLeafKeys[id + 1].p, res, res_num);
#endif
  }

}

void ZsbTree_finder::leaf_Get1(Leaf *leaf, LeafKey* res, int& res_num) {
  int leafnum = leaf->num;
#if istime == 2
  int newnum = 0;
  LeafKey* returnleafkeys = leaf->leafKeys;
  for(int i=0;i<leafnum;i++) {
    ts_time thistime = returnleafkeys[i].keytime_;
    if (startTime <= thistime && thistime <= endTime) res[res_num + newnum++] = returnleafkeys[i];
  }
  res_num += newnum;
#else
  memcpy(res + res_num, leaf->leafKeys, sizeof(LeafKey) * leafnum);
  res_num += leafnum;
#endif
}


// 在nonLeaf的范围里, 才调用
void ZsbTree_finder::nonLeaf_Get(NonLeaf &nonLeaf) {
  if (nonLeaf.isleaf) {
    to_find_nonleaf = &nonLeaf;
    return;
  }
  int l=0;
  int r=nonLeaf.num-1;
  while (l<r) {
    int mid = (l + r) / 2;
    if (leafkey <= nonLeaf.nonLeafKeys[mid].rsaxt) r = mid;
    else l = mid + 1;
  }
  int pos = zsbtreee_insert::whereofKey(nonLeaf.nonLeafKeys[l].lsaxt, nonLeaf.nonLeafKeys[l].rsaxt, leafkey);
  if (pos==0) {
    //里面
    nonLeaf_Get(*(NonLeaf *)(nonLeaf.nonLeafKeys[l].p));
  } else if (pos==-1){
    //前面有 先比相聚度下降程度,再看数量,但目前没有在非叶节点记录这种东西，所以这里直接比相聚度大小
    cod preco_d = nonLeaf.nonLeafKeys[l-1].co_d;
    saxt_only prelsaxt = nonLeaf.nonLeafKeys[l-1].lsaxt;
    cod nextco_d = nonLeaf.nonLeafKeys[l].co_d;
    saxt_only nextrsaxt = nonLeaf.nonLeafKeys[l].rsaxt;
    cod co_d1 = get_co_d_from_saxt(prelsaxt, leafkey, nonLeaf.co_d);
    cod co_d2 = get_co_d_from_saxt(leafkey, nextrsaxt, nonLeaf.co_d);
    if ((preco_d - co_d1) < (nextco_d - co_d2)) {
      // 跟前面
      r_Get_NonLeaf(nonLeaf.nonLeafKeys[l-1]);
    } else if ((preco_d - co_d1) > (nextco_d - co_d2)) {
      //跟后面
      l_Get_NonLeaf(nonLeaf.nonLeafKeys[l]);
    } else {
      if (co_d1 <= co_d2) {
        r_Get_NonLeaf(nonLeaf.nonLeafKeys[l-1]);
      } else {
        l_Get_NonLeaf(nonLeaf.nonLeafKeys[l]);
      }
    }

  } else {
    //后面有
    cod preco_d = nonLeaf.nonLeafKeys[l].co_d;
    saxt_only prelsaxt = nonLeaf.nonLeafKeys[l].lsaxt;
    cod nextco_d = nonLeaf.nonLeafKeys[l+1].co_d;
    saxt_only nextrsaxt = nonLeaf.nonLeafKeys[l+1].rsaxt;
    cod co_d1 = get_co_d_from_saxt(prelsaxt, leafkey, nonLeaf.co_d);
    cod co_d2 = get_co_d_from_saxt(leafkey, nextrsaxt, nonLeaf.co_d);
    if ((preco_d - co_d1) < (nextco_d - co_d2)) {
      // 跟前面
      r_Get_NonLeaf(nonLeaf.nonLeafKeys[l]);
    } else if ((preco_d - co_d1) > (nextco_d - co_d2)) {
      //跟后面
      l_Get_NonLeaf(nonLeaf.nonLeafKeys[l+1]);
    } else {
      if (co_d1 <= co_d2) {
        r_Get_NonLeaf(nonLeaf.nonLeafKeys[l]);
      } else {
        l_Get_NonLeaf(nonLeaf.nonLeafKeys[l+1]);
      }
    }
  }

}


void ZsbTree_finder::find_One(LeafKey* res, int& res_num) {
  int pos = zsbtreee_insert::whereofKey(to_find_nonleaf->lsaxt, to_find_nonleaf->rsaxt, leafkey);
  if (pos==0) {
    NonLeaf& nonLeaf = *to_find_nonleaf;
    int l=0;
    int r=nonLeaf.num-1;
    while (l<r) {
      int mid = (l + r) / 2;
      if (leafkey < nonLeaf.nonLeafKeys[mid].rsaxt) r = mid;
      else l = mid + 1;
    }
    pos = zsbtreee_insert::whereofKey(nonLeaf.nonLeafKeys[l].lsaxt, nonLeaf.nonLeafKeys[l].rsaxt, leafkey);
    if (pos==0) {
      //里面
      leaf_Get((Leaf*)nonLeaf.nonLeafKeys[l].p, l, res, res_num);
    } else if (pos==-1){
      //前面有 先比相聚度下降程度,再看数量,但目前没有在非叶节点记录这种东西，所以这里直接比相聚度大小
      cod preco_d = nonLeaf.nonLeafKeys[l-1].co_d;
      saxt_only prelsaxt = nonLeaf.nonLeafKeys[l-1].lsaxt;
      cod nextco_d = nonLeaf.nonLeafKeys[l].co_d;
      saxt_only nextrsaxt = nonLeaf.nonLeafKeys[l].rsaxt;
      cod co_d1 = get_co_d_from_saxt(prelsaxt, leafkey, nonLeaf.co_d);
      cod co_d2 = get_co_d_from_saxt(leafkey, nextrsaxt, nonLeaf.co_d);
      if ((preco_d - co_d1) < (nextco_d - co_d2)) {
        // 跟前面
        leaf_Get((Leaf*)nonLeaf.nonLeafKeys[l-1].p, l-1, res, res_num);
      } else if ((preco_d - co_d1) > (nextco_d - co_d2)) {
        //跟后面
        leaf_Get((Leaf*)nonLeaf.nonLeafKeys[l].p, l, res, res_num);
      } else {
        if (co_d1 <= co_d2) {
          leaf_Get((Leaf*)nonLeaf.nonLeafKeys[l-1].p, l-1, res, res_num);
        } else {
          leaf_Get((Leaf*)nonLeaf.nonLeafKeys[l].p, l, res, res_num);
        }
      }

    } else {
      //后面有
      cod preco_d = nonLeaf.nonLeafKeys[l].co_d;
      saxt_only prelsaxt = nonLeaf.nonLeafKeys[l].lsaxt;
      cod nextco_d = nonLeaf.nonLeafKeys[l+1].co_d;
      saxt_only nextrsaxt = nonLeaf.nonLeafKeys[l+1].rsaxt;
      cod co_d1 = get_co_d_from_saxt(prelsaxt, leafkey, nonLeaf.co_d);
      cod co_d2 = get_co_d_from_saxt(leafkey, nextrsaxt, nonLeaf.co_d);
      if ((preco_d - co_d1) < (nextco_d - co_d2)) {
        // 跟前面
        leaf_Get((Leaf*)nonLeaf.nonLeafKeys[l].p, l, res, res_num);
      } else if ((preco_d - co_d1) > (nextco_d - co_d2)) {
        //跟后面
        leaf_Get((Leaf*)nonLeaf.nonLeafKeys[l+1].p, l+1, res, res_num);
      } else {
        if (co_d1 <= co_d2) {
          leaf_Get((Leaf*)nonLeaf.nonLeafKeys[l].p, l, res, res_num);
        } else {
          leaf_Get((Leaf*)nonLeaf.nonLeafKeys[l+1].p, l+1, res, res_num);
        }
      }
    }
  }
  else if (pos==-1) {
    leaf_Get((Leaf*)to_find_nonleaf->nonLeafKeys[0].p, 0, res, res_num);
  } else {
    leaf_Get((Leaf*)to_find_nonleaf->nonLeafKeys[to_find_nonleaf->num-1].p, to_find_nonleaf->num-1, res, res_num);
  }
}

void ZsbTree_finder::sort() {
  for (int i=0;i<to_find_nonleaf->num;i++) {
    if(i!=oneId) {
      NonLeafKey& key = to_find_nonleaf->nonLeafKeys[i];
      if (key.co_d) {
        has_cod.emplace_back(minidist_paa_to_saxt(paa, key.lsaxt.asaxt+Bit_cardinality-key.co_d, key.co_d), key.p);
      } else {
        no_has_cod.push_back(key.p);
      }
    }
  }
  std::sort(has_cod.begin(), has_cod.end());
}

void ZsbTree_finder::find_One(LeafKey* res, int& res_num, Leaf* leaf) {
  res_num = leaf->num;
#if istime == 2
    int newnum = 0;
    LeafKey* returnleafkeys = leaf->leafKeys;
    for(int i=0;i<res_num;i++) {
        ts_time thistime = returnleafkeys[i].keytime_;
        if (startTime <= thistime && thistime <= endTime) res[newnum++] = returnleafkeys[i];
    }
    res_num = newnum;
#else
    memcpy(res, leaf->leafKeys, sizeof(LeafKey) * res_num);
#endif
}
