//
// Created by hh on 2022/11/26.
//

#include "zsbtree_Build.h"

Zsbtree_Build::Zsbtree_Build(int max_size, int min_size)
    :n(max_size), m(min_size) {
  nonleafkeys.reserve(10);
  leafkeys_rep = (LeafKey*)malloc(3*n*sizeof(LeafKey));
  leafkeys.Set(leafkeys_rep, 0, 2*n);
  nonleafkeys_rep.push_back((NonLeafKey*)malloc(3*n*sizeof(NonLeafKey)));
  nonleafkeys.emplace_back(nonleafkeys_rep[0], 0, 2*n);
}

void Zsbtree_Build::Add(LeafKey& leafkey) {
  if (!leafkeys.push_back(leafkey)) {
    //满窗口了
    int toid = buildtree_window(leafkeys);
    NonLeafKey* nonLeafKey_back = nonleafkeys[0].back_add();
    nonLeafKey_back->p = leafkeys_rep;
    int newnum = toid - nonLeafKey_back->num;
    int copynum = 2*n - newnum;
    int notoid = 2*n - toid;
    memcpy(leafkeys_rep, leafkeys.data() + newnum, copynum * sizeof(LeafKey));
    leafkeys.restart(leafkeys_rep + nonLeafKey_back->num);
    leafkeys.resize(notoid);
//    out("leafkeys构建了");
//    out(toid);
//    out(leafkeys.size_add());
  }
}

void Zsbtree_Build::Add(NonLeafKey& nonLeafKey, int dep) {}

void Zsbtree_Build::Emplace(int num, cod co_d, saxt_only lsaxt, saxt_only rsaxt,
                            void* leaf, int dep) {
//  out("Emplace"+to_string(dep));
//  out(nonleafkeys[dep].size_add());
//  saxt_print(lsaxt);
//  saxt_print(rsaxt);
  if (dep + 1 == nonleafkeys_rep.size()) {
    // 空间构造了，申请下一个
    nonleafkeys_rep.push_back((NonLeafKey*)malloc(3*n*sizeof(NonLeafKey)));
    nonleafkeys.emplace_back(nonleafkeys_rep[dep + 1], 0, 2*n);
    assert(nonleafkeys.size()<=10);
  }
  if (!nonleafkeys[dep].empty_add()) {
    //前面有，先实例化了
    if (!dep) {
      doleaf(nonleafkeys[dep].back_add());
    } else {
      dononleaf(nonleafkeys[dep].back_add(), dep == 1);
    }
  }
  newVector<NonLeafKey>& nonleafkeys_dep = nonleafkeys[dep];
  nonleafkeys_dep.topush_pos->Set(num, co_d, lsaxt, rsaxt, leaf);
  nonleafkeys_dep.posadd();
  if (nonleafkeys_dep.isfull()) {
    //满窗口了
    int toid = buildtree_window(nonleafkeys_dep, dep + 1);
    NonLeafKey* dep_rep = nonleafkeys_rep[dep];
    NonLeafKey* nonLeafKey_back = nonleafkeys[dep + 1].back_add();
    nonLeafKey_back->p = dep_rep;
    int newnum = toid - nonLeafKey_back->num;
    int copynum = 2*n - newnum;
    int notoid = 2*n - toid;
    memcpy(dep_rep, nonleafkeys_dep.data() + newnum, copynum * sizeof(NonLeafKey));
    nonleafkeys_dep.restart(dep_rep + nonLeafKey_back->num);
    nonleafkeys_dep.resize(notoid);
  }
}

void Zsbtree_Build::finish() {
  if (leafkeys.size_add()<=n && !leafkeys.empty_add()){
    saxt_only lsaxt = leafkeys.data()->asaxt;
    saxt_only rsaxt = leafkeys.back_add()->asaxt;
    build_leaf_and_nonleafkey(leafkeys, 0, leafkeys.size_add(), get_co_d_from_saxt(lsaxt, rsaxt), lsaxt, rsaxt);
  } else if (leafkeys.size_add() > n){
    buildtree_window_last(leafkeys, leafkeys.size_add());
  }
  for (int i=0;i<nonleafkeys.size() - 1;i++) {
    newVector<NonLeafKey>& nonleafkeys_dep = nonleafkeys[i];
//    out("i");
//    out(i);
//    out(nonleafkeys_dep.size_add());
//    out(nonleafkeys.size() - 1);
    if (!i) doleaf(nonleafkeys_dep.back_add());
    else dononleaf(nonleafkeys_dep.back_add(), i == 1);
    if (i && i == nonleafkeys.size() - 2 && nonleafkeys_dep.size_add() == 1) {
      //最高且只有一个，且不是0层，那这个就是root的nonleafkey
      //保证root一定能访问到一个nonleaf
      break;
    }
    if (nonleafkeys_dep.size_add()<=n && !nonleafkeys_dep.empty_add()) {
      saxt_only lsaxt = nonleafkeys_dep.data()->lsaxt;
      saxt_only rsaxt = nonleafkeys_dep.back_add()->rsaxt;
      build_leaf_and_nonleafkey(nonleafkeys_dep, 0, nonleafkeys_dep.size_add(), get_co_d_from_saxt(lsaxt, rsaxt), lsaxt, rsaxt, i + 1);
    } else if (nonleafkeys_dep.size_add() > n){
//      out("进入lastbu");
      buildtree_window_last(nonleafkeys_dep, nonleafkeys_dep.size_add(), i + 1);
    }
  }
}

LeafKey* Zsbtree_Build::GetLastLeafKey() {
  return leafkeys.back_add();
}

NonLeafKey* Zsbtree_Build::GetRootKey() {
  return nonleafkeys[nonleafkeys.size()-2].data();
}


inline saxt_only Zsbtree_Build::get_saxt_i(newVector<LeafKey> &leafKeys, int i){
  return leafKeys[i].asaxt;
}

//构造leaf和索引点
inline void Zsbtree_Build::build_leaf_and_nonleafkey(newVector<LeafKey> &leafKeys, int id,
                                      int num, cod co_d, saxt_only lsaxt, saxt_only rsaxt) {

  //构造nonleaf索引点
  Emplace(num, co_d, lsaxt, rsaxt, leafKeys.data()+id, 0);
}

//构造leaf和索引点,从中间平分
inline void Zsbtree_Build::build_leaf_and_nonleafkey_two(newVector<LeafKey> &leafKeys, int id,
                                          int num, cod co_d, saxt_only lsaxt, saxt_only rsaxt) {

  int tmpnum1 = num / 2;
  int tmpnum2 = num - tmpnum1;
  //构造leaf
  saxt_only tmpsaxt = leafKeys[id+tmpnum1-1].asaxt;
  //构造nonleaf索引点
  Emplace(tmpnum1, get_co_d_from_saxt(lsaxt, tmpsaxt, co_d), lsaxt, tmpsaxt, leafKeys.data()+id, 0);

  //第二个叶
  //构造leaf
  tmpsaxt = leafKeys[id+tmpnum1].asaxt;
  //构造nonleaf索引点
  Emplace(tmpnum2, get_co_d_from_saxt(tmpsaxt, rsaxt, co_d), tmpsaxt, rsaxt, leafKeys.data()+id+tmpnum1, 0);

}

//给一个叶子结点加一些key
inline void Zsbtree_Build::add_nonleafkey(newVector<LeafKey> &leafKeys, int id,
                           int num, cod co_d, saxt_only rsaxt) {

  NonLeafKey *nonLeafKey = nonleafkeys[0].back_add();
  nonLeafKey->co_d = co_d;
  nonLeafKey->num += num;
  nonLeafKey->setRsaxt(rsaxt);
}

//给一个叶子结点加一些key,到大于n了，平分
inline void Zsbtree_Build::split_nonleafkey(newVector<LeafKey> &leafKeys, int id, int allnum,
                             int num, cod co_d, saxt_only rsaxt) {

  NonLeafKey *nonLeafKey = nonleafkeys[0].back_add();
  LeafKey* leafs = (LeafKey*)(nonLeafKey->p);
  int tmpnum1 = allnum / 2;
  int tmpnum2 = allnum - tmpnum1;
  //更新前一个点
  saxt_only newRsaxt = leafs[tmpnum1-1].asaxt;
  nonLeafKey->num = tmpnum1;
  nonLeafKey->co_d = get_co_d_from_saxt(nonLeafKey->lsaxt, newRsaxt, nonLeafKey->co_d);

  nonLeafKey->setRsaxt(newRsaxt);
  //添加后一个点
  int tmpnum3 = tmpnum2 - num;
  saxt_only newLsaxt = leafs[tmpnum1].asaxt;
  Emplace(tmpnum2, get_co_d_from_saxt(newLsaxt, rsaxt, co_d), newLsaxt, rsaxt, leafs+tmpnum1, 0);
}

//在method2中，遇到大于n的，分一下
inline int Zsbtree_Build::getbestmid(newVector<LeafKey> &leafKeys, int id, int num, cod d1, saxt_only now_saxt, saxt_only tmplastsaxt) {
  int best_mid_id = id+num/2-1;
  int best_cod = d1+d1;
  for (int mid_id = id+m-1;mid_id<id+num-m;mid_id++){
    saxt_only tmprsaxt = get_saxt_i(leafKeys, mid_id);
    cod tmpd1 = get_co_d_from_saxt(now_saxt, tmprsaxt, d1);
    saxt_only tmplsaxt = get_saxt_i(leafKeys, mid_id+1);
    cod tmpd2 = get_co_d_from_saxt(tmplsaxt, tmplastsaxt, d1);
    int tmpd = tmpd2+tmpd1;
    if (tmpd>best_cod) {
      best_mid_id = mid_id;
      best_cod = tmpd;
    }
  }
  return best_mid_id;
}

int Zsbtree_Build::get_new_end(newVector<LeafKey>& leafKeys, int l, int r,
                               saxt_only saxt_, cod co_d) {
  while(l < r)
  {
    int mid = (l + r)/ 2;
    if(compare_saxt_d(leafKeys[mid].asaxt, saxt_, co_d)) r = mid;
    else l = mid + 1;
  }
  return l;
}

int Zsbtree_Build::get_new_end_1(newVector<LeafKey>& leafKeys, int l, int r,
                                 saxt_only saxt_, cod co_d) {
  while(l < r)
  {
    int mid = (l + r + 1)/ 2;
    if(compare_saxt_d(leafKeys[mid].asaxt, saxt_, co_d)) l = mid;
    else r = mid - 1;
  }
  return l;
}


//待考虑几个平分时分节点有很多d=8的情况
//批量构建while循环内, 2n个
int Zsbtree_Build::buildtree_window(newVector<LeafKey> &leafKeys) {
  int end = 2 * n - 1;
  saxt_only first_saxt = get_saxt_i(leafKeys, 0);
  saxt_only last_saxt = get_saxt_i(leafKeys, end);
  cod window_co_d = get_co_d_from_saxt(first_saxt, last_saxt);
#if isgreed
  //method2 begin
  cod d1 = window_co_d + 1;
  //先从2n-1往前看,删掉后面的,最多删掉n个
  int new_end = get_new_end(leafKeys, n, end, last_saxt, d1);
  //从前往后看
  std::vector<method2_node> d1Arr;
  int num = 1;
  int id = 0;
  int nextid = id + m - 1;
  saxt_only now_saxt = first_saxt;
  //跟第n/2个比
  saxt_only now1_saxt = get_saxt_i(leafKeys, nextid);
  bool mark = true;
  while (true){
    if (mark){
      if (compare_saxt_d(now_saxt, now1_saxt, d1)) {
        mark = false;
      } else {
        id = get_new_end(leafKeys, id + 1, nextid, now1_saxt, d1);
        nextid = id + m - 1;
        if (nextid >= new_end) break;
        now_saxt = now1_saxt;
        now1_saxt = get_saxt_i(leafKeys, nextid);
      }
    } else {
      int overid = get_new_end_1(leafKeys, nextid, new_end-1, now1_saxt, d1);
      num = overid - id + 1;
      if (num <= n) {
        saxt_only tmplsaxt = get_saxt_i(leafKeys, id);
        saxt_only tmprsaxt = get_saxt_i(leafKeys, overid);
        cod tmpd = get_co_d_from_saxt(tmplsaxt, tmprsaxt, d1);
        d1Arr.push_back({tmpd, id, num, tmplsaxt, tmprsaxt});
      } else {
        //要找d+2,但这里先直接平分,其实可以从m开始一个一个找最小的区域
        //平分代码
        int tmpnum1 = num / 2;
        int tmpnum2 = num - tmpnum1;
        int id1 = id + tmpnum1;
        saxt_only tmplsaxt = get_saxt_i(leafKeys, id);
        saxt_only tmprsaxt = get_saxt_i(leafKeys, id1-1);
        cod tmpd = get_co_d_from_saxt(tmplsaxt, tmprsaxt, d1);
        d1Arr.push_back({tmpd, id, tmpnum1, tmplsaxt, tmprsaxt});
        tmplsaxt = get_saxt_i(leafKeys, id1);
        tmprsaxt = get_saxt_i(leafKeys, overid);
        tmpd = get_co_d_from_saxt(tmplsaxt, tmprsaxt, d1);
        d1Arr.push_back({tmpd, id1, tmpnum2, tmplsaxt, tmprsaxt});
        //找d+2代码，遍历中间每一个，寻找d最大的方案，可能有3个就再分
        //可能的区间 1000000才触发18次，太少了，不如平分
        /*
                  saxt_only tmplastsaxt = get_saxt_i(leafKeys, i-1);
                  int best_mid_id = getbestmid(leafKeys, n, m, id, num, d1, now_saxt, tmplastsaxt);
                  int tmpnum1 = best_mid_id-id+1;
                  int tmpnum2 = num-(best_mid_id-id+1);
                  saxt_only tmprsaxt = get_saxt_i(leafKeys, best_mid_id);
                  cod tmpd1 = get_co_d_from_saxt(now_saxt, tmprsaxt, d1);
                  if (tmpnum1<=n) {
                      d1Arr.push_back({tmpd1, id, tmpnum1, now_saxt, tmprsaxt});
                  } else {
                      //再分
                      int best_mid_id_ = getbestmid(leafKeys, n, m, id, tmpnum1, tmpd1, now_saxt, tmprsaxt);
                      int tmpnum1_ = best_mid_id_-id+1;
                      int tmpnum2_ = tmpnum1-tmpnum1_;
                      saxt_only tmprsaxt_ = get_saxt_i(leafKeys, best_mid_id_);
                      cod tmpd1_ = get_co_d_from_saxt(now_saxt, tmprsaxt_, tmpd1);
                      d1Arr.push_back({tmpd1_, id, tmpnum1_, now_saxt, tmprsaxt_});
                      saxt_only tmplsaxt_ = get_saxt_i(leafKeys, best_mid_id_+1);
                      cod tmpd2_ = get_co_d_from_saxt(tmplsaxt_, tmprsaxt, tmpd1);
                      d1Arr.push_back({tmpd2_, best_mid_id_+1, tmpnum2_, tmplsaxt_, tmprsaxt});
                  }
                  saxt_only tmplsaxt = get_saxt_i(leafKeys, best_mid_id+1);
                  cod tmpd2 = get_co_d_from_saxt(tmplsaxt, tmplastsaxt, d1);
                  if (tmpnum2<=n) {
                      d1Arr.push_back({tmpd2, best_mid_id+1, tmpnum2, tmplsaxt, tmplastsaxt});
                  } else {
                      //再分
                      int best_mid_id_ = getbestmid(leafKeys, n, m, best_mid_id+1, tmpnum2, tmpd2, tmplsaxt, tmplastsaxt);
                      int tmpnum1_ = best_mid_id_-best_mid_id;
                      int tmpnum2_ = tmpnum2-tmpnum1_;
                      saxt_only tmprsaxt_ = get_saxt_i(leafKeys, best_mid_id_);
                      cod tmpd1_ = get_co_d_from_saxt(tmplsaxt, tmprsaxt_, tmpd2);
                      d1Arr.push_back({tmpd1_, best_mid_id+1, tmpnum1_, tmplsaxt, tmprsaxt_});
                      saxt_only tmplsaxt_ = get_saxt_i(leafKeys, best_mid_id_+1);
                      cod tmpd2_ = get_co_d_from_saxt(tmplsaxt_, tmplastsaxt, tmpd2);
                      d1Arr.push_back({tmpd2_, best_mid_id_+1, tmpnum2_, tmplsaxt_, tmplastsaxt});
                  }
                  */
      }
      id = overid + 1;
      nextid = id + m - 1;
      if (nextid >= new_end) break;
      now_saxt = get_saxt_i(leafKeys, id);
      now1_saxt = get_saxt_i(leafKeys, nextid);
      mark = true;
    }
  }
/*
  for(int i=0;; i++) {
    if (mark) {
      if (i>=new_end-m+1) break;
      if (compare_saxt_d(now_saxt, now1_saxt, d1)) {
        num = m;
        id = i;
        mark = false;
        now1_saxt = get_saxt_i(leafKeys, i+m);
        i = i + m - 1;
      } else {
        now_saxt = get_saxt_i(leafKeys, i+1);
        now1_saxt = get_saxt_i(leafKeys, i+m);
      }
    } else {
      if (i>=new_end) {
        if (num <= n) {
          saxt_only tmpsaxt = get_saxt_i(leafKeys, i-1);
          cod tmpd = get_co_d_from_saxt(now_saxt, tmpsaxt, d1);
          d1Arr.push_back({tmpd, id, num, now_saxt, tmpsaxt});
        } else {
          //要找d+2,但这里先直接平分,其实可以从m开始一个一个找最小的区域
          //平分代码
          int tmpnum1 = num / 2;
          int tmpnum2 = num - tmpnum1;
          int id1 = id + tmpnum1;
          saxt_only tmprsaxt = get_saxt_i(leafKeys, id1-1);
          cod tmpd = get_co_d_from_saxt(now_saxt, tmprsaxt, d1);
          d1Arr.push_back({tmpd, id, tmpnum1, now_saxt, tmprsaxt});
          saxt_only tmplsaxt = get_saxt_i(leafKeys, id1);
          tmprsaxt = get_saxt_i(leafKeys, i-1);
          tmpd = get_co_d_from_saxt(tmplsaxt, tmprsaxt, d1);
          d1Arr.push_back({tmpd, id1, tmpnum2, tmplsaxt, tmprsaxt});
          //找d+2代码，遍历中间每一个，寻找d最大的方案，可能有3个就再分
          //可能的区间 1000000才触发18次，太少了，不如平分
//                    saxt_only tmplastsaxt = get_saxt_i(leafKeys, i-1);
//                    int best_mid_id = getbestmid(leafKeys, n, m, id, num, d1, now_saxt, tmplastsaxt);
//                    int tmpnum1 = best_mid_id-id+1;
//                    int tmpnum2 = num-(best_mid_id-id+1);
//                    saxt_only tmprsaxt = get_saxt_i(leafKeys, best_mid_id);
//                    cod tmpd1 = get_co_d_from_saxt(now_saxt, tmprsaxt, d1);
//                    if (tmpnum1<=n) {
//                        d1Arr.push_back({tmpd1, id, tmpnum1, now_saxt, tmprsaxt});
//                    } else {
//                        //再分
//                        int best_mid_id_ = getbestmid(leafKeys, n, m, id, tmpnum1, tmpd1, now_saxt, tmprsaxt);
//                        int tmpnum1_ = best_mid_id_-id+1;
//                        int tmpnum2_ = tmpnum1-tmpnum1_;
//                        saxt_only tmprsaxt_ = get_saxt_i(leafKeys, best_mid_id_);
//                        cod tmpd1_ = get_co_d_from_saxt(now_saxt, tmprsaxt_, tmpd1);
//                        d1Arr.push_back({tmpd1_, id, tmpnum1_, now_saxt, tmprsaxt_});
//                        saxt_only tmplsaxt_ = get_saxt_i(leafKeys, best_mid_id_+1);
//                        cod tmpd2_ = get_co_d_from_saxt(tmplsaxt_, tmprsaxt, tmpd1);
//                        d1Arr.push_back({tmpd2_, best_mid_id_+1, tmpnum2_, tmplsaxt_, tmprsaxt});
//                    }
//                    saxt_only tmplsaxt = get_saxt_i(leafKeys, best_mid_id+1);
//                    cod tmpd2 = get_co_d_from_saxt(tmplsaxt, tmplastsaxt, d1);
//                    if (tmpnum2<=n) {
//                        d1Arr.push_back({tmpd2, best_mid_id+1, tmpnum2, tmplsaxt, tmplastsaxt});
//                    } else {
//                        //再分
//                        int best_mid_id_ = getbestmid(leafKeys, n, m, best_mid_id+1, tmpnum2, tmpd2, tmplsaxt, tmplastsaxt);
//                        int tmpnum1_ = best_mid_id_-best_mid_id;
//                        int tmpnum2_ = tmpnum2-tmpnum1_;
//                        saxt_only tmprsaxt_ = get_saxt_i(leafKeys, best_mid_id_);
//                        cod tmpd1_ = get_co_d_from_saxt(tmplsaxt, tmprsaxt_, tmpd2);
//                        d1Arr.push_back({tmpd1_, best_mid_id+1, tmpnum1_, tmplsaxt, tmprsaxt_});
//                        saxt_only tmplsaxt_ = get_saxt_i(leafKeys, best_mid_id_+1);
//                        cod tmpd2_ = get_co_d_from_saxt(tmplsaxt_, tmplastsaxt, tmpd2);
//                        d1Arr.push_back({tmpd2_, best_mid_id_+1, tmpnum2_, tmplsaxt_, tmplastsaxt});
//                    }
        }
        break;
      }
      if (compare_saxt_d(now_saxt, now1_saxt, d1)) {
        num++;
        now1_saxt = get_saxt_i(leafKeys, i+1);
      } else {
        if (num <= n) {
          saxt_only tmpsaxt = get_saxt_i(leafKeys, i-1);
          cod tmpd = get_co_d_from_saxt(now_saxt, tmpsaxt, d1);
          d1Arr.push_back({tmpd, id, num, now_saxt, tmpsaxt});
        } else {
          //要找d+2,但这里先直接平分,其实可以从m开始一个一个找最小的区域
          //平分代码
          int tmpnum1 = num / 2;
          int tmpnum2 = num - tmpnum1;
          int id1 = id + tmpnum1;
          saxt_only tmprsaxt = get_saxt_i(leafKeys, id1-1);
          cod tmpd = get_co_d_from_saxt(now_saxt, tmprsaxt, d1);
          d1Arr.push_back({tmpd, id, tmpnum1, now_saxt, tmprsaxt});
          saxt_only tmplsaxt = get_saxt_i(leafKeys, id1);
          tmprsaxt = get_saxt_i(leafKeys, i-1);
          tmpd = get_co_d_from_saxt(tmplsaxt, tmprsaxt, d1);
          d1Arr.push_back({tmpd, id1, tmpnum2, tmplsaxt, tmprsaxt});
          //找d+2代码，遍历中间每一个，寻找d最大的方案
          //可能的区间
//                    saxt_only tmplastsaxt = get_saxt_i(leafKeys, i-1);
//                    int best_mid_id = getbestmid(leafKeys, n, m, id, num, d1, now_saxt, tmplastsaxt);
//                    int tmpnum1 = best_mid_id-id+1;
//                    int tmpnum2 = num-(best_mid_id-id+1);
//                    saxt_only tmprsaxt = get_saxt_i(leafKeys, best_mid_id);
//                    cod tmpd1 = get_co_d_from_saxt(now_saxt, tmprsaxt, d1);
//                    if (tmpnum1<=n) {
//                        d1Arr.push_back({tmpd1, id, tmpnum1, now_saxt, tmprsaxt});
//                    } else {
//                        //再分
//                        int best_mid_id_ = getbestmid(leafKeys, n, m, id, tmpnum1, tmpd1, now_saxt, tmprsaxt);
//                        int tmpnum1_ = best_mid_id_-id+1;
//                        int tmpnum2_ = tmpnum1-tmpnum1_;
//                        saxt_only tmprsaxt_ = get_saxt_i(leafKeys, best_mid_id_);
//                        cod tmpd1_ = get_co_d_from_saxt(now_saxt, tmprsaxt_, tmpd1);
//                        d1Arr.push_back({tmpd1_, id, tmpnum1_, now_saxt, tmprsaxt_});
//                        saxt_only tmplsaxt_ = get_saxt_i(leafKeys, best_mid_id_+1);
//                        cod tmpd2_ = get_co_d_from_saxt(tmplsaxt_, tmprsaxt, tmpd1);
//                        d1Arr.push_back({tmpd2_, best_mid_id_+1, tmpnum2_, tmplsaxt_, tmprsaxt});
//                    }
//                    saxt_only tmplsaxt = get_saxt_i(leafKeys, best_mid_id+1);
//                    cod tmpd2 = get_co_d_from_saxt(tmplsaxt, tmplastsaxt, d1);
//                    if (tmpnum2<=n) {
//                        d1Arr.push_back({tmpd2, best_mid_id+1, tmpnum2, tmplsaxt, tmplastsaxt});
//                    } else {
//                        //再分
//                        int best_mid_id_ = getbestmid(leafKeys, n, m, best_mid_id+1, tmpnum2, tmpd2, tmplsaxt, tmplastsaxt);
//                        int tmpnum1_ = best_mid_id_-best_mid_id;
//                        int tmpnum2_ = tmpnum2-tmpnum1_;
//                        saxt_only tmprsaxt_ = get_saxt_i(leafKeys, best_mid_id_);
//                        cod tmpd1_ = get_co_d_from_saxt(tmplsaxt, tmprsaxt_, tmpd2);
//                        d1Arr.push_back({tmpd1_, best_mid_id+1, tmpnum1_, tmplsaxt, tmprsaxt_});
//                        saxt_only tmplsaxt_ = get_saxt_i(leafKeys, best_mid_id_+1);
//                        cod tmpd2_ = get_co_d_from_saxt(tmplsaxt_, tmplastsaxt, tmpd2);
//                        d1Arr.push_back({tmpd2_, best_mid_id_+1, tmpnum2_, tmplsaxt_, tmplastsaxt});
//                    }
        }
        mark = true;
        if (i>=new_end-m+1) break;
        i--;
        now_saxt = now1_saxt;
        now1_saxt = get_saxt_i(leafKeys, i+m);
      }
    }
  }
  */
  //method2 end
  //构建叶子结点和非叶子的索引点
  int todoid = 0;
  for(method2_node anode: d1Arr) {
    int todonum = anode.id - todoid;
    if (todonum == 0) {
//            out("laile");
      build_leaf_and_nonleafkey(leafKeys, todoid, anode.num, anode.co_d, anode.lsaxt, anode.rsaxt);
//            out("laile2");
      todoid += anode.num;
      continue;
    }
    //前面小范围的
    saxt_only re_lsaxt = get_saxt_i(leafKeys, todoid);
    saxt_only re_rsaxt = get_saxt_i(leafKeys, anode.id-1);
    if (todonum <= m) {
      if (nonleafkeys[0].empty_add()){
        //只看后面
        //如果后面是d+1且加起来<=n，则合起来，不然不要了。这里直接合
        int tmpnum = todonum + anode.num;
        if (tmpnum <= n){
          build_leaf_and_nonleafkey(leafKeys, todoid, tmpnum, window_co_d, re_lsaxt, anode.rsaxt);
          todoid += tmpnum;
          continue;
        } else {
          //构造leaf
          build_leaf_and_nonleafkey_two(leafKeys, todoid, tmpnum, window_co_d, re_lsaxt, anode.rsaxt);
          todoid += tmpnum;
          continue;
        }
      } else {
        //要看前面
        cod preco_d = nonleafkeys[0].back_add()->co_d;
        saxt_only prelsaxt = nonleafkeys[0].back_add()->lsaxt;
        int prenum = nonleafkeys[0].back_add()->num;

        cod nextco_d = anode.co_d;
        saxt_only nextrsaxt = anode.rsaxt;
        int nextnum = anode.num;
        //先对比下降程度
        cod co_d1;
        if (todoid == 0) co_d1 = get_co_d_from_saxt(prelsaxt, re_rsaxt);
        else co_d1 = get_co_d_from_saxt(prelsaxt, re_rsaxt, window_co_d);
        cod co_d2 = get_co_d_from_saxt(re_lsaxt, anode.rsaxt, window_co_d);
        if ((preco_d - co_d1) < (nextco_d - co_d2)) {
          //跟前面合
          int tmpnum = prenum + todonum;
          if (tmpnum <= n) {
            add_nonleafkey(leafKeys, todoid, todonum, co_d1, re_rsaxt);
          } else {
            //平分
            split_nonleafkey(leafKeys, todoid, tmpnum, todonum, co_d1, re_rsaxt);
          }
          build_leaf_and_nonleafkey(leafKeys, anode.id, anode.num, anode.co_d, anode.lsaxt, anode.rsaxt);
          todoid = todoid + todonum + anode.num;
          continue;
        } else if ((preco_d - co_d1) > (nextco_d - co_d2)) {
          //跟后面合
          int tmpnum = nextnum + todonum;
          if (tmpnum <= n) {
            build_leaf_and_nonleafkey(leafKeys, todoid, tmpnum, co_d2, re_lsaxt, anode.rsaxt);
            todoid += tmpnum;
            continue;
          } else {
            //平分
            build_leaf_and_nonleafkey_two(leafKeys, todoid, tmpnum, co_d2, re_lsaxt, anode.rsaxt);
            todoid += tmpnum;
            continue;
          }
        } else {
          //再比大小
          int tmpnum1 = prenum + todonum;
          int tmpnum2 = nextnum + todonum;
          if (tmpnum1 <= n && tmpnum2 > n) {
            add_nonleafkey(leafKeys, todoid, todonum, co_d1, re_rsaxt);
            build_leaf_and_nonleafkey(leafKeys, anode.id, anode.num, anode.co_d, anode.lsaxt, anode.rsaxt);
            todoid = todoid + todonum + anode.num;
            continue;
          } else if (tmpnum1 > n && tmpnum2 <= n) {
            build_leaf_and_nonleafkey(leafKeys, todoid, tmpnum2, co_d2, re_lsaxt, anode.rsaxt);
            todoid += tmpnum2;
            continue;
          } else if (tmpnum1 >n && tmpnum2 > n){
            // 比相聚度,跟大的合
            if (co_d2 >= co_d1) {
              build_leaf_and_nonleafkey_two(leafKeys, todoid, tmpnum2, co_d2, re_lsaxt, anode.rsaxt);
              todoid += tmpnum2;
              continue;
            } else {
              split_nonleafkey(leafKeys, todoid, tmpnum1, todonum, co_d1, re_rsaxt);
              build_leaf_and_nonleafkey(leafKeys, anode.id, anode.num, anode.co_d, anode.lsaxt, anode.rsaxt);
              todoid += tmpnum2;
              continue;
            }
          } else {
            // 比相聚度
            if (co_d2 >= co_d1) {
              build_leaf_and_nonleafkey(leafKeys, todoid, tmpnum2, co_d2, re_lsaxt, anode.rsaxt);
              todoid += tmpnum2;
              continue;
            } else {
              add_nonleafkey(leafKeys, todoid, todonum, co_d1, re_rsaxt);
              build_leaf_and_nonleafkey(leafKeys, anode.id, anode.num, anode.co_d, anode.lsaxt, anode.rsaxt);
              todoid += tmpnum2;
              continue;
            }
          }
        }
      }
    } else if (todonum >m && todonum <= n) {
      //其实就是window_co_d
      build_leaf_and_nonleafkey(leafKeys, todoid, todonum, get_co_d_from_saxt(re_lsaxt, re_rsaxt, window_co_d), re_lsaxt, re_rsaxt);
      build_leaf_and_nonleafkey(leafKeys, anode.id, anode.num, anode.co_d, anode.lsaxt, anode.rsaxt);
      todoid = todoid + todonum + anode.num;
      continue;
    } else {
      build_leaf_and_nonleafkey_two(leafKeys, todoid, todonum, window_co_d, re_lsaxt, re_rsaxt);
      build_leaf_and_nonleafkey(leafKeys, anode.id, anode.num, anode.co_d, anode.lsaxt, anode.rsaxt);
      todoid = todoid + todonum + anode.num;
      continue;
    }
  }
  if (todoid == 0) {
    //打包n个
    build_leaf_and_nonleafkey(leafKeys, todoid, n, window_co_d, first_saxt, get_saxt_i(leafKeys, n-1));
    todoid += n;
  }
  return todoid;
#else

  build_leaf_and_nonleafkey_two(leafKeys, 0, n*2, window_co_d, first_saxt, last_saxt);
  return 2*n;
#endif
}


void Zsbtree_Build::buildtree_window_last(newVector<LeafKey> &leafKeys, int allnum) {
  assert(allnum>n);
  int end = allnum - 1;
  saxt_only first_saxt = get_saxt_i(leafKeys, 0);
  saxt_only last_saxt = get_saxt_i(leafKeys, end);
  cod window_co_d = get_co_d_from_saxt(first_saxt, last_saxt);
#if isgreed
  //method2 begin
  cod d1 = window_co_d + 1;
  //先从2n-1往前看,删掉后面的,最多删掉n个
  int new_end = end;
  //从前往后看
  std::vector<method2_node> d1Arr;
  int num = 1;
  int id = 0;
  int nextid = id + m - 1;
  saxt_only now_saxt = first_saxt;
  //跟第n/2个比
  saxt_only now1_saxt = get_saxt_i(leafKeys, nextid);
  bool mark = true;
  while (true){
    if (mark){
      if (compare_saxt_d(now_saxt, now1_saxt, d1)) {
        mark = false;
      } else {
        id = get_new_end(leafKeys, id + 1, nextid, now1_saxt, d1);
        nextid = id + m - 1;
        if (nextid >= new_end) break;
        now_saxt = now1_saxt;
        now1_saxt = get_saxt_i(leafKeys, nextid);
      }
    } else {
      int overid = get_new_end_1(leafKeys, nextid, new_end-1, now1_saxt, d1);
      num = overid - id + 1;
      if (num <= n) {
        saxt_only tmplsaxt = get_saxt_i(leafKeys, id);
        saxt_only tmprsaxt = get_saxt_i(leafKeys, overid);
        cod tmpd = get_co_d_from_saxt(tmplsaxt, tmprsaxt, d1);
        d1Arr.push_back({tmpd, id, num, tmplsaxt, tmprsaxt});
      } else {
        //要找d+2,但这里先直接平分,其实可以从m开始一个一个找最小的区域
        //平分代码
        int tmpnum1 = num / 2;
        int tmpnum2 = num - tmpnum1;
        int id1 = id + tmpnum1;
        saxt_only tmplsaxt = get_saxt_i(leafKeys, id);
        saxt_only tmprsaxt = get_saxt_i(leafKeys, id1-1);
        cod tmpd = get_co_d_from_saxt(tmplsaxt, tmprsaxt, d1);
        d1Arr.push_back({tmpd, id, tmpnum1, tmplsaxt, tmprsaxt});
        tmplsaxt = get_saxt_i(leafKeys, id1);
        tmprsaxt = get_saxt_i(leafKeys, overid);
        tmpd = get_co_d_from_saxt(tmplsaxt, tmprsaxt, d1);
        d1Arr.push_back({tmpd, id1, tmpnum2, tmplsaxt, tmprsaxt});
        //找d+2代码，遍历中间每一个，寻找d最大的方案，可能有3个就再分
        //可能的区间 1000000才触发18次，太少了，不如平分
        /*
                  saxt_only tmplastsaxt = get_saxt_i(leafKeys, i-1);
                  int best_mid_id = getbestmid(leafKeys, n, m, id, num, d1, now_saxt, tmplastsaxt);
                  int tmpnum1 = best_mid_id-id+1;
                  int tmpnum2 = num-(best_mid_id-id+1);
                  saxt_only tmprsaxt = get_saxt_i(leafKeys, best_mid_id);
                  cod tmpd1 = get_co_d_from_saxt(now_saxt, tmprsaxt, d1);
                  if (tmpnum1<=n) {
                      d1Arr.push_back({tmpd1, id, tmpnum1, now_saxt, tmprsaxt});
                  } else {
                      //再分
                      int best_mid_id_ = getbestmid(leafKeys, n, m, id, tmpnum1, tmpd1, now_saxt, tmprsaxt);
                      int tmpnum1_ = best_mid_id_-id+1;
                      int tmpnum2_ = tmpnum1-tmpnum1_;
                      saxt_only tmprsaxt_ = get_saxt_i(leafKeys, best_mid_id_);
                      cod tmpd1_ = get_co_d_from_saxt(now_saxt, tmprsaxt_, tmpd1);
                      d1Arr.push_back({tmpd1_, id, tmpnum1_, now_saxt, tmprsaxt_});
                      saxt_only tmplsaxt_ = get_saxt_i(leafKeys, best_mid_id_+1);
                      cod tmpd2_ = get_co_d_from_saxt(tmplsaxt_, tmprsaxt, tmpd1);
                      d1Arr.push_back({tmpd2_, best_mid_id_+1, tmpnum2_, tmplsaxt_, tmprsaxt});
                  }
                  saxt_only tmplsaxt = get_saxt_i(leafKeys, best_mid_id+1);
                  cod tmpd2 = get_co_d_from_saxt(tmplsaxt, tmplastsaxt, d1);
                  if (tmpnum2<=n) {
                      d1Arr.push_back({tmpd2, best_mid_id+1, tmpnum2, tmplsaxt, tmplastsaxt});
                  } else {
                      //再分
                      int best_mid_id_ = getbestmid(leafKeys, n, m, best_mid_id+1, tmpnum2, tmpd2, tmplsaxt, tmplastsaxt);
                      int tmpnum1_ = best_mid_id_-best_mid_id;
                      int tmpnum2_ = tmpnum2-tmpnum1_;
                      saxt_only tmprsaxt_ = get_saxt_i(leafKeys, best_mid_id_);
                      cod tmpd1_ = get_co_d_from_saxt(tmplsaxt, tmprsaxt_, tmpd2);
                      d1Arr.push_back({tmpd1_, best_mid_id+1, tmpnum1_, tmplsaxt, tmprsaxt_});
                      saxt_only tmplsaxt_ = get_saxt_i(leafKeys, best_mid_id_+1);
                      cod tmpd2_ = get_co_d_from_saxt(tmplsaxt_, tmplastsaxt, tmpd2);
                      d1Arr.push_back({tmpd2_, best_mid_id_+1, tmpnum2_, tmplsaxt_, tmplastsaxt});
                  }
                  */
      }
      id = overid + 1;
      nextid = id + m - 1;
      if (nextid >= new_end) break;
      now_saxt = get_saxt_i(leafKeys, id);
      now1_saxt = get_saxt_i(leafKeys, nextid);
      mark = true;
    }
  }
/*
  for (int i = 0;; i++) {
    if (mark) {
      if (i >= new_end - m + 1) break;
      if (compare_saxt_d(now_saxt, now1_saxt, d1)) {
        num = m;
        id = i;
        mark = false;
        now1_saxt = get_saxt_i(leafKeys, i + m);
        i = i + m - 1;
      } else {
        now_saxt = get_saxt_i(leafKeys, i + 1);
        now1_saxt = get_saxt_i(leafKeys, i + m);
      }
    } else {
      if (i >= new_end) {
        if (num <= n) {
          saxt_only tmpsaxt = get_saxt_i(leafKeys, i - 1);
          cod tmpd = get_co_d_from_saxt(now_saxt, tmpsaxt, d1);
          d1Arr.push_back({tmpd, id, num, now_saxt, tmpsaxt});
        } else {
          //要找d+2,但这里先直接平分,其实可以从m开始一个一个找最小的区域
          //平分代码
          int tmpnum1 = num / 2;
          int tmpnum2 = num - tmpnum1;
          int id1 = id + tmpnum1;
          saxt_only tmprsaxt = get_saxt_i(leafKeys, id1 - 1);
          cod tmpd = get_co_d_from_saxt(now_saxt, tmprsaxt, d1);
          d1Arr.push_back({tmpd, id, tmpnum1, now_saxt, tmprsaxt});
          saxt_only tmplsaxt = get_saxt_i(leafKeys, id1);
          tmprsaxt = get_saxt_i(leafKeys, i - 1);
          tmpd = get_co_d_from_saxt(tmplsaxt, tmprsaxt, d1);
          d1Arr.push_back({tmpd, id1, tmpnum2, tmplsaxt, tmprsaxt});
          //找d+2代码，遍历中间每一个，寻找d最大的方案，可能有3个就再分
          //可能的区间 1000000才触发18次，太少了，不如平分
//                    saxt_only tmplastsaxt = get_saxt_i(leafKeys, i-1);
//                    int best_mid_id = getbestmid(leafKeys, n, m, id, num, d1, now_saxt, tmplastsaxt);
//                    int tmpnum1 = best_mid_id-id+1;
//                    int tmpnum2 = num-(best_mid_id-id+1);
//                    saxt_only tmprsaxt = get_saxt_i(leafKeys, best_mid_id);
//                    cod tmpd1 = get_co_d_from_saxt(now_saxt, tmprsaxt, d1);
//                    if (tmpnum1<=n) {
//                        d1Arr.push_back({tmpd1, id, tmpnum1, now_saxt, tmprsaxt});
//                    } else {
//                        //再分
//                        int best_mid_id_ = getbestmid(leafKeys, n, m, id, tmpnum1, tmpd1, now_saxt, tmprsaxt);
//                        int tmpnum1_ = best_mid_id_-id+1;
//                        int tmpnum2_ = tmpnum1-tmpnum1_;
//                        saxt_only tmprsaxt_ = get_saxt_i(leafKeys, best_mid_id_);
//                        cod tmpd1_ = get_co_d_from_saxt(now_saxt, tmprsaxt_, tmpd1);
//                        d1Arr.push_back({tmpd1_, id, tmpnum1_, now_saxt, tmprsaxt_});
//                        saxt_only tmplsaxt_ = get_saxt_i(leafKeys, best_mid_id_+1);
//                        cod tmpd2_ = get_co_d_from_saxt(tmplsaxt_, tmprsaxt, tmpd1);
//                        d1Arr.push_back({tmpd2_, best_mid_id_+1, tmpnum2_, tmplsaxt_, tmprsaxt});
//                    }
//                    saxt_only tmplsaxt = get_saxt_i(leafKeys, best_mid_id+1);
//                    cod tmpd2 = get_co_d_from_saxt(tmplsaxt, tmplastsaxt, d1);
//                    if (tmpnum2<=n) {
//                        d1Arr.push_back({tmpd2, best_mid_id+1, tmpnum2, tmplsaxt, tmplastsaxt});
//                    } else {
//                        //再分
//                        int best_mid_id_ = getbestmid(leafKeys, n, m, best_mid_id+1, tmpnum2, tmpd2, tmplsaxt, tmplastsaxt);
//                        int tmpnum1_ = best_mid_id_-best_mid_id;
//                        int tmpnum2_ = tmpnum2-tmpnum1_;
//                        saxt_only tmprsaxt_ = get_saxt_i(leafKeys, best_mid_id_);
//                        cod tmpd1_ = get_co_d_from_saxt(tmplsaxt, tmprsaxt_, tmpd2);
//                        d1Arr.push_back({tmpd1_, best_mid_id+1, tmpnum1_, tmplsaxt, tmprsaxt_});
//                        saxt_only tmplsaxt_ = get_saxt_i(leafKeys, best_mid_id_+1);
//                        cod tmpd2_ = get_co_d_from_saxt(tmplsaxt_, tmplastsaxt, tmpd2);
//                        d1Arr.push_back({tmpd2_, best_mid_id_+1, tmpnum2_, tmplsaxt_, tmplastsaxt});
//                    }
        }
        break;
      }
      if (compare_saxt_d(now_saxt, now1_saxt, d1)) {
        num++;
        now1_saxt = get_saxt_i(leafKeys, i + 1);
      } else {
        if (num <= n) {
          saxt_only tmpsaxt = get_saxt_i(leafKeys, i - 1);
          cod tmpd = get_co_d_from_saxt(now_saxt, tmpsaxt, d1);
          d1Arr.push_back({tmpd, id, num, now_saxt, tmpsaxt});
        } else {
          //要找d+2,但这里先直接平分,其实可以从m开始一个一个找最小的区域
          //平分代码
          int tmpnum1 = num / 2;
          int tmpnum2 = num - tmpnum1;
          int id1 = id + tmpnum1;
          saxt_only tmprsaxt = get_saxt_i(leafKeys, id1 - 1);
          cod tmpd = get_co_d_from_saxt(now_saxt, tmprsaxt, d1);
          d1Arr.push_back({tmpd, id, tmpnum1, now_saxt, tmprsaxt});
          saxt_only tmplsaxt = get_saxt_i(leafKeys, id1);
          tmprsaxt = get_saxt_i(leafKeys, i - 1);
          tmpd = get_co_d_from_saxt(tmplsaxt, tmprsaxt, d1);
          d1Arr.push_back({tmpd, id1, tmpnum2, tmplsaxt, tmprsaxt});
          //找d+2代码，遍历中间每一个，寻找d最大的方案
          //可能的区间
//                    saxt_only tmplastsaxt = get_saxt_i(leafKeys, i-1);
//                    int best_mid_id = getbestmid(leafKeys, n, m, id, num, d1, now_saxt, tmplastsaxt);
//                    int tmpnum1 = best_mid_id-id+1;
//                    int tmpnum2 = num-(best_mid_id-id+1);
//                    saxt_only tmprsaxt = get_saxt_i(leafKeys, best_mid_id);
//                    cod tmpd1 = get_co_d_from_saxt(now_saxt, tmprsaxt, d1);
//                    if (tmpnum1<=n) {
//                        d1Arr.push_back({tmpd1, id, tmpnum1, now_saxt, tmprsaxt});
//                    } else {
//                        //再分
//                        int best_mid_id_ = getbestmid(leafKeys, n, m, id, tmpnum1, tmpd1, now_saxt, tmprsaxt);
//                        int tmpnum1_ = best_mid_id_-id+1;
//                        int tmpnum2_ = tmpnum1-tmpnum1_;
//                        saxt_only tmprsaxt_ = get_saxt_i(leafKeys, best_mid_id_);
//                        cod tmpd1_ = get_co_d_from_saxt(now_saxt, tmprsaxt_, tmpd1);
//                        d1Arr.push_back({tmpd1_, id, tmpnum1_, now_saxt, tmprsaxt_});
//                        saxt_only tmplsaxt_ = get_saxt_i(leafKeys, best_mid_id_+1);
//                        cod tmpd2_ = get_co_d_from_saxt(tmplsaxt_, tmprsaxt, tmpd1);
//                        d1Arr.push_back({tmpd2_, best_mid_id_+1, tmpnum2_, tmplsaxt_, tmprsaxt});
//                    }
//                    saxt_only tmplsaxt = get_saxt_i(leafKeys, best_mid_id+1);
//                    cod tmpd2 = get_co_d_from_saxt(tmplsaxt, tmplastsaxt, d1);
//                    if (tmpnum2<=n) {
//                        d1Arr.push_back({tmpd2, best_mid_id+1, tmpnum2, tmplsaxt, tmplastsaxt});
//                    } else {
//                        //再分
////                        out("num");
////                        out(best_mid_id+1);
////                        out(tmpnum2);
//                        int best_mid_id_ = getbestmid(leafKeys, n, m, best_mid_id+1, tmpnum2, tmpd2, tmplsaxt, tmplastsaxt);
////                        out(best_mid_id_);
//                        int tmpnum1_ = best_mid_id_-best_mid_id;
//                        int tmpnum2_ = tmpnum2-tmpnum1_;
////                        out(tmpnum2_);
//                        saxt_only tmprsaxt_ = get_saxt_i(leafKeys, best_mid_id_);
//                        cod tmpd1_ = get_co_d_from_saxt(tmplsaxt, tmprsaxt_, tmpd2);
//                        d1Arr.push_back({tmpd1_, best_mid_id+1, tmpnum1_, tmplsaxt, tmprsaxt_});
//                        saxt_only tmplsaxt_ = get_saxt_i(leafKeys, best_mid_id_+1);
//                        cod tmpd2_ = get_co_d_from_saxt(tmplsaxt_, tmplastsaxt, tmpd2);
//                        d1Arr.push_back({tmpd2_, best_mid_id_+1, tmpnum2_, tmplsaxt_, tmplastsaxt});
//                    }
        }
        mark = true;
        if (i >= new_end - m + 1) break;
        i--;
        now_saxt = now1_saxt;
        now1_saxt = get_saxt_i(leafKeys, i + m);
      }
    }
  }
  */
  //method2 end
  //构建叶子结点和非叶子的索引点
  int todoid = 0;
  for (method2_node anode: d1Arr) {
    int todonum = anode.id - todoid;
    if (todonum == 0) {
//            out("laile");
      build_leaf_and_nonleafkey(leafKeys, todoid, anode.num, anode.co_d, anode.lsaxt, anode.rsaxt);
//            out("laile2");
      todoid += anode.num;
      continue;
    }
    //前面小范围的
    saxt_only re_lsaxt = get_saxt_i(leafKeys, todoid);
    saxt_only re_rsaxt = get_saxt_i(leafKeys, anode.id - 1);
    if (todonum <= m) {
      if (nonleafkeys[0].empty_add()) {
        //只看后面
        //如果后面是d+1且加起来<=n，则合起来，不然不要了。这里直接合
        int tmpnum = todonum + anode.num;
        if (tmpnum <= n) {
          build_leaf_and_nonleafkey(leafKeys, todoid, tmpnum, window_co_d, re_lsaxt,
                                    anode.rsaxt);
          todoid += tmpnum;
          continue;
        } else {
          //构造leaf
          build_leaf_and_nonleafkey_two(leafKeys, todoid, tmpnum, window_co_d, re_lsaxt,
                                        anode.rsaxt);
          todoid += tmpnum;
          continue;
        }
      } else {
//                out("Jinru:");
        //要看前面
        cod preco_d = nonleafkeys[0].back_add()->co_d;
        saxt_only prelsaxt = nonleafkeys[0].back_add()->lsaxt;
        int prenum = nonleafkeys[0].back_add()->num;

        cod nextco_d = anode.co_d;
        saxt_only nextrsaxt = anode.rsaxt;
        int nextnum = anode.num;
        //先对比下降程度
        cod co_d1;
        if (todoid == 0) co_d1 = get_co_d_from_saxt(prelsaxt, re_rsaxt);
        else co_d1 = get_co_d_from_saxt(prelsaxt, re_rsaxt, window_co_d);
        cod co_d2 = get_co_d_from_saxt(re_lsaxt, anode.rsaxt, window_co_d);

//                out((int)preco_d);
//                out((int)co_d1);
//                out((int)nextco_d);
//                out((int)co_d2);

        if ((preco_d - co_d1) < (nextco_d - co_d2)) {
//                    out(prenum);
//                    out(todonum);
          //跟前面合
          int tmpnum = prenum + todonum;
          if (tmpnum <= n) {
//                        out("laile");
            add_nonleafkey(leafKeys, todoid, todonum, co_d1, re_rsaxt);
          } else {
            //平分
            split_nonleafkey(leafKeys, todoid, tmpnum, todonum, co_d1, re_rsaxt);
          }
          build_leaf_and_nonleafkey(leafKeys, anode.id, anode.num, anode.co_d, anode.lsaxt,
                                    anode.rsaxt);
          todoid = todoid + todonum + anode.num;
          continue;
        } else if ((preco_d - co_d1) > (nextco_d - co_d2)) {
          //跟后面合
          int tmpnum = nextnum + todonum;
          if (tmpnum <= n) {
            build_leaf_and_nonleafkey(leafKeys, todoid, tmpnum, co_d2, re_lsaxt, anode.rsaxt);
            todoid += tmpnum;
            continue;
          } else {
            //平分
            build_leaf_and_nonleafkey_two(leafKeys, todoid, tmpnum, co_d2, re_lsaxt,
                                          anode.rsaxt);
            todoid += tmpnum;
            continue;
          }
        } else {
          //再比大小
          int tmpnum1 = prenum + todonum;
          int tmpnum2 = nextnum + todonum;
          if (tmpnum1 <= n && tmpnum2 > n) {
            add_nonleafkey(leafKeys, todoid, todonum, co_d1, re_rsaxt);
            build_leaf_and_nonleafkey(leafKeys, anode.id, anode.num, anode.co_d, anode.lsaxt,
                                      anode.rsaxt);
            todoid = todoid + todonum + anode.num;
            continue;
          } else if (tmpnum1 > n && tmpnum2 <= n) {
            build_leaf_and_nonleafkey(leafKeys, todoid, tmpnum2, co_d2, re_lsaxt, anode.rsaxt);
            todoid += tmpnum2;
            continue;
          } else if (tmpnum1 > n && tmpnum2 > n) {
            // 比相聚度,跟大的合
            if (co_d2 >= co_d1) {
              build_leaf_and_nonleafkey_two(leafKeys, todoid, tmpnum2, co_d2, re_lsaxt,
                                            anode.rsaxt);
              todoid += tmpnum2;
              continue;
            } else {
              split_nonleafkey(leafKeys, todoid, tmpnum1, todonum, co_d1, re_rsaxt);
              build_leaf_and_nonleafkey(leafKeys, anode.id, anode.num, anode.co_d,
                                        anode.lsaxt, anode.rsaxt);
              todoid += tmpnum2;
              continue;
            }
          } else {
            // 比相聚度
            if (co_d2 >= co_d1) {
              build_leaf_and_nonleafkey(leafKeys, todoid, tmpnum2, co_d2, re_lsaxt,
                                        anode.rsaxt);
              todoid += tmpnum2;
              continue;
            } else {
              add_nonleafkey(leafKeys, todoid, todonum, co_d1, re_rsaxt);
              build_leaf_and_nonleafkey(leafKeys, anode.id, anode.num, anode.co_d,
                                        anode.lsaxt, anode.rsaxt);
              todoid += tmpnum2;
              continue;
            }
          }
        }
      }
    } else if (todonum > m && todonum <= n) {
      //其实就是window_co_d
      build_leaf_and_nonleafkey(leafKeys, todoid, todonum,
                                get_co_d_from_saxt(re_lsaxt, re_rsaxt, window_co_d), re_lsaxt, re_rsaxt);
      build_leaf_and_nonleafkey(leafKeys, anode.id, anode.num, anode.co_d, anode.lsaxt, anode.rsaxt);
      todoid = todoid + todonum + anode.num;
      continue;
    } else {
      build_leaf_and_nonleafkey_two(leafKeys, todoid, todonum, window_co_d, re_lsaxt, re_rsaxt);
      build_leaf_and_nonleafkey(leafKeys, anode.id, anode.num, anode.co_d, anode.lsaxt, anode.rsaxt);
      todoid = todoid + todonum + anode.num;
      continue;
    }
  }
  int todonum = allnum - todoid;
  if (todonum > n) {
    //直接平分打包
    saxt_only lsaxt = get_saxt_i(leafKeys, todoid);
    build_leaf_and_nonleafkey_two(leafKeys, todoid, todonum, window_co_d, lsaxt, last_saxt);
  } else if (todonum > m && todonum <= n) {
    //直接打包
    saxt_only lsaxt = get_saxt_i(leafKeys, todoid);
    build_leaf_and_nonleafkey(leafKeys, todoid, todonum, window_co_d, lsaxt, last_saxt);
  } else if (todonum > 0) {
    if (!nonleafkeys[0].empty_add()) {
      //跟前面合
      saxt_only prelsaxt = nonleafkeys[0].back_add()->lsaxt;
      int prenum = nonleafkeys[0].back_add()->num;
      int tmpnum = prenum + todonum;
      cod co_d1;
      if (todoid == 0) co_d1 = get_co_d_from_saxt(prelsaxt, last_saxt);
      else co_d1 = get_co_d_from_saxt(prelsaxt, last_saxt, window_co_d);
      if (tmpnum <= n) {
        add_nonleafkey(leafKeys, todoid, todonum, co_d1, last_saxt);
      } else {
        //平分
        split_nonleafkey(leafKeys, todoid, tmpnum, todonum, co_d1, last_saxt);
      }
    } else {
      //前面一个点没有直接打包
      build_leaf_and_nonleafkey(leafKeys, todoid, todonum, window_co_d, first_saxt, last_saxt);
    }
  }
#else
  build_leaf_and_nonleafkey_two(leafKeys, 0, allnum, window_co_d, first_saxt, last_saxt);
#endif
}


inline saxt_only Zsbtree_Build::get_saxt_i(const newVector<NonLeafKey> &leafKeys, int i){
  return leafKeys[i].lsaxt;
}

inline saxt_only Zsbtree_Build::get_saxt_i_r(const newVector<NonLeafKey> &leafKeys, int i){
  return leafKeys[i].rsaxt;
}


//构造leaf和索引点
inline void Zsbtree_Build::build_leaf_and_nonleafkey(const newVector<NonLeafKey> &leafKeys, int id,
                                      int num, cod co_d, saxt_only lsaxt, saxt_only rsaxt, int dep) {

  Emplace(num, co_d, lsaxt, rsaxt, leafKeys.data()+id, dep);
}

//构造leaf和索引点,从中间平分
inline void Zsbtree_Build::build_leaf_and_nonleafkey_two(const newVector<NonLeafKey> &leafKeys, int id,
                                          int num, cod co_d, saxt_only lsaxt, saxt_only rsaxt, int dep) {
  int tmpnum1 = num / 2;
  int tmpnum2 = num - tmpnum1;
  //构造leaf
  saxt_only tmpsaxt = leafKeys[id+tmpnum1-1].rsaxt;
  //构造nonleaf索引点
  Emplace(tmpnum1, get_co_d_from_saxt(lsaxt, tmpsaxt, co_d), lsaxt, tmpsaxt, leafKeys.data()+id, dep);
  //第二个叶
  //构造leaf
  tmpsaxt = leafKeys[id+tmpnum1].lsaxt;
  //构造nonleaf索引点
  Emplace(tmpnum2, get_co_d_from_saxt(tmpsaxt, rsaxt, co_d), tmpsaxt, rsaxt, leafKeys.data()+id+tmpnum1, dep);
}

//给一个叶子结点加一些key
inline void Zsbtree_Build::add_nonleafkey(const newVector<NonLeafKey> &leafKeys, int id,
                           int num, cod co_d, saxt_only rsaxt, int dep) {
  NonLeafKey *nonLeafKey = nonleafkeys[dep].back_add();
  nonLeafKey->co_d = co_d;
  nonLeafKey->num += num;
  nonLeafKey->setRsaxt(rsaxt);
}

//给一个叶子结点加一些key,到大于n了，平分
inline void Zsbtree_Build::split_nonleafkey(const newVector<NonLeafKey> &leafKeys, int id, int allnum,
                             int num, cod co_d, saxt_only rsaxt, int dep) {

  NonLeafKey *nonLeafKey = nonleafkeys[dep].back_add();
  NonLeafKey *leafs = (NonLeafKey *)(nonLeafKey->p);
  int tmpnum1 = allnum / 2;
  int tmpnum2 = allnum - tmpnum1;
  //更新前一个点
  saxt_only newRsaxt = leafs[tmpnum1-1].rsaxt;
  nonLeafKey->num = tmpnum1;
  nonLeafKey->co_d = get_co_d_from_saxt(nonLeafKey->lsaxt, newRsaxt, nonLeafKey->co_d);
  nonLeafKey->setRsaxt(newRsaxt);
  //添加后一个点
  int tmpnum3 = tmpnum2 - num;
  saxt_only newLsaxt = leafs[tmpnum1].lsaxt;
  Emplace(tmpnum2, get_co_d_from_saxt(newLsaxt, rsaxt, co_d), newLsaxt, rsaxt, leafs+tmpnum1, dep);
}

//在method2中，遇到大于n的，分一下
inline int Zsbtree_Build::getbestmid(const newVector<NonLeafKey> &leafKeys, int id, int num, cod d1, saxt_only now_saxt, saxt_only tmplastsaxt) {
  int best_mid_id = id+num/2-1;
  int best_cod = d1+d1;
  for (int mid_id = id+m-1;mid_id<id+num-m;mid_id++){
    saxt_only tmprsaxt = get_saxt_i_r(leafKeys, mid_id);
    cod tmpd1 = get_co_d_from_saxt(now_saxt, tmprsaxt, d1);
    saxt_only tmplsaxt = get_saxt_i(leafKeys, mid_id+1);
    cod tmpd2 = get_co_d_from_saxt(tmplsaxt, tmplastsaxt, d1);
    int tmpd = tmpd2+tmpd1;
    if (tmpd>best_cod) {
      best_mid_id = mid_id;
      best_cod = tmpd;
    }
  }
  return best_mid_id;
}


int Zsbtree_Build::get_new_end(const newVector<NonLeafKey>& leafKeys, int l, int r,
                               saxt_only saxt_, cod co_d) {
  while(l < r)
  {
    int mid = (l + r + 1)/ 2;
    if(compare_saxt_d(get_saxt_i(leafKeys, mid), saxt_, co_d)) r = mid - 1;
    else l = mid;
  }
  return l;
}

int Zsbtree_Build::get_new_end_1(const newVector<NonLeafKey>& leafKeys, int l,
                                 int r, saxt_only saxt_, cod co_d) {
  while(l < r)
  {
    int mid = (l + r + 1)/ 2;
    if(compare_saxt_d(get_saxt_i_r(leafKeys, mid), saxt_, co_d)) l = mid;
    else r = mid - 1;
  }
  return l;
}

//待考虑几个平分时分节点有很多d=8的情况
//批量构建while循环内, 2n个
int Zsbtree_Build::buildtree_window(const newVector<NonLeafKey> &leafKeys, int dep) {
  int end = 2 * n - 1;
  saxt_only first_saxt = get_saxt_i(leafKeys, 0);
  saxt_only last_saxt = get_saxt_i_r(leafKeys, end);
  cod window_co_d = get_co_d_from_saxt(first_saxt, last_saxt);

#if isgreed
  //method2 begin
  cod d1 = window_co_d + 1;
  //先从2n-1往前看,删掉后面的,最多删掉n个
  int new_end = end;
  if (leafKeys[new_end].co_d >= d1) new_end = get_new_end(leafKeys, n, end, last_saxt, d1);
  //从前往后看
  std::vector<method2_node> d1Arr;
  int num = 1;
  int id = 0;
  int nextid = id + m - 1;
  saxt_only now_saxt = first_saxt;
  //跟第n/2个比
  saxt_only now1_saxt = get_saxt_i_r(leafKeys, nextid);
  bool mark = true;
  while (true){
    if (mark){
      if (compare_saxt_d(now_saxt, now1_saxt, d1)) {
        mark = false;
      } else {
        if (leafKeys[nextid].co_d >= d1) id = get_new_end(leafKeys, id + 1, nextid, now1_saxt, d1);
        else id = nextid + 1;
        nextid = id + m - 1;
        if (nextid >= new_end) break;
        now_saxt = get_saxt_i(leafKeys, id);
        now1_saxt = get_saxt_i_r(leafKeys, nextid);
      }
    } else {
      int overid = get_new_end_1(leafKeys, nextid, new_end-1, now1_saxt, d1);
      num = overid - id + 1;
      if (num <= n) {
        saxt_only tmprsaxt = get_saxt_i_r(leafKeys, overid);
        cod tmpd = get_co_d_from_saxt(now_saxt, tmprsaxt, d1);
        d1Arr.push_back({tmpd, id, num, now_saxt, tmprsaxt});
      } else {
        //要找d+2,但这里先直接平分,其实可以从m开始一个一个找最小的区域
        //平分代码
        int tmpnum1 = num / 2;
        int tmpnum2 = num - tmpnum1;
        int id1 = id + tmpnum1;
        saxt_only tmprsaxt = get_saxt_i_r(leafKeys, id1-1);
        cod tmpd = get_co_d_from_saxt(now_saxt, tmprsaxt, d1);
        d1Arr.push_back({tmpd, id, tmpnum1, now_saxt, tmprsaxt});
        saxt_only tmplsaxt = get_saxt_i(leafKeys, id1);
        tmprsaxt = get_saxt_i_r(leafKeys, overid);
        tmpd = get_co_d_from_saxt(tmplsaxt, tmprsaxt, d1);
        d1Arr.push_back({tmpd, id1, tmpnum2, tmplsaxt, tmprsaxt});
        //找d+2代码，遍历中间每一个，寻找d最大的方案，可能有3个就再分
        //可能的区间 1000000才触发18次，太少了，不如平分
        /*
                  saxt_only tmplastsaxt = get_saxt_i(leafKeys, i-1);
                  int best_mid_id = getbestmid(leafKeys, n, m, id, num, d1, now_saxt, tmplastsaxt);
                  int tmpnum1 = best_mid_id-id+1;
                  int tmpnum2 = num-(best_mid_id-id+1);
                  saxt_only tmprsaxt = get_saxt_i(leafKeys, best_mid_id);
                  cod tmpd1 = get_co_d_from_saxt(now_saxt, tmprsaxt, d1);
                  if (tmpnum1<=n) {
                      d1Arr.push_back({tmpd1, id, tmpnum1, now_saxt, tmprsaxt});
                  } else {
                      //再分
                      int best_mid_id_ = getbestmid(leafKeys, n, m, id, tmpnum1, tmpd1, now_saxt, tmprsaxt);
                      int tmpnum1_ = best_mid_id_-id+1;
                      int tmpnum2_ = tmpnum1-tmpnum1_;
                      saxt_only tmprsaxt_ = get_saxt_i(leafKeys, best_mid_id_);
                      cod tmpd1_ = get_co_d_from_saxt(now_saxt, tmprsaxt_, tmpd1);
                      d1Arr.push_back({tmpd1_, id, tmpnum1_, now_saxt, tmprsaxt_});
                      saxt_only tmplsaxt_ = get_saxt_i(leafKeys, best_mid_id_+1);
                      cod tmpd2_ = get_co_d_from_saxt(tmplsaxt_, tmprsaxt, tmpd1);
                      d1Arr.push_back({tmpd2_, best_mid_id_+1, tmpnum2_, tmplsaxt_, tmprsaxt});
                  }
                  saxt_only tmplsaxt = get_saxt_i(leafKeys, best_mid_id+1);
                  cod tmpd2 = get_co_d_from_saxt(tmplsaxt, tmplastsaxt, d1);
                  if (tmpnum2<=n) {
                      d1Arr.push_back({tmpd2, best_mid_id+1, tmpnum2, tmplsaxt, tmplastsaxt});
                  } else {
                      //再分
                      int best_mid_id_ = getbestmid(leafKeys, n, m, best_mid_id+1, tmpnum2, tmpd2, tmplsaxt, tmplastsaxt);
                      int tmpnum1_ = best_mid_id_-best_mid_id;
                      int tmpnum2_ = tmpnum2-tmpnum1_;
                      saxt_only tmprsaxt_ = get_saxt_i(leafKeys, best_mid_id_);
                      cod tmpd1_ = get_co_d_from_saxt(tmplsaxt, tmprsaxt_, tmpd2);
                      d1Arr.push_back({tmpd1_, best_mid_id+1, tmpnum1_, tmplsaxt, tmprsaxt_});
                      saxt_only tmplsaxt_ = get_saxt_i(leafKeys, best_mid_id_+1);
                      cod tmpd2_ = get_co_d_from_saxt(tmplsaxt_, tmplastsaxt, tmpd2);
                      d1Arr.push_back({tmpd2_, best_mid_id_+1, tmpnum2_, tmplsaxt_, tmplastsaxt});
                  }
                  */
      }
      id = overid + 1;
      nextid = id + m - 1;
      if (nextid >= new_end) break;
      now_saxt = get_saxt_i(leafKeys, id);
      now1_saxt = get_saxt_i_r(leafKeys, nextid);
      mark = true;
    }
  }
  /*
  for(int i=0;; i++) {
    if (mark) {
      if (i>=new_end-m+1) break;
      if (compare_saxt_d(now_saxt, now1_saxt, d1)) {
        num = m;
        id = i;
        mark = false;
        now1_saxt = get_saxt_i_r(leafKeys, i+m);
        i = i + m - 1;
      } else {
        now_saxt = get_saxt_i(leafKeys, i+1);
        now1_saxt = get_saxt_i_r(leafKeys, i+m);
      }
    } else {
      if (i>=new_end) {
        if (num <= n) {
          saxt_only tmpsaxt = get_saxt_i_r(leafKeys, i-1);
          cod tmpd = get_co_d_from_saxt(now_saxt, tmpsaxt, d1);
          d1Arr.push_back({tmpd, id, num, now_saxt, tmpsaxt});
        } else {
          //要找d+2,但这里先直接平分,其实可以从m开始一个一个找最小的区域
          //平分代码
          int tmpnum1 = num / 2;
          int tmpnum2 = num - tmpnum1;
          int id1 = id + tmpnum1;
          saxt_only tmprsaxt = get_saxt_i_r(leafKeys, id1-1);
          cod tmpd = get_co_d_from_saxt(now_saxt, tmprsaxt, d1);
          d1Arr.push_back({tmpd, id, tmpnum1, now_saxt, tmprsaxt});
          saxt_only tmplsaxt = get_saxt_i(leafKeys, id1);
          tmprsaxt = get_saxt_i_r(leafKeys, i-1);
          tmpd = get_co_d_from_saxt(tmplsaxt, tmprsaxt, d1);
          d1Arr.push_back({tmpd, id1, tmpnum2, tmplsaxt, tmprsaxt});
          //找d+2代码，遍历中间每一个，寻找d最大的方案，可能有3个就再分
          //可能的区间 1000000才触发18次，太少了，不如平分
//                    saxt_only tmplastsaxt = get_saxt_i(leafKeys, i-1);
//                    int best_mid_id = getbestmid(leafKeys, n, m, id, num, d1, now_saxt, tmplastsaxt);
//                    int tmpnum1 = best_mid_id-id+1;
//                    int tmpnum2 = num-(best_mid_id-id+1);
//                    saxt_only tmprsaxt = get_saxt_i(leafKeys, best_mid_id);
//                    cod tmpd1 = get_co_d_from_saxt(now_saxt, tmprsaxt, d1);
//                    if (tmpnum1<=n) {
//                        d1Arr.push_back({tmpd1, id, tmpnum1, now_saxt, tmprsaxt});
//                    } else {
//                        //再分
//                        int best_mid_id_ = getbestmid(leafKeys, n, m, id, tmpnum1, tmpd1, now_saxt, tmprsaxt);
//                        int tmpnum1_ = best_mid_id_-id+1;
//                        int tmpnum2_ = tmpnum1-tmpnum1_;
//                        saxt_only tmprsaxt_ = get_saxt_i(leafKeys, best_mid_id_);
//                        cod tmpd1_ = get_co_d_from_saxt(now_saxt, tmprsaxt_, tmpd1);
//                        d1Arr.push_back({tmpd1_, id, tmpnum1_, now_saxt, tmprsaxt_});
//                        saxt_only tmplsaxt_ = get_saxt_i(leafKeys, best_mid_id_+1);
//                        cod tmpd2_ = get_co_d_from_saxt(tmplsaxt_, tmprsaxt, tmpd1);
//                        d1Arr.push_back({tmpd2_, best_mid_id_+1, tmpnum2_, tmplsaxt_, tmprsaxt});
//                    }
//                    saxt_only tmplsaxt = get_saxt_i(leafKeys, best_mid_id+1);
//                    cod tmpd2 = get_co_d_from_saxt(tmplsaxt, tmplastsaxt, d1);
//                    if (tmpnum2<=n) {
//                        d1Arr.push_back({tmpd2, best_mid_id+1, tmpnum2, tmplsaxt, tmplastsaxt});
//                    } else {
//                        //再分
//                        int best_mid_id_ = getbestmid(leafKeys, n, m, best_mid_id+1, tmpnum2, tmpd2, tmplsaxt, tmplastsaxt);
//                        int tmpnum1_ = best_mid_id_-best_mid_id;
//                        int tmpnum2_ = tmpnum2-tmpnum1_;
//                        saxt_only tmprsaxt_ = get_saxt_i(leafKeys, best_mid_id_);
//                        cod tmpd1_ = get_co_d_from_saxt(tmplsaxt, tmprsaxt_, tmpd2);
//                        d1Arr.push_back({tmpd1_, best_mid_id+1, tmpnum1_, tmplsaxt, tmprsaxt_});
//                        saxt_only tmplsaxt_ = get_saxt_i(leafKeys, best_mid_id_+1);
//                        cod tmpd2_ = get_co_d_from_saxt(tmplsaxt_, tmplastsaxt, tmpd2);
//                        d1Arr.push_back({tmpd2_, best_mid_id_+1, tmpnum2_, tmplsaxt_, tmplastsaxt});
//                    }
        }
        break;
      }
      if (compare_saxt_d(now_saxt, now1_saxt, d1)) {
        num++;
        now1_saxt = get_saxt_i_r(leafKeys, i+1);
      } else {
        if (num <= n) {
          saxt_only tmpsaxt = get_saxt_i_r(leafKeys, i-1);
          cod tmpd = get_co_d_from_saxt(now_saxt, tmpsaxt, d1);
          d1Arr.push_back({tmpd, id, num, now_saxt, tmpsaxt});
        } else {
          //要找d+2,但这里先直接平分,其实可以从m开始一个一个找最小的区域
          //平分代码
          int tmpnum1 = num / 2;
          int tmpnum2 = num - tmpnum1;
          int id1 = id + tmpnum1;
          saxt_only tmprsaxt = get_saxt_i_r(leafKeys, id1-1);
          cod tmpd = get_co_d_from_saxt(now_saxt, tmprsaxt, d1);
          d1Arr.push_back({tmpd, id, tmpnum1, now_saxt, tmprsaxt});
          saxt_only tmplsaxt = get_saxt_i(leafKeys, id1);
          tmprsaxt = get_saxt_i_r(leafKeys, i-1);
          tmpd = get_co_d_from_saxt(tmplsaxt, tmprsaxt, d1);
          d1Arr.push_back({tmpd, id1, tmpnum2, tmplsaxt, tmprsaxt});
          //找d+2代码，遍历中间每一个，寻找d最大的方案
          //可能的区间
//                    saxt_only tmplastsaxt = get_saxt_i(leafKeys, i-1);
//                    int best_mid_id = getbestmid(leafKeys, n, m, id, num, d1, now_saxt, tmplastsaxt);
//                    int tmpnum1 = best_mid_id-id+1;
//                    int tmpnum2 = num-(best_mid_id-id+1);
//                    saxt_only tmprsaxt = get_saxt_i(leafKeys, best_mid_id);
//                    cod tmpd1 = get_co_d_from_saxt(now_saxt, tmprsaxt, d1);
//                    if (tmpnum1<=n) {
//                        d1Arr.push_back({tmpd1, id, tmpnum1, now_saxt, tmprsaxt});
//                    } else {
//                        //再分
//                        int best_mid_id_ = getbestmid(leafKeys, n, m, id, tmpnum1, tmpd1, now_saxt, tmprsaxt);
//                        int tmpnum1_ = best_mid_id_-id+1;
//                        int tmpnum2_ = tmpnum1-tmpnum1_;
//                        saxt_only tmprsaxt_ = get_saxt_i(leafKeys, best_mid_id_);
//                        cod tmpd1_ = get_co_d_from_saxt(now_saxt, tmprsaxt_, tmpd1);
//                        d1Arr.push_back({tmpd1_, id, tmpnum1_, now_saxt, tmprsaxt_});
//                        saxt_only tmplsaxt_ = get_saxt_i(leafKeys, best_mid_id_+1);
//                        cod tmpd2_ = get_co_d_from_saxt(tmplsaxt_, tmprsaxt, tmpd1);
//                        d1Arr.push_back({tmpd2_, best_mid_id_+1, tmpnum2_, tmplsaxt_, tmprsaxt});
//                    }
//                    saxt_only tmplsaxt = get_saxt_i(leafKeys, best_mid_id+1);
//                    cod tmpd2 = get_co_d_from_saxt(tmplsaxt, tmplastsaxt, d1);
//                    if (tmpnum2<=n) {
//                        d1Arr.push_back({tmpd2, best_mid_id+1, tmpnum2, tmplsaxt, tmplastsaxt});
//                    } else {
//                        //再分
//                        int best_mid_id_ = getbestmid(leafKeys, n, m, best_mid_id+1, tmpnum2, tmpd2, tmplsaxt, tmplastsaxt);
//                        int tmpnum1_ = best_mid_id_-best_mid_id;
//                        int tmpnum2_ = tmpnum2-tmpnum1_;
//                        saxt_only tmprsaxt_ = get_saxt_i(leafKeys, best_mid_id_);
//                        cod tmpd1_ = get_co_d_from_saxt(tmplsaxt, tmprsaxt_, tmpd2);
//                        d1Arr.push_back({tmpd1_, best_mid_id+1, tmpnum1_, tmplsaxt, tmprsaxt_});
//                        saxt_only tmplsaxt_ = get_saxt_i(leafKeys, best_mid_id_+1);
//                        cod tmpd2_ = get_co_d_from_saxt(tmplsaxt_, tmplastsaxt, tmpd2);
//                        d1Arr.push_back({tmpd2_, best_mid_id_+1, tmpnum2_, tmplsaxt_, tmplastsaxt});
//                    }
        }
        mark = true;
        if (i>=new_end-m+1) break;
        now_saxt = get_saxt_i(leafKeys, i);;
        i--;
        now1_saxt = get_saxt_i_r(leafKeys, i+m);
      }
    }
  }
   */
  //method2 end
  //构建叶子结点和非叶子的索引点
  int todoid = 0;
  for(method2_node anode: d1Arr) {
    int todonum = anode.id - todoid;
    if (todonum == 0) {
//            out("laile");
      build_leaf_and_nonleafkey(leafKeys, todoid, anode.num, anode.co_d, anode.lsaxt, anode.rsaxt, dep);
//            out("laile2");
      todoid += anode.num;
      continue;
    }
    //前面小范围的
    saxt_only re_lsaxt = get_saxt_i(leafKeys, todoid);
    saxt_only re_rsaxt = get_saxt_i_r(leafKeys, anode.id-1);
    if (todonum <= m) {
      if (nonleafkeys[dep].empty_add()){
        //只看后面
        //如果后面是d+1且加起来<=n，则合起来，不然不要了。这里直接合
        int tmpnum = todonum + anode.num;
        if (tmpnum <= n){
          build_leaf_and_nonleafkey(leafKeys, todoid, tmpnum, window_co_d, re_lsaxt, anode.rsaxt, dep);
          todoid += tmpnum;
          continue;
        } else {
          //构造leaf
          build_leaf_and_nonleafkey_two(leafKeys, todoid, tmpnum, window_co_d, re_lsaxt, anode.rsaxt, dep);
          todoid += tmpnum;
          continue;
        }
      } else {
        //要看前面
        cod preco_d = nonleafkeys[dep].back_add()->co_d;
        saxt_only prelsaxt = nonleafkeys[dep].back_add()->lsaxt;
        int prenum = nonleafkeys[dep].back_add()->num;

        cod nextco_d = anode.co_d;
        int nextnum = anode.num;
        //先对比下降程度
        cod co_d1;
        if (todoid == 0) co_d1 = get_co_d_from_saxt(prelsaxt, re_rsaxt);
        else co_d1 = get_co_d_from_saxt(prelsaxt, re_rsaxt, window_co_d);
        cod co_d2 = get_co_d_from_saxt(re_lsaxt, anode.rsaxt, window_co_d);
        if ((preco_d - co_d1) < (nextco_d - co_d2)) {
          //跟前面合
          int tmpnum = prenum + todonum;
          if (tmpnum <= n) {
            add_nonleafkey(leafKeys, todoid, todonum, co_d1, re_rsaxt, dep);
          } else {
            //平分
            split_nonleafkey(leafKeys, todoid, tmpnum, todonum, co_d1, re_rsaxt, dep);
          }
          build_leaf_and_nonleafkey(leafKeys, anode.id, anode.num, anode.co_d, anode.lsaxt, anode.rsaxt, dep);
          todoid = todoid + todonum + anode.num;
          continue;
        } else if ((preco_d - co_d1) > (nextco_d - co_d2)) {
          //跟后面合
          int tmpnum = nextnum + todonum;
          if (tmpnum <= n) {
            build_leaf_and_nonleafkey(leafKeys, todoid, tmpnum, co_d2, re_lsaxt, anode.rsaxt, dep);
            todoid += tmpnum;
            continue;
          } else {
            //平分
            build_leaf_and_nonleafkey_two(leafKeys, todoid, tmpnum, co_d2, re_lsaxt, anode.rsaxt, dep);
            todoid += tmpnum;
            continue;
          }
        } else {
          //再比大小
          int tmpnum1 = prenum + todonum;
          int tmpnum2 = nextnum + todonum;
          if (tmpnum1 <= n && tmpnum2 > n) {
            add_nonleafkey(leafKeys, todoid, todonum, co_d1, re_rsaxt, dep);
            build_leaf_and_nonleafkey(leafKeys, anode.id, anode.num, anode.co_d, anode.lsaxt, anode.rsaxt, dep);
            todoid = todoid + todonum + anode.num;
            continue;
          } else if (tmpnum1 > n && tmpnum2 <= n) {
            build_leaf_and_nonleafkey(leafKeys, todoid, tmpnum2, co_d2, re_lsaxt, anode.rsaxt, dep);
            todoid += tmpnum2;
            continue;
          } else if (tmpnum1 >n && tmpnum2 > n){
            // 比相聚度,跟大的合
            if (co_d2 >= co_d1) {
              build_leaf_and_nonleafkey_two(leafKeys, todoid, tmpnum2, co_d2, re_lsaxt, anode.rsaxt, dep);
              todoid += tmpnum2;
              continue;
            } else {
              split_nonleafkey(leafKeys, todoid, tmpnum1, todonum, co_d1, re_rsaxt, dep);
              build_leaf_and_nonleafkey(leafKeys, anode.id, anode.num, anode.co_d, anode.lsaxt, anode.rsaxt, dep);
              todoid += tmpnum2;
              continue;
            }
          } else {
            // 比相聚度
            if (co_d2 >= co_d1) {
              build_leaf_and_nonleafkey(leafKeys, todoid, tmpnum2, co_d2, re_lsaxt, anode.rsaxt, dep);
              todoid += tmpnum2;
              continue;
            } else {
              add_nonleafkey(leafKeys, todoid, todonum, co_d1, re_rsaxt, dep);
              build_leaf_and_nonleafkey(leafKeys, anode.id, anode.num, anode.co_d, anode.lsaxt, anode.rsaxt, dep);
              todoid += tmpnum2;
              continue;
            }
          }
        }
      }
    } else if (todonum >m && todonum <= n) {
      //其实就是window_co_d
      build_leaf_and_nonleafkey(leafKeys, todoid, todonum, get_co_d_from_saxt(re_lsaxt, re_rsaxt, window_co_d), re_lsaxt, re_rsaxt, dep);
      build_leaf_and_nonleafkey(leafKeys, anode.id, anode.num, anode.co_d, anode.lsaxt, anode.rsaxt, dep);
      todoid = todoid + todonum + anode.num;
      continue;
    } else {
      build_leaf_and_nonleafkey_two(leafKeys, todoid, todonum, window_co_d, re_lsaxt, re_rsaxt, dep);
      build_leaf_and_nonleafkey(leafKeys, anode.id, anode.num, anode.co_d, anode.lsaxt, anode.rsaxt, dep);
      todoid = todoid + todonum + anode.num;
      continue;
    }
  }
  if (todoid == 0) {
    //打包n个
    build_leaf_and_nonleafkey(leafKeys, todoid, n, window_co_d, first_saxt, get_saxt_i_r(leafKeys, n-1), dep);
    todoid += n;
  }
  return todoid;
#else

  build_leaf_and_nonleafkey_two(leafKeys, 0, n*2, window_co_d, first_saxt, last_saxt, dep);
  return n*2;
#endif
}


void Zsbtree_Build::buildtree_window_last(const newVector<NonLeafKey> &leafKeys, int allnum, int dep) {
  assert(allnum>n);
  int end = allnum - 1;
  saxt_only first_saxt = get_saxt_i(leafKeys, 0);
  saxt_only last_saxt = get_saxt_i_r(leafKeys, end);
  cod window_co_d = get_co_d_from_saxt(first_saxt, last_saxt);
#if isgreed
  //method2 begin
  cod d1 = window_co_d + 1;
  int new_end = end;
  //从前往后看
  std::vector<method2_node> d1Arr;
  int num = 1;
  int id = 0;
  int nextid = id + m - 1;
  saxt_only now_saxt = first_saxt;
  //跟第n/2个比
  saxt_only now1_saxt = get_saxt_i_r(leafKeys, nextid);
  bool mark = true;
  while (true){
    if (mark){
      if (compare_saxt_d(now_saxt, now1_saxt, d1)) {
        mark = false;
      } else {
        if (leafKeys[nextid].co_d >= d1) id = get_new_end(leafKeys, id + 1, nextid, now1_saxt, d1);
        else id = nextid + 1;
        nextid = id + m - 1;
        if (nextid >= new_end) break;
        now_saxt = get_saxt_i(leafKeys, id);
        now1_saxt = get_saxt_i_r(leafKeys, nextid);
      }
    } else {
      int overid = get_new_end_1(leafKeys, nextid, new_end-1, now1_saxt, d1);
      num = overid - id + 1;
      if (num <= n) {
        saxt_only tmprsaxt = get_saxt_i_r(leafKeys, overid);
        cod tmpd = get_co_d_from_saxt(now_saxt, tmprsaxt, d1);
        d1Arr.push_back({tmpd, id, num, now_saxt, tmprsaxt});
      } else {
        //要找d+2,但这里先直接平分,其实可以从m开始一个一个找最小的区域
        //平分代码
        int tmpnum1 = num / 2;
        int tmpnum2 = num - tmpnum1;
        int id1 = id + tmpnum1;
        saxt_only tmprsaxt = get_saxt_i_r(leafKeys, id1-1);
        cod tmpd = get_co_d_from_saxt(now_saxt, tmprsaxt, d1);
        d1Arr.push_back({tmpd, id, tmpnum1, now_saxt, tmprsaxt});
        saxt_only tmplsaxt = get_saxt_i(leafKeys, id1);
        tmprsaxt = get_saxt_i_r(leafKeys, overid);
        tmpd = get_co_d_from_saxt(tmplsaxt, tmprsaxt, d1);
        d1Arr.push_back({tmpd, id1, tmpnum2, tmplsaxt, tmprsaxt});
        //找d+2代码，遍历中间每一个，寻找d最大的方案，可能有3个就再分
        //可能的区间 1000000才触发18次，太少了，不如平分
        /*
                  saxt_only tmplastsaxt = get_saxt_i(leafKeys, i-1);
                  int best_mid_id = getbestmid(leafKeys, n, m, id, num, d1, now_saxt, tmplastsaxt);
                  int tmpnum1 = best_mid_id-id+1;
                  int tmpnum2 = num-(best_mid_id-id+1);
                  saxt_only tmprsaxt = get_saxt_i(leafKeys, best_mid_id);
                  cod tmpd1 = get_co_d_from_saxt(now_saxt, tmprsaxt, d1);
                  if (tmpnum1<=n) {
                      d1Arr.push_back({tmpd1, id, tmpnum1, now_saxt, tmprsaxt});
                  } else {
                      //再分
                      int best_mid_id_ = getbestmid(leafKeys, n, m, id, tmpnum1, tmpd1, now_saxt, tmprsaxt);
                      int tmpnum1_ = best_mid_id_-id+1;
                      int tmpnum2_ = tmpnum1-tmpnum1_;
                      saxt_only tmprsaxt_ = get_saxt_i(leafKeys, best_mid_id_);
                      cod tmpd1_ = get_co_d_from_saxt(now_saxt, tmprsaxt_, tmpd1);
                      d1Arr.push_back({tmpd1_, id, tmpnum1_, now_saxt, tmprsaxt_});
                      saxt_only tmplsaxt_ = get_saxt_i(leafKeys, best_mid_id_+1);
                      cod tmpd2_ = get_co_d_from_saxt(tmplsaxt_, tmprsaxt, tmpd1);
                      d1Arr.push_back({tmpd2_, best_mid_id_+1, tmpnum2_, tmplsaxt_, tmprsaxt});
                  }
                  saxt_only tmplsaxt = get_saxt_i(leafKeys, best_mid_id+1);
                  cod tmpd2 = get_co_d_from_saxt(tmplsaxt, tmplastsaxt, d1);
                  if (tmpnum2<=n) {
                      d1Arr.push_back({tmpd2, best_mid_id+1, tmpnum2, tmplsaxt, tmplastsaxt});
                  } else {
                      //再分
                      int best_mid_id_ = getbestmid(leafKeys, n, m, best_mid_id+1, tmpnum2, tmpd2, tmplsaxt, tmplastsaxt);
                      int tmpnum1_ = best_mid_id_-best_mid_id;
                      int tmpnum2_ = tmpnum2-tmpnum1_;
                      saxt_only tmprsaxt_ = get_saxt_i(leafKeys, best_mid_id_);
                      cod tmpd1_ = get_co_d_from_saxt(tmplsaxt, tmprsaxt_, tmpd2);
                      d1Arr.push_back({tmpd1_, best_mid_id+1, tmpnum1_, tmplsaxt, tmprsaxt_});
                      saxt_only tmplsaxt_ = get_saxt_i(leafKeys, best_mid_id_+1);
                      cod tmpd2_ = get_co_d_from_saxt(tmplsaxt_, tmplastsaxt, tmpd2);
                      d1Arr.push_back({tmpd2_, best_mid_id_+1, tmpnum2_, tmplsaxt_, tmplastsaxt});
                  }
                  */
      }
      id = overid + 1;
      nextid = id + m - 1;
      if (nextid >= new_end) break;
      now_saxt = get_saxt_i(leafKeys, id);
      now1_saxt = get_saxt_i_r(leafKeys, nextid);
      mark = true;
    }
  }
/*
  for(int i=0;; i++) {
    if (mark) {
      if (i>=new_end-m+1) break;
      if (compare_saxt_d(now_saxt, now1_saxt, d1)) {
        num = m;
        id = i;
        mark = false;
        now1_saxt = get_saxt_i_r(leafKeys, i+m);
        i = i + m - 1;
      } else {
        now_saxt = get_saxt_i(leafKeys, i+1);
        now1_saxt = get_saxt_i_r(leafKeys, i+m);
      }
    } else {
      if (i>=new_end) {
        if (num <= n) {
          saxt_only tmpsaxt = get_saxt_i_r(leafKeys, i-1);
          cod tmpd = get_co_d_from_saxt(now_saxt, tmpsaxt, d1);
          d1Arr.push_back({tmpd, id, num, now_saxt, tmpsaxt});
        } else {
          //要找d+2,但这里先直接平分,其实可以从m开始一个一个找最小的区域
          //平分代码
          int tmpnum1 = num / 2;
          int tmpnum2 = num - tmpnum1;
          int id1 = id + tmpnum1;
          saxt_only tmprsaxt = get_saxt_i_r(leafKeys, id1-1);
          cod tmpd = get_co_d_from_saxt(now_saxt, tmprsaxt, d1);
          d1Arr.push_back({tmpd, id, tmpnum1, now_saxt, tmprsaxt});
          saxt_only tmplsaxt = get_saxt_i(leafKeys, id1);
          tmprsaxt = get_saxt_i_r(leafKeys, i-1);
          tmpd = get_co_d_from_saxt(tmplsaxt, tmprsaxt, d1);
          d1Arr.push_back({tmpd, id1, tmpnum2, tmplsaxt, tmprsaxt});
          //找d+2代码，遍历中间每一个，寻找d最大的方案，可能有3个就再分
          //可能的区间 1000000才触发18次，太少了，不如平分
//                    saxt_only tmplastsaxt = get_saxt_i(leafKeys, i-1);
//                    int best_mid_id = getbestmid(leafKeys, n, m, id, num, d1, now_saxt, tmplastsaxt);
//                    int tmpnum1 = best_mid_id-id+1;
//                    int tmpnum2 = num-(best_mid_id-id+1);
//                    saxt_only tmprsaxt = get_saxt_i(leafKeys, best_mid_id);
//                    cod tmpd1 = get_co_d_from_saxt(now_saxt, tmprsaxt, d1);
//                    if (tmpnum1<=n) {
//                        d1Arr.push_back({tmpd1, id, tmpnum1, now_saxt, tmprsaxt});
//                    } else {
//                        //再分
//                        int best_mid_id_ = getbestmid(leafKeys, n, m, id, tmpnum1, tmpd1, now_saxt, tmprsaxt);
//                        int tmpnum1_ = best_mid_id_-id+1;
//                        int tmpnum2_ = tmpnum1-tmpnum1_;
//                        saxt_only tmprsaxt_ = get_saxt_i(leafKeys, best_mid_id_);
//                        cod tmpd1_ = get_co_d_from_saxt(now_saxt, tmprsaxt_, tmpd1);
//                        d1Arr.push_back({tmpd1_, id, tmpnum1_, now_saxt, tmprsaxt_});
//                        saxt_only tmplsaxt_ = get_saxt_i(leafKeys, best_mid_id_+1);
//                        cod tmpd2_ = get_co_d_from_saxt(tmplsaxt_, tmprsaxt, tmpd1);
//                        d1Arr.push_back({tmpd2_, best_mid_id_+1, tmpnum2_, tmplsaxt_, tmprsaxt});
//                    }
//                    saxt_only tmplsaxt = get_saxt_i(leafKeys, best_mid_id+1);
//                    cod tmpd2 = get_co_d_from_saxt(tmplsaxt, tmplastsaxt, d1);
//                    if (tmpnum2<=n) {
//                        d1Arr.push_back({tmpd2, best_mid_id+1, tmpnum2, tmplsaxt, tmplastsaxt});
//                    } else {
//                        //再分
//                        int best_mid_id_ = getbestmid(leafKeys, n, m, best_mid_id+1, tmpnum2, tmpd2, tmplsaxt, tmplastsaxt);
//                        int tmpnum1_ = best_mid_id_-best_mid_id;
//                        int tmpnum2_ = tmpnum2-tmpnum1_;
//                        saxt_only tmprsaxt_ = get_saxt_i(leafKeys, best_mid_id_);
//                        cod tmpd1_ = get_co_d_from_saxt(tmplsaxt, tmprsaxt_, tmpd2);
//                        d1Arr.push_back({tmpd1_, best_mid_id+1, tmpnum1_, tmplsaxt, tmprsaxt_});
//                        saxt_only tmplsaxt_ = get_saxt_i(leafKeys, best_mid_id_+1);
//                        cod tmpd2_ = get_co_d_from_saxt(tmplsaxt_, tmplastsaxt, tmpd2);
//                        d1Arr.push_back({tmpd2_, best_mid_id_+1, tmpnum2_, tmplsaxt_, tmplastsaxt});
//                    }
        }
        break;
      }
      if (compare_saxt_d(now_saxt, now1_saxt, d1)) {
        num++;
        now1_saxt = get_saxt_i_r(leafKeys, i+1);
      } else {
        if (num <= n) {
          saxt_only tmpsaxt = get_saxt_i_r(leafKeys, i-1);
          cod tmpd = get_co_d_from_saxt(now_saxt, tmpsaxt, d1);
          d1Arr.push_back({tmpd, id, num, now_saxt, tmpsaxt});
        } else {
          //要找d+2,但这里先直接平分,其实可以从m开始一个一个找最小的区域
          //平分代码
          int tmpnum1 = num / 2;
          int tmpnum2 = num - tmpnum1;
          int id1 = id + tmpnum1;
          saxt_only tmprsaxt = get_saxt_i_r(leafKeys, id1-1);
          cod tmpd = get_co_d_from_saxt(now_saxt, tmprsaxt, d1);
          d1Arr.push_back({tmpd, id, tmpnum1, now_saxt, tmprsaxt});
          saxt_only tmplsaxt = get_saxt_i(leafKeys, id1);
          tmprsaxt = get_saxt_i_r(leafKeys, i-1);
          tmpd = get_co_d_from_saxt(tmplsaxt, tmprsaxt, d1);
          d1Arr.push_back({tmpd, id1, tmpnum2, tmplsaxt, tmprsaxt});
          //找d+2代码，遍历中间每一个，寻找d最大的方案
          //可能的区间
//                    saxt_only tmplastsaxt = get_saxt_i(leafKeys, i-1);
//                    int best_mid_id = getbestmid(leafKeys, n, m, id, num, d1, now_saxt, tmplastsaxt);
//                    int tmpnum1 = best_mid_id-id+1;
//                    int tmpnum2 = num-(best_mid_id-id+1);
//                    saxt_only tmprsaxt = get_saxt_i(leafKeys, best_mid_id);
//                    cod tmpd1 = get_co_d_from_saxt(now_saxt, tmprsaxt, d1);
//                    if (tmpnum1<=n) {
//                        d1Arr.push_back({tmpd1, id, tmpnum1, now_saxt, tmprsaxt});
//                    } else {
//                        //再分
//                        int best_mid_id_ = getbestmid(leafKeys, n, m, id, tmpnum1, tmpd1, now_saxt, tmprsaxt);
//                        int tmpnum1_ = best_mid_id_-id+1;
//                        int tmpnum2_ = tmpnum1-tmpnum1_;
//                        saxt_only tmprsaxt_ = get_saxt_i(leafKeys, best_mid_id_);
//                        cod tmpd1_ = get_co_d_from_saxt(now_saxt, tmprsaxt_, tmpd1);
//                        d1Arr.push_back({tmpd1_, id, tmpnum1_, now_saxt, tmprsaxt_});
//                        saxt_only tmplsaxt_ = get_saxt_i(leafKeys, best_mid_id_+1);
//                        cod tmpd2_ = get_co_d_from_saxt(tmplsaxt_, tmprsaxt, tmpd1);
//                        d1Arr.push_back({tmpd2_, best_mid_id_+1, tmpnum2_, tmplsaxt_, tmprsaxt});
//                    }
//                    saxt_only tmplsaxt = get_saxt_i(leafKeys, best_mid_id+1);
//                    cod tmpd2 = get_co_d_from_saxt(tmplsaxt, tmplastsaxt, d1);
//                    if (tmpnum2<=n) {
//                        d1Arr.push_back({tmpd2, best_mid_id+1, tmpnum2, tmplsaxt, tmplastsaxt});
//                    } else {
//                        //再分
//                        int best_mid_id_ = getbestmid(leafKeys, n, m, best_mid_id+1, tmpnum2, tmpd2, tmplsaxt, tmplastsaxt);
//                        int tmpnum1_ = best_mid_id_-best_mid_id;
//                        int tmpnum2_ = tmpnum2-tmpnum1_;
//                        saxt_only tmprsaxt_ = get_saxt_i(leafKeys, best_mid_id_);
//                        cod tmpd1_ = get_co_d_from_saxt(tmplsaxt, tmprsaxt_, tmpd2);
//                        d1Arr.push_back({tmpd1_, best_mid_id+1, tmpnum1_, tmplsaxt, tmprsaxt_});
//                        saxt_only tmplsaxt_ = get_saxt_i(leafKeys, best_mid_id_+1);
//                        cod tmpd2_ = get_co_d_from_saxt(tmplsaxt_, tmplastsaxt, tmpd2);
//                        d1Arr.push_back({tmpd2_, best_mid_id_+1, tmpnum2_, tmplsaxt_, tmplastsaxt});
//                    }
        }
        mark = true;
        if (i>=new_end-m+1) break;
        now_saxt = get_saxt_i(leafKeys, i);;
        i--;
        now1_saxt = get_saxt_i_r(leafKeys, i+m);
      }
    }
  }
  */
  //method2 end
  //构建叶子结点和非叶子的索引点
  int todoid = 0;
  for(method2_node anode: d1Arr) {
    int todonum = anode.id - todoid;
    if (todonum == 0) {
//            out("laile");
      build_leaf_and_nonleafkey(leafKeys, todoid, anode.num, anode.co_d, anode.lsaxt, anode.rsaxt, dep);
//            out("laile2");
      todoid += anode.num;
      continue;
    }
    //前面小范围的
    saxt_only re_lsaxt = get_saxt_i(leafKeys, todoid);
    saxt_only re_rsaxt = get_saxt_i_r(leafKeys, anode.id-1);
    if (todonum <= m) {
      if (nonleafkeys[dep].empty_add()){
        //只看后面
        //如果后面是d+1且加起来<=n，则合起来，不然不要了。这里直接合
        int tmpnum = todonum + anode.num;
        if (tmpnum <= n){
          build_leaf_and_nonleafkey(leafKeys, todoid, tmpnum, window_co_d, re_lsaxt, anode.rsaxt, dep);
          todoid += tmpnum;
          continue;
        } else {
          //构造leaf
          build_leaf_and_nonleafkey_two(leafKeys, todoid, tmpnum, window_co_d, re_lsaxt, anode.rsaxt, dep);
          todoid += tmpnum;
          continue;
        }
      } else {
        //要看前面
        cod preco_d = nonleafkeys[dep].back_add()->co_d;
        saxt_only prelsaxt = nonleafkeys[dep].back_add()->lsaxt;
        int prenum = nonleafkeys[dep].back_add()->num;

        cod nextco_d = anode.co_d;
        int nextnum = anode.num;
        //先对比下降程度
        cod co_d1;
        if (todoid == 0) co_d1 = get_co_d_from_saxt(prelsaxt, re_rsaxt);
        else co_d1 = get_co_d_from_saxt(prelsaxt, re_rsaxt, window_co_d);
        cod co_d2 = get_co_d_from_saxt(re_lsaxt, anode.rsaxt, window_co_d);
        if ((preco_d - co_d1) < (nextco_d - co_d2)) {
          //跟前面合
          int tmpnum = prenum + todonum;
          if (tmpnum <= n) {
            add_nonleafkey(leafKeys, todoid, todonum, co_d1, re_rsaxt, dep);
          } else {
            //平分
            split_nonleafkey(leafKeys, todoid, tmpnum, todonum, co_d1, re_rsaxt, dep);
          }
          build_leaf_and_nonleafkey(leafKeys, anode.id, anode.num, anode.co_d, anode.lsaxt, anode.rsaxt, dep);
          todoid = todoid + todonum + anode.num;
          continue;
        } else if ((preco_d - co_d1) > (nextco_d - co_d2)) {
          //跟后面合
          int tmpnum = nextnum + todonum;
          if (tmpnum <= n) {
            build_leaf_and_nonleafkey(leafKeys, todoid, tmpnum, co_d2, re_lsaxt, anode.rsaxt, dep);
            todoid += tmpnum;
            continue;
          } else {
            //平分
            build_leaf_and_nonleafkey_two(leafKeys, todoid, tmpnum, co_d2, re_lsaxt, anode.rsaxt, dep);
            todoid += tmpnum;
            continue;
          }
        } else {
          //再比大小
          int tmpnum1 = prenum + todonum;
          int tmpnum2 = nextnum + todonum;
          if (tmpnum1 <= n && tmpnum2 > n) {
            add_nonleafkey(leafKeys, todoid, todonum, co_d1, re_rsaxt, dep);
            build_leaf_and_nonleafkey(leafKeys, anode.id, anode.num, anode.co_d, anode.lsaxt, anode.rsaxt, dep);
            todoid = todoid + todonum + anode.num;
            continue;
          } else if (tmpnum1 > n && tmpnum2 <= n) {
            build_leaf_and_nonleafkey(leafKeys, todoid, tmpnum2, co_d2, re_lsaxt, anode.rsaxt, dep);
            todoid += tmpnum2;
            continue;
          } else if (tmpnum1 >n && tmpnum2 > n){
            // 比相聚度,跟大的合
            if (co_d2 >= co_d1) {
              build_leaf_and_nonleafkey_two(leafKeys, todoid, tmpnum2, co_d2, re_lsaxt, anode.rsaxt, dep);
              todoid += tmpnum2;
              continue;
            } else {
              split_nonleafkey(leafKeys, todoid, tmpnum1, todonum, co_d1, re_rsaxt, dep);
              build_leaf_and_nonleafkey(leafKeys, anode.id, anode.num, anode.co_d, anode.lsaxt, anode.rsaxt, dep);
              todoid += tmpnum2;
              continue;
            }
          } else {
            // 比相聚度
            if (co_d2 >= co_d1) {
              build_leaf_and_nonleafkey(leafKeys, todoid, tmpnum2, co_d2, re_lsaxt, anode.rsaxt, dep);
              todoid += tmpnum2;
              continue;
            } else {
              add_nonleafkey(leafKeys, todoid, todonum, co_d1, re_rsaxt, dep);
              build_leaf_and_nonleafkey(leafKeys, anode.id, anode.num, anode.co_d, anode.lsaxt, anode.rsaxt, dep);
              todoid += tmpnum2;
              continue;
            }
          }
        }
      }
    } else if (todonum >m && todonum <= n) {
      //其实就是window_co_d
      build_leaf_and_nonleafkey(leafKeys, todoid, todonum, get_co_d_from_saxt(re_lsaxt, re_rsaxt, window_co_d), re_lsaxt, re_rsaxt, dep);
      build_leaf_and_nonleafkey(leafKeys, anode.id, anode.num, anode.co_d, anode.lsaxt, anode.rsaxt, dep);
      todoid = todoid + todonum + anode.num;
      continue;
    } else {
      build_leaf_and_nonleafkey_two(leafKeys, todoid, todonum, window_co_d, re_lsaxt, re_rsaxt, dep);
      build_leaf_and_nonleafkey(leafKeys, anode.id, anode.num, anode.co_d, anode.lsaxt, anode.rsaxt, dep);
      todoid = todoid + todonum + anode.num;
      continue;
    }
  }
  int todonum = allnum - todoid;
  if (todonum > n) {
    //直接平分打包
    saxt_only lsaxt = get_saxt_i(leafKeys, todoid);
    build_leaf_and_nonleafkey_two(leafKeys, todoid, todonum, window_co_d, lsaxt, last_saxt, dep);
  } else if (todonum > m && todonum <= n) {
    //直接打包
    saxt_only lsaxt = get_saxt_i(leafKeys, todoid);
    build_leaf_and_nonleafkey(leafKeys, todoid, todonum, window_co_d, lsaxt, last_saxt, dep);
  } else if (todonum > 0) {
    if (!nonleafkeys[dep].empty_add()) {
      //跟前面合
      saxt_only prelsaxt = nonleafkeys[dep].back_add()->lsaxt;
      int prenum = nonleafkeys[dep].back_add()->num;
      int tmpnum = prenum + todonum;
      cod co_d1;
      if (todoid == 0) co_d1 = get_co_d_from_saxt(prelsaxt, last_saxt);
      else co_d1 = get_co_d_from_saxt(prelsaxt, last_saxt, window_co_d);
      if (tmpnum <= n) {
        add_nonleafkey(leafKeys, todoid, todonum, co_d1, last_saxt, dep);
      } else {
        //平分
        split_nonleafkey(leafKeys, todoid, tmpnum, todonum, co_d1, last_saxt, dep);
      }
    } else {
      //前面一个点没有直接打包
      build_leaf_and_nonleafkey(leafKeys, todoid, todonum, window_co_d, first_saxt, last_saxt, dep);
    }
  }
#else
  build_leaf_and_nonleafkey_two(leafKeys, 0, allnum, window_co_d, first_saxt, last_saxt, dep);
#endif
}

Zsbtree_Build::~Zsbtree_Build() {
  free(leafkeys_rep);
  for(auto item: nonleafkeys_rep){
    free(item);
  }
}


Zsbtree_Build_Mem::Zsbtree_Build_Mem(int max_size, int min_size) : Zsbtree_Build(max_size, min_size) {}

void Zsbtree_Build_Mem::doleaf(NonLeafKey* nonLeafKey) {
  Leaf *leaf = new Leaf(nonLeafKey->num, nonLeafKey->co_d, nonLeafKey->lsaxt, nonLeafKey->rsaxt, (LeafKey*)nonLeafKey->p);
  nonLeafKey->p = leaf;
}
void Zsbtree_Build_Mem::dononleaf(NonLeafKey* nonLeafKey, bool isleaf) {
  NonLeaf *leaf = new NonLeaf(nonLeafKey->num, nonLeafKey->co_d, isleaf, nonLeafKey->lsaxt, nonLeafKey->rsaxt, (NonLeafKey*)nonLeafKey->p);
  nonLeafKey->p = leaf;
}







