//
// Created by hh on 2022/11/26.
//

#ifndef TODOZSBTREE_ZSBTREE_BUILD_H
#define TODOZSBTREE_ZSBTREE_BUILD_H

#include "zsbtree_Insert.h"
#include "zsbtree_LeafBuild.h"
#include "zsbtree_NonLeafBuild.h"

typedef struct {
  NonLeaf* root;
  int leafNum;
} zsbtree_table_mem;

static NonLeaf* build_tree_from_leaf(newVector<LeafKey> &leafKeys, const int n, const int m, int &leafNum) {
  vector<NonLeafKey> nonLeafKeys[2];

  leaf_method::buildtree(leafKeys, nonLeafKeys[0], n, m);
  leafNum = nonLeafKeys[0].size();


  bool isleaf = true;
  int out_1 = 0;
  while (isleaf || nonLeafKeys[out_1].size()>Leaf_maxnum) {
    newVector<NonLeafKey> nonleafKeys_in(nonLeafKeys[out_1]);

    nonleaf_method::buildtree(nonleafKeys_in, nonLeafKeys[1-out_1], isleaf, Leaf_maxnum, Leaf_minnum);
    if (isleaf) isleaf = false;
    nonLeafKeys[out_1].clear();
    out_1 = 1 - out_1;
//        for (int i=0;i<10;i++) {
//            out("l");
//            saxt_print(((Leaf *)(nonleafKeys_in[i].p))->lsaxt);
//            out("r");
//            saxt_print(((Leaf *)(nonleafKeys_in[i].p))->rsaxt);
//        }
  }
  if (nonLeafKeys[out_1].size() == 1) {
    return (NonLeaf*)nonLeafKeys[out_1][0].p;
  } else {
    saxt_only lsaxt = nonLeafKeys[out_1][0].lsaxt;
    saxt_only rsaxt = nonLeafKeys[out_1].back().rsaxt;
    cod co_d = get_co_d_from_saxt(lsaxt, rsaxt);
    return new NonLeaf(nonLeafKeys[out_1].size(), co_d, isleaf, lsaxt, rsaxt, nonLeafKeys[out_1].data());
  }
}

static NonLeaf* build_tree_from_nonleaf(newVector<NonLeafKey> &nonLeafKeys) {

  vector<NonLeafKey> nonLeafKeys_rep[2];

  int out_1 = 1;
  bool isleaf = true;
  while (isleaf || nonLeafKeys_rep[out_1].size()>Leaf_maxnum) {
    if (isleaf) {
      nonleaf_method::buildtree(nonLeafKeys, nonLeafKeys_rep[1 - out_1], isleaf, Leaf_maxnum, Leaf_minnum);
      out_1 = 1 - out_1;
      isleaf = false;
    } else {
      newVector<NonLeafKey> nonleafKeys_in(nonLeafKeys_rep[out_1]);
      //创建下一层
      nonleaf_method::buildtree(nonleafKeys_in, nonLeafKeys_rep[1 - out_1], isleaf, Leaf_maxnum, Leaf_minnum);
      nonLeafKeys_rep[out_1].clear();
      out_1 = 1 - out_1;
    }
//        for (int i=0;i<10;i++) {
//            out("l");
//            saxt_print(((Leaf *)(nonleafKeys_in[i].p))->lsaxt);
//            out("r");
//            saxt_print(((Leaf *)(nonleafKeys_in[i].p))->rsaxt);
//        }
  }
  if (nonLeafKeys_rep[out_1].size() == 1) {
    return (NonLeaf*)nonLeafKeys_rep[out_1][0].p;
  } else {
    saxt_only lsaxt = nonLeafKeys_rep[out_1][0].lsaxt;
    saxt_only rsaxt = nonLeafKeys_rep[out_1].back().rsaxt;
    cod co_d = get_co_d_from_saxt(lsaxt, rsaxt);
    return new NonLeaf(nonLeafKeys_rep[out_1].size(), co_d, isleaf, lsaxt, rsaxt, nonLeafKeys_rep[out_1].data());
  }
}


static zsbtree_table_mem BuildTree_new(newVector<NonLeafKey>& nonLeafKeys) {
  return {build_tree_from_nonleaf(nonLeafKeys), (int)(nonLeafKeys.size())};
}

static inline int get_1_Num(int x) {
  int res = 0;
  while(x) x -= x & (-x) , res++;
  return res;
}


static vector<pair<int, int>> get_drange_rebalance(vector<int> &memNum_period){
  int m = memNum_period.size();
  //平均值
  float nd = 0;
  for(auto item: memNum_period) {
    nd += item;
  }
  nd /= m;
  vector<bool> istoRebalance;
  istoRebalance.reserve(m + 1);
  istoRebalance.push_back(false);
  //从低到高
  bool mark = false;
  for(auto item: memNum_period) {
    if (abs(item - nd) > 0.2 * nd) mark = true, istoRebalance.push_back(true);
    else istoRebalance.push_back(false);
  }
  //本来就平衡了
  vector<pair<int, int>> res;
  if (!mark) return res;

  //首先求前缀和
  vector<int> prefix;
  prefix.reserve(m + 1);
  prefix.push_back(0);
  for(auto item: memNum_period) {
    prefix.push_back(prefix.back() + item);
  }

//    for (int j = 1; j < m+1; ++j) {
//        cout<<istoRebalance[j]<<endl;
//    }

  const int INF = 0x3f3f3f3f;
  //数量
  int* f = (int*)malloc(sizeof(int) * (m+1));
  //从哪个状态转移的
  int* g = (int*)malloc(sizeof(int) * (m+1));
  f[0] = 0;
  for(int i=1;i<=m;i++){
    if (!istoRebalance[i]) {
      //不选
      f[i] = f[i-1];
      g[i] = i-1;
      //选
      for(int l=1;l<i;l++){
        int addnum = i - l + 1;
        if ((float )abs((prefix[i] - prefix[l-1])/addnum - nd) > 0.2 * nd) continue;
        if (addnum + f[l-1] < f[i]) {
          f[i] = addnum + f[l-1];
          g[i] = l-1;
        }
      }

    } else {
      f[i] = INF;
      for(int l=1;l<i;l++){
        int addnum = i - l + 1;
        if ((float )abs((prefix[i] - prefix[l-1])/addnum - nd) > 0.2 * nd) continue;
        if (addnum + f[l-1] < f[i]) {
          f[i] = addnum + f[l-1];
          g[i] = l-1;
        }
      }
    }
  }

  for(int i = m;i; i = g[i]){
    if (i - g[i] >= 2) {
      res.emplace_back(g[i], i-1);
    }
  }
  reverse(res.begin(), res.end());
  free(f);
  free(g);
  return res;
}


//省空间模式
class Zsbtree_Build {
 public:
  Zsbtree_Build(int max_size, int min_size);

  void Add(LeafKey& leafkey);

  void Add(NonLeafKey& nonLeafKey, int dep);

  void Emplace(int num, cod co_d, saxt_only lsaxt, saxt_only rsaxt, void* leaf, int dep);

  void finish();



  NonLeafKey* GetRootKey();

  LeafKey* GetLastLeafKey();

  ~Zsbtree_Build();


 protected:
  //new或者存磁盘，但是不移动
  virtual void doleaf(NonLeafKey* nonLeafKey) = 0;
  virtual void dononleaf(NonLeafKey* nonLeafKey, bool isleaf) = 0;
 private:

  inline saxt_only get_saxt_i(newVector<LeafKey> &leafKeys, int i);
  inline void build_leaf_and_nonleafkey(newVector<LeafKey> &leafKeys, int id,
                                        int num, cod co_d, saxt_only lsaxt, saxt_only rsaxt);
  inline void build_leaf_and_nonleafkey_two(newVector<LeafKey> &leafKeys, int id,
                                            int num, cod co_d, saxt_only lsaxt, saxt_only rsaxt);
  inline void add_nonleafkey(newVector<LeafKey> &leafKeys, int id,
                             int num, cod co_d, saxt_only rsaxt);
  inline void split_nonleafkey(newVector<LeafKey> &leafKeys, int id, int allnum,
                               int num, cod co_d, saxt_only rsaxt);
  inline int getbestmid(newVector<LeafKey> &leafKeys, int id, int num, cod d1, saxt_only now_saxt, saxt_only tmplastsaxt);
  inline int get_new_end(newVector<LeafKey> &leafKeys, int l, int r, saxt_only saxt_, cod co_d);
  inline int get_new_end_1(newVector<LeafKey> &leafKeys, int l, int r, saxt_only saxt_, cod co_d);
  int buildtree_window(newVector<LeafKey> &leafKeys);
  void buildtree_window_last(newVector<LeafKey> &leafKeys, int allnum);

  inline saxt_only get_saxt_i(const newVector<NonLeafKey> &leafKeys, int i);
  inline saxt_only get_saxt_i_r(const newVector<NonLeafKey> &leafKeys, int i);
  inline void build_leaf_and_nonleafkey(const newVector<NonLeafKey> &leafKeys, int id,
                                        int num, cod co_d, saxt_only lsaxt, saxt_only rsaxt, int dep);
  inline void build_leaf_and_nonleafkey_two(const newVector<NonLeafKey> &leafKeys, int id,
                                            int num, cod co_d, saxt_only lsaxt, saxt_only rsaxt, int dep);
  inline void add_nonleafkey(const newVector<NonLeafKey> &leafKeys, int id,
                             int num, cod co_d, saxt_only rsaxt, int dep);
  inline void split_nonleafkey(const newVector<NonLeafKey> &leafKeys, int id, int allnum,
                               int num, cod co_d, saxt_only rsaxt, int dep);
  inline int getbestmid(const newVector<NonLeafKey> &leafKeys, int id, int num, cod d1, saxt_only now_saxt, saxt_only tmplastsaxt);
  inline int get_new_end(const newVector<NonLeafKey> &leafKeys, int l, int r, saxt_only saxt_, cod co_d);
  inline int get_new_end_1(const newVector<NonLeafKey> &leafKeys, int l, int r, saxt_only saxt_, cod co_d);
  int buildtree_window(const newVector<NonLeafKey> &leafKeys, int dep);
  void buildtree_window_last(const newVector<NonLeafKey> &leafKeys, int allnum, int dep);

 public:
  typedef struct {
    cod co_d;
    int id;
    int num;
    saxt_only lsaxt;
    saxt_only rsaxt;
  } method2_node;

  int n;
  int m;
  newVector<LeafKey> leafkeys;
  vector<newVector<NonLeafKey> > nonleafkeys;

//  vector<LeafKey> leafkeys_rep;
  LeafKey* leafkeys_rep;
  vector<NonLeafKey*> nonleafkeys_rep;

};


class Zsbtree_Build_Mem : public Zsbtree_Build{
  void doleaf(NonLeafKey* nonLeafKey) override ;
  void dononleaf(NonLeafKey* nonLeafKey, bool isleaf) override;

 public:
  Zsbtree_Build_Mem(int max_size, int min_size);
};







#endif //TODOZSBTREE_ZSBTREE_BUILD_H
