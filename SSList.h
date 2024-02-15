//
// Created by 21411 on 2023/4/19.
//

#ifndef LSMKV_SSLIST_H
#define LSMKV_SSLIST_H
#include <vector>
#include <fstream>
#include <iostream>
#include <cstdint>
#include "SSTable.h"
//该类用于管辖disk中的所有SSTable，我们将之名为SSList
class SSList {
public:
    //管辖所有的SSTable，每一层的文件，用index索引所在的层
    std::vector<std::vector<SSTable *>> index;
    std::vector<uint64_t >MAXSET;
    std::vector<uint64_t >MINSET;
    std::vector<uint64_t >TIMESTAMPSET;
    SSList();
    ~SSList();
    //通过确定层号，确定层的索引来读取SSTable
    SSTable *readSSTable(int level,int ind,std::fstream *in);
    //添加SSTable
    SSTable *addToList(int level,int i,uint64_t time,std::vector<bool>BF,std::vector<IndexNode>ind);
    //由key返回对应SSTable的指针并设置offset的参数
    SSTable *search(uint64_t key,uint32_t &offset);
    //删除文件后，也会在索引中删除
    void deleteIndex(int level,int ind);
    //返回level层键值与minKey到maxKey有交集的所有SSTable的id，并在index里删除这些索引，重排id
    std::vector<int> Intersection(int level, uint64_t minKey, uint64_t maxKey);
    //将level层中为oldid的文件更新为newid
    void changeid(int level,int newly,int old);
    void clear();

};


#endif //LSMKV_SSLIST_H
