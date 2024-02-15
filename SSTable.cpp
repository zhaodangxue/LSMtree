//
// Created by 21411 on 2023/4/12.
//

#include "SSTable.h"
#include "MurmurHash3.h"
SSTable::SSTable(uint64_t timestamp, std::vector<bool> BF,
                 std::vector<IndexNode> index,int l,int i) {
    TimeStamp=timestamp;
    Num=index.size();
    BloomFilter=BF;
    Index=index;
    level=l;
    id=i;
}
SSTable::~SSTable()
{
}

uint64_t SSTable::getMinKey() {
    MinKey=Index.front().key;
    return MinKey;
}
uint64_t SSTable::getMaxKey() {
    MaxKey=Index.back().key;
    return MaxKey;
}
uint64_t SSTable::getTimeStamp() {
    return TimeStamp;
}
uint32_t SSTable::existKey(uint64_t key) {
    //用过滤器判断Key是否存在
    unsigned int hash[4]={0};
    MurmurHash3_x64_128(&key,sizeof (key),1,hash);
    for(size_t i=0;i<4;i++)
    {
        if(BloomFilter[hash[i]%BFSIZE]== false)
        {
            return 0;
        }
    }
    return SearchOffset_Key(key);
}
uint32_t SSTable::SearchOffset_Key(uint64_t key) {
    //利用二分查找去搜索
    int left = 0, right = Index.size() - 1;
    int middle;
    while (left <= right)
    {
        middle = (left + right) / 2;
        if (Index[middle].key == key)
        {
            return Index[middle].offset;
        }
        else if (key < Index[middle].key)
        {
            right = middle - 1;
        }
        else
        {
            left = middle + 1;
        }
    }
    return 0;
}

int SSTable::getId() {
    return id;
}
int SSTable::getLevel() {
    return level;
}

void SSTable::changeId(int newId) {
    id=newId;
}