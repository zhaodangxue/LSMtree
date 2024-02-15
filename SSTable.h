//
// Created by 21411 on 2023/4/12.
//

#ifndef LSMKV_SSTABLE_H
#define LSMKV_SSTABLE_H
#include<vector>
#include <cstdint>
//10240个字节
const int BFSIZE = 81920;
//索引点的构建，包含了key以及offset
struct IndexNode
{
    uint32_t offset;
    uint64_t key;
    IndexNode(uint64_t a, uint32_t b) : key(a), offset(b) {}
    //默认设置为0，0
    IndexNode() : key(0), offset(0) {}
};
class SSTable {
private:
    //层号
    int level;
    //在该层的编号
    int id;
    //时间戳
    uint64_t TimeStamp;
    //键值对的数量
    uint64_t Num;
    //键最小值
    uint64_t MinKey;
    //键最大值
    uint64_t MaxKey;
    //布隆过滤器
    std::vector<bool> BloomFilter;
    //索引区
    std::vector<IndexNode> Index;
public:
    //由key去寻找相应的offset
    uint32_t SearchOffset_Key(uint64_t key);
    //构造函数（时间戳，过滤器，索引区）
    SSTable( uint64_t timestamp, std::vector<bool> BF, std::vector<IndexNode> index,int l,int i);
    //析构函数
    ~SSTable();
    //判断该SSTable中是否存在key，若存在返回offset，不存在返回0
    uint32_t existKey(uint64_t key);
    //得到时间戳
    uint64_t getTimeStamp();
    //得到键最小值
    uint64_t getMinKey();
    //得到键最大值
    uint64_t getMaxKey();
    //获取编号
    int getId();
    void changeId(int newId);
    int getLevel();
};


#endif //LSMKV_SSTABLE_H
