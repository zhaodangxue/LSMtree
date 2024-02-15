#pragma once

#include "kvstore_api.h"
#include "SkipList.h"
#include <iostream>
#include <fstream>
#include <string>
#include "utils.h"
#include "SSList.h"
#include "Buffer.h"
class KVStore : public KVStoreAPI {
private:
    SkipList *memTable;//内存中的暂存跳表
    SSList *ssList;//缓存，管辖disk中的所有SSTable
    Buffer *buffer;//缓存区用于读取，写，合并文件
    std::string rootDir;//根目录
    std::vector<int>levelFileNum;//代表每一层文件的数量
    uint64_t maxTimeStamp;//记录整个中最大的时间戳
    int memTableSize();//告知整个内存的空间大小
    void write();//存储到磁盘
    void compact(int l);//合并函数
    std::string search(std::uint64_t key);//根据对应的key值在SSTable中寻找值
    std::string createByLevel(int l);//根据对应的level大小生成对应的层
    std::string LevelName(int l);//根据level的值去寻找对应数据目录
    std::string SSTableName(int n,uint64_t min,uint64_t max,uint64_t timeStamp);//根据对应的层号以及索引寻找SSTable的路径
public:
	KVStore(const std::string &dir);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) override;
};
