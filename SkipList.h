//
// Created by 21411 on 2023/4/6.
//

#ifndef LSMKV_SKIPLIST_H
#define LSMKV_SKIPLIST_H
#include <iostream>
#include<ctime>
#include <vector>

struct Node
{
    uint64_t key;
    std::string val;
    Node *right, *down;
    Node(uint64_t key, std::string val, Node *right = nullptr, Node *down = nullptr) : key(key), val(val), right(right), down(down) {}
    Node() : key(0), val(""), right(nullptr), down(nullptr) {}
};
//跳表我们只提供向右和向下寻找，每次均从跳表头开始
class SkipList
{
private:
    Node *head;

public:
    SkipList();
    ~SkipList();
    void insert(uint64_t key, std::string val);//插入某一个键值对
    std::string getValue(uint64_t key);//通过某一个key得到具体的string值
    Node *findNode(uint64_t key);//通过键值找到一个具体的点
    bool remove(uint64_t key);//删除某一个键值
    void clear();//清空整个跳表
    uint64_t size(); //返回键值对数量
    int dataSize();  //返回value累计大小
    uint64_t MaxKey();//得到最大的键值
    uint64_t MinKey();//最小的键值
    std::vector<bool> get_BF();//由跳表得到过滤器
    Node *getLowestHead(); //返回最底层的head
};

#endif //LSMKV_SKIPLIST_H
