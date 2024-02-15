//
// Created by 21411 on 2023/4/17.
//

#ifndef LSMKV_BUFFER_H
#define LSMKV_BUFFER_H
#include "SSTable.h"
#include <cstdint>
#include <string>
#include <fstream>
#include <vector>
//一个数据点，存储一个键值对
struct DataNode
{
    uint64_t key;
    std::string value;
    DataNode(uint64_t k, std::string v) : key(k), value(v) {}
};
//键值对的表，存储批量数据加上时间戳
struct List
{
    uint64_t TimeStamp;
    std::vector<DataNode> data;
    List(std::vector<DataNode> a, uint64_t b) : data(a), TimeStamp(b) {}
};
class Buffer {
    std::vector<List> dataList;//存储所有要被合并的SSTable
    std::vector<DataNode> out;//完成归并排序后的输出
public:
    uint64_t TimeStamp;//合并后新的时间戳
    Buffer();
    ~Buffer();
    void read(std::fstream *in);//将需要的合并的数据读入
    void compact(bool isempty);//isempty为真代表下一层为空，否则为假
    void write(std::fstream *pout);//以SSTable格式输出
    uint64_t MaxKey();//读取最大键值
    uint64_t MinKey();//读取最小键值
    std::vector<DataNode> getData_2MB(std::vector<bool>&BF);//从输出中获取SSTable不超过2MB的数据，并在参数中需要加入过滤器
    bool isEmpty();//判断输出out是否为空
    void clear();//清空数据，用于实现初始化
    std::vector<DataNode> getData_2MB_noDecrease();
};


#endif //LSMKV_BUFFER_H
