//
// Created by 21411 on 2023/4/17.
//
#include "MurmurHash3.h"
#include "Buffer.h"

Buffer::Buffer() {
    clear();
}
Buffer::~Buffer() {

}
void Buffer::read(std::fstream *in) {
    //读取时间戳
    uint64_t timeStamp;
    in->read((char *)&timeStamp,sizeof (timeStamp));//必须加上&代表取地址再强制类型转化
    //键值对的数量
    uint64_t num_kv;
    num_kv=0;
    in->read((char *)&num_kv,sizeof (num_kv));
    //将索引区设为对应的in数据流的开头,为读取索引区中的内容做准备
    in->seekg(10272,std::ios::beg);
    std::vector<IndexNode> Index;
    uint64_t key;
    uint32_t offset;
    //读取索引区的值存入Index中
    for(uint64_t i=0;i<num_kv;i++)
    {
        in->read((char *)&key,sizeof (key));
        in->read((char *)&offset,sizeof (offset));
        Index.push_back(IndexNode(key,offset));
    }
    //将对应数据存到dataList中
    std::string value;
    std::vector<DataNode> T;//存储数据点，本质上是键值对
    char a[200000]={0};//用来存储string
    value="";
    for(uint64_t i=0;i<num_kv;i++)
    {
        in->seekg(Index[i].offset,std::ios::beg);
        in->get(a,200000,'\0');
        value=a;
        T.push_back(DataNode(Index[i].key,value));
    }
    //以上操作能让T中存满了一个SSTable中所有的键值对
    //将所有需要合并的SSTable存入dataList
    List list(T,timeStamp);
    dataList.push_back(list);
}
void Buffer::clear()
{
    dataList.clear();
    out.clear();
    TimeStamp=0;
}
void Buffer::compact(bool isempty) {
    out.clear();
    //代表被取值的目标文件，在datalist中索引，每轮取最小
    int location=0;
    //每一轮归并排序的键最小值
    uint64_t Min=UINT64_MAX;
    //每一轮归并排序的最大时间戳
    uint64_t MaxTime=0;
    //初始化
    TimeStamp=0;
    //选取最大的时间戳作为整个的时间戳
    for (uint64_t i = 0; i < dataList.size(); i++)
    {
        if (dataList[i].TimeStamp > TimeStamp)
            TimeStamp = dataList[i].TimeStamp;
    }
    //当dataList非空，我们的目的是为了删除所有的重复键值，选取其中的最大时间戳中的键值作为真正的键值
    while(dataList.empty()== false){
        int SS_num=dataList.size();//代表SSTable的数量
        Min=UINT64_MAX;
        MaxTime=0;
        location=0;
        for(int i=0;i<SS_num;i++)
        {
            //优先对每个表中的第一个元素操作
            DataNode data = dataList[i].data.front();
            //如果当前的键值比最小键值小，让它为最小键值
            if (data.key < Min)
            {
                MaxTime = dataList[i].TimeStamp;
                location = i;
                Min = data.key;
            }
            else if (data.key == Min)
            {
                if (dataList[i].TimeStamp > MaxTime)
                {
                    //如果当前的时间戳较大，则原来的键值对可以被舍弃，并且重新定位location
                    dataList[location].data.erase(dataList[location].data.begin());
                    MaxTime = dataList[i].TimeStamp;
                    location = i;
                }
                else
                {
                    //本身时间戳较小，可以删
                    dataList[i].data.erase(dataList[i].data.begin());
                }
            }
        }
        //在上述的操作之后，我们可以取出这个最小的键，同时也是最大的时间戳
        DataNode a = dataList[location].data.front();
        //如果不是一个删除标记且下一层为空，将数据存入out中
        if (!(isempty && a.value == "~DELETED~"))
            out.push_back(a);
        //删除最小的键，同时也是最大的时间戳的位置
        dataList[location].data.erase(dataList[location].data.begin());
        SS_num = dataList.size();
        //检查是否存在已经读完的数据表
        for (int i = 0; i < SS_num; i++)
        {
            if (dataList[i].data.empty())
            {
                dataList.erase(dataList.begin() + i);
                i -= 1; //由于是用下标访问，每次循环i++, 不减一的话会跳过下一个元素
                SS_num -= 1;
            }
        }
    }
}
void Buffer::write(std::fstream *pout) {
    if(out.empty())
    {
        return;
    }
    std::vector<bool>BF;
    std::vector<DataNode> dataSet= getData_2MB(BF);
    //写入时间戳
    pout->write((char *)&TimeStamp,sizeof (TimeStamp));
    //写入键值对的数量
    uint64_t Size=dataSet.size();
    pout->write((char *)&Size,sizeof (Size));
    //写入最大和最小键值
    uint64_t Min=dataSet.front().key;
    uint64_t Max=dataSet.back().key;
    pout->write((char *)&Min, sizeof(Min));
    pout->write((char *)&Max, sizeof(Max));
    //写入过滤器
    for (size_t i=0;i<BFSIZE;i+=8)
    {
        char tmp = 0;
        //一个一个字节进行设置
        for (int j = 0; j < 8; ++j)
        {
            //当对应的位置上不为0时为对应的位置设置值
            if (BF[i + j])
            {
                tmp =  (1 << (7 - j)) | tmp;
            }
        }
        pout->write(&tmp, sizeof(tmp));
    }
    //写入索引区
    uint32_t offset = 10272+12*Size;
    for (uint64_t i=0;i<Size;i++)
    {
        pout->write((char *)&(dataSet[i].key), sizeof(dataSet[i].key));
        pout->write((char *)&offset, sizeof(offset));
        offset += dataSet[i].value.size() + 1;
    }
    //写入数据区
    for (uint64_t i=0;i<Size;i++)
    {
        pout->write((char *)(dataSet[i].value.c_str()), dataSet[i].value.size() + 1);
    }
}
//该方法用于提供键值对，同时生成对应的过滤器，为上面的write函数服务
std::vector<DataNode> Buffer::getData_2MB(std::vector<bool> &BF) {
    int max=2*1024*1024;//生成的文件大小不能超过2MB
    BF.assign(BFSIZE,0);//初始化
    int initial_size=10272;//过滤器加上header的大小
    std::vector<DataNode> data;
    while(1){
        //如果输出为空，直接退出
        if(out.empty())
            break;
        initial_size+=12;//一对索引(key,offset)的大小
        initial_size+=out.front().value.size()+1;//value的大小（注意\0的存在）
        //不能超过2MB
        if(initial_size>max)
            break;
        //从out中取出并擦除
        data.push_back(out.front());
        out.erase(out.begin());
        //过滤器的写
        unsigned int hash[4] = {0};
        MurmurHash3_x64_128(&data.back().key, sizeof(data.back().key), 1, hash);
        for (int i = 0; i < 4; i++)
            BF[hash[i] % BFSIZE] = 1;
    }
    return data;
}
uint64_t Buffer::MaxKey() {
    uint64_t max=0;
    int k=dataList.size();
    for(int i=0;i<k;i++)
    {
        //索引是有顺序的我们只需要从尾部就能找到最大的键值
        if(max<dataList[i].data.back().key)
            max=dataList[i].data.back().key;
    }
    return max;
}
uint64_t Buffer::MinKey() {
    uint64_t min=UINT64_MAX;
    int k=dataList.size();
    for(int i=0;i<k;i++)
    {
        //索引是有顺序的我们只需要从头部就能找到最小的键值
        if(min>dataList[i].data.front().key)
            min=dataList[i].data.front().key;
    }
    return min;
}
bool Buffer::isEmpty() {
    return out.empty();
}
std::vector<DataNode> Buffer::getData_2MB_noDecrease() {
    std::vector<bool>BF;
    int max=2*1024*1024;//生成的文件大小不能超过2MB
    BF.assign(BFSIZE,0);//初始化
    int initial_size=10272;//过滤器加上header的大小
    std::vector<DataNode> data;
    std::vector<DataNode>tmp=out;
    while(1){
        //如果输出为空，直接退出
        if(tmp.empty())
            break;
        initial_size+=12;//一对索引(key,offset)的大小
        initial_size+=tmp.front().value.size()+1;//value的大小（注意\0的存在）
        //不能超过2MB
        if(initial_size>max)
            break;
        //从out中取出并擦除
        data.push_back(tmp.front());
        tmp.erase(tmp.begin());
        //过滤器的写
        unsigned int hash[4] = {0};
        MurmurHash3_x64_128(&data.back().key, sizeof(data.back().key), 1, hash);
        for (int i = 0; i < 4; i++)
            BF[hash[i] % BFSIZE] = 1;
    }
    return data;
}