//
// Created by 21411 on 2023/4/19.
//

#include "SSList.h"
SSList::SSList() {

}
SSList::~SSList() {
    clear();
}
//将原本SSList中的所有元素清空
void SSList::clear() {
    for (size_t i = 0; i < index.size(); i++)
    {
        for (size_t j = 0; j < index[i].size(); j++)
        {
            delete index[i][j];
        }
        index[i].clear();
    }
    index.clear();
}
//从一个输入流中将其读出为SSTable
SSTable *SSList::readSSTable(int level, int i, std::fstream *in) {
    //读时间戳
    uint64_t TimeStamp;
    in->read((char *)&TimeStamp, sizeof(TimeStamp));
    //读键值对数量
    uint64_t num=0;
    in->read((char *)&num, sizeof(num));
    //读取过滤器
    in->seekg(32, std::ios::beg);
    std::vector<bool> BF(BFSIZE, 0);
    char b;
    //每次读取一个字节
    for (size_t i = 0; i < BFSIZE; i += 8)
    {
        in->read(&b, sizeof(b));
        for (int j = 0; j < 8; j++)
        {
            //对应的位置上如果是一，就设置
            if (b & (1 << (7 - j)))
            {
                BF[i + j] = 1;
            }
        }
    }
    //读取索引区
    std::vector<IndexNode> a;
    uint64_t key;
    uint32_t offset;
    for(uint64_t i=0;i<num;i++)
    {
        in->read((char *)&key, sizeof(key));
        in->read((char *)&offset, sizeof(offset));
        a.push_back(IndexNode(key,offset));
    }
    return addToList(level,i,TimeStamp,BF,a);
}

SSTable *SSList::addToList(int level, int i, uint64_t time, std::vector<bool> BF, std::vector<IndexNode> ind) {
    SSTable *s=new SSTable(time,BF,ind,level,i);
    //如果当前层号比总层数多，创建一个新的空层
    while((int)(index.size()-1)<level){
        index.push_back(std::vector<SSTable*>());
    }
    bool f=true;
    //读取这一层中的所有元素,按从小到大的编号找到对应的位置插入元素
    for(std::vector<SSTable *>::iterator it = index[level].begin(); it != index[level].end(); it++)
    {
        if(i<(*it)->getId())
        {
            index[level].insert(it,s);
            f= false;
            break;
        }
    }
    //否则插入最后
    if(f== true){
        index[level].push_back(s);
    }
    return s;
}

SSTable *SSList::search(uint64_t key, uint32_t &offset) {
    SSTable *s= nullptr;
    uint32_t Offset;
    bool f= false;
    for(size_t i=0;i<index.size();i++)
    {
        for(size_t j=0;j<index[i].size();j++)
        {
            if((Offset=index[i][j]->existKey(key))>0)
            {
                //如果为空就直接更改，如果非空需要对时间戳进行比较，用较大的时间戳替换较小的时间戳
                if((s== nullptr)||((s!= nullptr)&&(index[i][j]->getTimeStamp()>s->getTimeStamp())))
                {
                    offset=Offset;
                    s=index[i][j];
                    f=true;
                }
            }
        }
        //找到了就不用浪费时间，直接返回
        if (f){
            break;
        }
    }
    return s;
}
//根据索引和层数删除对应的元素
void SSList::deleteIndex(int level, int ind) {
    for(std::vector<SSTable *>::iterator it=index[level].begin();it!=index[level].end();it++)
    {
        if((*it)->getId()==ind)
        {
            index[level].erase(it);
            break;
        }
    }
}

std::vector<int> SSList::Intersection(int level, uint64_t minKey, uint64_t maxKey) {
    std::vector<int> a;
    MAXSET.clear();
    MINSET.clear();
    TIMESTAMPSET.clear();
    for(std::vector<SSTable *>::iterator it=index[level].begin();it!=index[level].end();it++)
    {
        //如果该SSTable与最大和最小键值存在重叠，它们将都会被选取,为了方便，我们只选它们的id
        if(!(((*it)->getMinKey() > maxKey)||((*it)->getMaxKey() < minKey)))
        {
            a.push_back((*it)->getId());
            MAXSET.push_back((*it)->getMaxKey());
            MINSET.push_back((*it)->getMinKey());
            TIMESTAMPSET.push_back((*it)->getTimeStamp());
            index[level].erase(it);
            it--;
        }
    }
    int num=index[level].size();
    //因为上面的元素被选取了，导致该层元素数目变少，需要重新为新的元素排序
    for(int i=0;i<num;i++)
        index[level][i]->changeId(i);
    return a;
}
//批量的将level层的为oldId的元素改为newId
void SSList::changeid(int level, int newly, int old) {
    for(std::vector<SSTable *>::iterator it=index[level].begin();it!=index[level].end();it++){
        if((*it)->getId()==old)
        {
            (*it)->changeId(newly);
            break;
        }
    }
}