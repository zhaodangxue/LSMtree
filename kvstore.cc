#include "kvstore.h"
#include <string>
using namespace std;
//返回memTable的大小
int KVStore::memTableSize() {
    int size1=memTable->size()*12;
    int size2=memTable->dataSize();
    return 32+10240+size1+size2;
}
//创建Level层的路径
std::string KVStore::LevelName(int l) {
    std::string pathName=rootDir+"/level-"+ to_string(l)+"/";
    return pathName;
}
//生成Level层路径的文件夹
std::string KVStore::createByLevel(int l) {
    std::string pathName= LevelName(l);
    if(!utils::dirExists(pathName))
    {
        utils::mkdir(pathName.c_str());
        levelFileNum.push_back(0);
    }
    return pathName;
}
std::string KVStore::SSTableName(int l, uint64_t min,uint64_t max,uint64_t timeStamp) {
    std::string pathName= LevelName(l);
    pathName+="/SSTable"+ to_string(min)+"-"+ to_string(max)+"-"+ to_string(timeStamp)+ ".sst";
    return pathName;
}
std::string KVStore::search(std::uint64_t key) {
    std::string ans;
    char a[200000]={0};
    uint32_t offset=0;
    SSTable*tmp=ssList->search(key,offset);
    //如果为空
    if(!tmp)
    {
        return "";
    }
    //不为空,去找到对应的文件，读出其中的字符串
    fstream input(SSTableName(tmp->getLevel(),tmp->getMinKey(),tmp->getMaxKey(),tmp->getTimeStamp()),ios::binary|ios::in);
    input.seekg(offset,ios::beg);
    input.get(a,200000,'\0');//读入到预先设置的a中
    input.close();
    ans=a;
    if(ans=="~DELETED~")
    {
        return "";
    }
    return ans;
}
void KVStore::write() {
    std::string SSTablePath;
    std::string Level_0= createByLevel(0);//生成第0层文件夹，没有其他用
    SSTablePath= SSTableName(0,memTable->MinKey(),memTable->MaxKey(),maxTimeStamp);
    levelFileNum[0]+=1;
    fstream output(SSTablePath.c_str(),ios::binary|ios::out);
    //写入时间戳
    output.write((char *)&maxTimeStamp,sizeof (maxTimeStamp));
    maxTimeStamp+=1;
    //写入键值对数量
    uint64_t size=memTable->size();
    output.write((char *)&size,sizeof (size));
    //写入最小值
    uint64_t min=memTable->MinKey();
    output.write((char *)&min,sizeof (min));
    //写入最大值
    uint64_t max=memTable->MaxKey();
    output.write((char *)&max,sizeof (max));
    //写入过滤器
    std::vector<bool>BF=memTable->get_BF();
    for(size_t i=0;i<BF.size();i+=8)
    {
        char a=0;
        for(int j=0;j<8;j++)
        {
            if(BF[i+j])
            {
                a=(1<<(7-j))|a;
            }
        }
        output.write(&a,sizeof (a));
    }
    //索引区
    uint32_t offset=10272+12*size;
    Node *tmp=memTable->getLowestHead();
    tmp=tmp->right;
    while(tmp)
    {
        output.write((char *)&(tmp->key),sizeof (tmp->key));
        output.write((char *)&offset,sizeof (offset));
        offset+=tmp->val.size()+1;//注意有'\0'的存在
        tmp=tmp->right;
    }
    //数据区
    tmp=memTable->getLowestHead();
    tmp=tmp->right;
    while(tmp)
    {
        output.write((char *)(tmp->val.c_str()),tmp->val.size()+1);
        tmp=tmp->right;
    }
    output.close();
    //需要在index之中存储，方便之后调用合并
    fstream *input=new fstream (SSTablePath.c_str(),ios::binary|ios::in);
    ssList->readSSTable(0,levelFileNum[0]-1,input);
    input->close();
    delete input;
    memTable->clear();
}
void KVStore::compact(int l) {
    buffer->clear();//每一次合并前需要清空之前用于合并的buffer
    std::string SSTablePath;
    int order=0;
    //第0层是Tiering直接从第0个SSTable进行合并
    if(l==0)
    {
        order=0;
    }
    //其他的层从挑选的超出的文件那部分进行合并
    else
        order=1<<(l+1);
    int oldLevelFileNum=levelFileNum[l];
    //后面的操作是将需要读入的文件SSTable读入buffer，并且删除原有文件
    for(int i=order;i<oldLevelFileNum;i++)
    {
        SSTablePath= SSTableName(l,ssList->index[l][order]->getMinKey(),ssList->index[l][order]->getMaxKey(),ssList->index[l][order]->getTimeStamp());
        fstream *input=new fstream (SSTablePath.c_str(),ios::binary|ios::in);
        buffer->read(input);
        input->close();
        delete input;
        utils::rmfile(SSTablePath.c_str());
        ssList->deleteIndex(l,i);
        levelFileNum[l]--;
    }
    int nl=l+1;
    std::vector<int>com_Table;//记录所有拥有共同键值对SSTable的编号
    if(levelFileNum.size()==nl)//下一层为空
    {
        bool isEmpty=true;
        buffer->compact(isEmpty);
        createByLevel(nl);//创建下一层的文件夹
    }
    else
    {
        oldLevelFileNum=levelFileNum[nl];
        int min=buffer->MinKey();
        int max=buffer->MaxKey();

        com_Table=ssList->Intersection(nl,min,max);
        for(size_t i=0;i<com_Table.size();i++)
        {
            SSTablePath= SSTableName(nl,ssList->MINSET[i],ssList->MAXSET[i],ssList->TIMESTAMPSET[i]);
            fstream *input=new fstream (SSTablePath.c_str(),ios::binary|ios::in);
            buffer->read(input);
            input->close();
            delete input;
            utils::rmfile(SSTablePath.c_str());
            levelFileNum[nl]--;
        }
        //因为对下一层文件有修改，需要重写文件名,从零开始
        bool isEmpty= false;
        buffer->compact(isEmpty);
    }
    //当前buffer中只要非空，就一直往nl层写文件
    while(!buffer->isEmpty())
    {
    SSTablePath= SSTableName(nl,buffer->getData_2MB_noDecrease().front().key,buffer->getData_2MB_noDecrease().back().key,buffer->TimeStamp);
    levelFileNum[nl]+=1;
    fstream  *output=new fstream (SSTablePath.c_str(),ios::out|ios::binary);
    buffer->write(output);
    output->close();
    delete output;
    fstream *input=new fstream (SSTablePath.c_str(),ios::in|ios::binary);
    ssList->readSSTable(nl,levelFileNum[nl]-1,input);
    input->close();
    delete input;
    }
    //如果该层数量还是太多继续递归
    if(levelFileNum[nl]>(1<<(1+nl)))
    {
        compact(nl);
    }
}
KVStore::KVStore(const std::string &dir): KVStoreAPI(dir) {
    memTable = new SkipList();
    ssList = new SSList();
    maxTimeStamp = 0;
    buffer = new Buffer();
    rootDir = dir;
    int Level;
    string directPath;
    for (Level = 0, directPath = LevelName(Level); utils::dirExists(directPath); directPath = LevelName(
            ++Level)) {
        levelFileNum.push_back(0);
        std::vector<std::string> ret;
        ret.clear();
        int k=0;
        utils::scanDir(directPath,ret);
        for(std::vector<std::string>::iterator it=ret.begin();it!=ret.end();it++)
        {
            fstream *input = new fstream((directPath+*it).c_str(), ios::binary | ios::in);
            if (!input->is_open()) {
                input->close();
                delete input;
                break;
            }
            levelFileNum[Level]++;//打开成功就让这一层的文件数量增加
            SSTable *t = ssList->readSSTable(Level, k, input);//读取对应位置处的SSTable
            if (t->getTimeStamp() >= maxTimeStamp)
                maxTimeStamp = t->getTimeStamp() + 1;//如果此处SSTable的时间戳比最大时间戳大，更新最大时间戳
            k++;
            input->close();
            delete input;
        }
    }
}

KVStore::~KVStore()
{
    write();
    delete memTable;
    delete ssList;
    delete buffer;
}
/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    if(memTableSize()+12+s.size()>2*1024*1024)//如果内存中的总大小超过2MB
    {
        write();//先保存到磁盘中
        if(levelFileNum[0]>2)
        {
            compact(0);//第一层中文件数量大于2就进行合并
        }
    }
    memTable->insert(key,s);
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
    std::string ans="";
    ans=memTable->getValue(key);
    if(ans=="~DELETED~")
    {
        return "";
    }
    if(ans!="")
    {
        return ans;
    }
    ans= search(key);
	return ans;
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    std::string ans;
    ans=memTable->getValue(key);
    if(ans=="~DELETED~")
    {
        return false;
    }
    if(ans!="")
    {
        memTable->remove(key);
        put(key,"~DELETED~");
        return true;
    }
    ans= search(key);
    if(ans=="~DELETED~"||ans=="")
    {
        return false;
    }
    put(key,"~DELETED~");
	return true;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    memTable->clear();
    ssList->clear();
    int Level;
    std::string directPath;
    for(Level=0,directPath= LevelName(Level);utils::dirExists(directPath);directPath= LevelName(++Level))
    {
        //找出每一层所有的文件名
        std::vector<string>Lists;
        utils::scanDir(directPath,Lists);
        for(std::vector<string>::iterator it=Lists.begin();it!=Lists.end();++it)
        {
            //一一删除这些被找到的文件
            std::string path=directPath + *it;
            utils::rmfile(path.c_str());
        }
        utils::rmdir(directPath.c_str());
    }
}


/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list)
{	
}