//
// Created by 21411 on 2023/4/6.
//

#include "SkipList.h"
#include<ctime>
#include "SSTable.h"
#include "MurmurHash3.h"
//跳表头我们需要做一个标记
SkipList::SkipList()
{
    head = new Node(0, "header");
}

SkipList::~SkipList()
{
    clear();
}

void SkipList::insert(uint64_t key, std::string val)
{
    Node *p = head;
    std::vector<Node *> pathList; //只记录从上到下的搜索路径
    bool exist = false;  //判断该key对应的节点是否已经存在
    while (p)
    {
        //如果右边的节点存在并且该节点的键值小于要插入的键值，我们向右移动
        while ( p->right && p->right->key < key )
            p = p->right;
        if (p->right && p->right->key == key)
        {                        //如果查找到已经存在
            p->right->val = val; //覆盖其val
            exist = true;    //修改exist为true
        }
        //找到对应的从上到下的路径
        pathList.push_back(p);
        //这种情况说明该节点的键值小于key，但右节点的键的值大于key
        p = p->down;
    }
    if (exist)
        return; //该key对应的节点已经存在，且值已经覆盖完毕，可以直接return
        // 剩下的情况就是不存在这样的一个键值
    Node *downNode = nullptr;
    bool Up = true;//代表是否需要往上插入
    //从下至上搜索路径回溯，50%概率
    while (Up && pathList.size() > 0)
    {
        //取出末尾的节点，当前节点的键值小于key，但是该节点右节点的键值又大于key
        Node *newNode = pathList.back();
        pathList.pop_back();
        newNode->right = new Node(key, val, newNode->right, downNode); //add新结点
        downNode = newNode->right;                                        //把新结点赋值为downNode
        Up = (rand() & 1);//该方法判断是否需要向上增长
    }
    //可能会有超出原来跳表高度的情况，这个时候需要插入新的头结点
    if (Up)
    { //插入新的头结点，加层
        Node *oldHead = head;
        head = new Node(0, "header");
        head->right = new Node(key, val, nullptr, downNode);
        head->down = oldHead;
    }
}
//通过键的值去寻找值
std::string SkipList::getValue(uint64_t key)
{
    Node *p = findNode(key);
    if (p)
    {
        return p->val;
    }
    //找不到返回空
    else
    {
        return "";
    }
}
//通过键的值去寻找点
Node *SkipList::findNode(uint64_t key)
{
    Node *p = head;
    while (p)
    {
        while (p->right && p->right->key < key)
        {
            p = p->right;
        }
        if (p->right && p->right->key == key)
            return p->right;
        p = p->down;
    }
    return nullptr;
}
//删除节点
bool SkipList::remove(uint64_t key)
{
    bool k = false;
    Node *p = head, *tmp = nullptr;
    while (p)
    {
        while (p->right && p->right->key < key)
        {
            p = p->right;
        }
        if (p->right && p->right->key == key)
        {
            tmp = p->right;
            p->right = tmp->right;
            delete tmp;
            k = true;
            //该删除操作可能会引起某一层上的所有元素为空，这是不允许的，需要去除这一层
            if (p == head && p->right == nullptr && p->down != nullptr)
            {
                tmp = head;
                head = head->down;
                delete tmp;
                p = head;
                continue;
            }
        }
        p = p->down;
    }
    return k;
}
//清除整个跳表
void SkipList::clear()
{
    Node *p = head, *tmp = nullptr;
    while (true)
    {
        //我们横向进行删除
        while (p->right)
        {
            tmp = p->right;
            p->right = tmp->right;
            delete tmp;
        }
        if (p->down == nullptr)
        {
            //保留唯一的head
            break;
        }
        tmp = p;
        p = p->down;
        delete tmp;
    }
    head = p;
}
//含有的节点数量
uint64_t SkipList::size()
{
    uint64_t size = 0;
    Node *p = head;
    while (p && p->down)
    {
        p = p->down;
    }
    while (p && p->right)
    {
        p = p->right;
        size++;
    }
    return size;
}
//所有字符串val的总和大小，对于我们后面控制文件大小有用
int SkipList::dataSize()
{
    int result = 0;
    Node *p = head;
    while (p->down)
    {
        p = p->down;
    }
    while (p->right)
    {
        result += p->right->val.size() + 1;
        p = p->right;
    }

    return result;
}
//获取最大键值
uint64_t SkipList::MaxKey()
{
    uint64_t maxKey = 0;
    Node *p = head;
    //先到最底层
    while (p->down)
        p = p->down;
    //再一直向右寻找（非空）
    while (p->right)
        p = p->right;
    maxKey = p->key;
    return maxKey;
}

uint64_t SkipList::MinKey()
{
    uint64_t minKey = 0;
    Node *p = head;
    //先到最底层
    while (p->down)
        p = p->down;
    //再向右寻找一次即可
    if (p->right)
        p = p->right;
    minKey = p->key;
    return minKey;
}
//得到过滤器
std::vector<bool> SkipList::get_BF()
{
    std::vector<bool> data(BFSIZE, 0);
    Node *p = head;
    //先到整个跳表的底部
    while (p->down)
        p = p->down;
    //一直移动到右节点为空为止
    while (p->right)
    {
        unsigned int hash[4] = {0};
        MurmurHash3_x64_128(&(p->right->key), sizeof(p->right->key), 1, hash);
        for (int i = 0; i < 4; i++)
        {
            data[hash[i] % BFSIZE] = 1;
        }
        p = p->right;
    }
    return data;
}
//得到最低位置的head
Node *SkipList::getLowestHead()
{
    Node *p = head;
    while (p->down)
    {
        p = p->down;
    }
    return p;
}