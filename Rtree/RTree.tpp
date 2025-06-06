//
// Created by ruihong on 2/14/25.
//
#include "RTree.h"
RTREE_TEMPLATE
thread_local std::map<int, SELCC_Guard>*  RTREE_QUAL::gptrs_mp = nullptr;

RTREE_TEMPLATE
RTREE_QUAL::RTree(uint16_t Btr_id,DDSM* ddsm)
{
            RTREE_ASSERT(MAXNODES > MINNODES);
            RTREE_ASSERT(MINNODES > 0);
    m_tree_id = Btr_id;
    ddsm_ = ddsm;
    if (gptrs_mp == nullptr) {
        gptrs_mp = new std::map<int, SELCC_Guard>();
    }
    // Precomputed volumes of the unit spheres for the first few dimensions
    const float UNIT_SPHERE_VOLUMES[] = {
            0.000000f, 2.000000f, 3.141593f, // Dimension  0,1,2
            4.188790f, 4.934802f, 5.263789f, // Dimension  3,4,5
            5.167713f, 4.724766f, 4.058712f, // Dimension  6,7,8
            3.298509f, 2.550164f, 1.884104f, // Dimension  9,10,11
            1.335263f, 0.910629f, 0.599265f, // Dimension  12,13,14
            0.381443f, 0.235331f, 0.140981f, // Dimension  15,16,17
            0.082146f, 0.046622f, 0.025807f, // Dimension  18,19,20
    };
    m_unitSphereVolume = (ELEMTYPEREAL)UNIT_SPHERE_VOLUMES[NUMDIMS];
    auto rdma_mg = ddsm->rdma_mg;
    if (DSMEngine::RDMA_Manager::node_id == 0){
        root_handle = AllocNode();
        root_gptr = root_handle->gptr;
        m_root = (Node*)((ibv_mr*)root_handle->value)->addr;
        m_root->m_level = 0;
        ddsm_->Update_Root_GCL(m_tree_id, root_gptr);
    }else{
        root_gptr  = ddsm_->Get_Root_GCL(m_tree_id);
        while(ddsm_->Get_Root_GCL(m_tree_id) == GlobalAddress::Null()){
            root_gptr  = ddsm_->Get_Root_GCL(m_tree_id);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
//        // get the root node.
//        Exclusive_Guard guard(m_root, root_gptr, root_handle);
    }

}


RTREE_TEMPLATE
RTREE_QUAL::RTree(const RTree& other) : RTree()
{
    CopyRec(m_root, other.m_root);
}


RTREE_TEMPLATE
RTREE_QUAL::~RTree()
{
    Reset(); // Free, or reset node memory
}


RTREE_TEMPLATE
void RTREE_QUAL::Insert(const ELEMTYPE a_min[NUMDIMS], const ELEMTYPE a_max[NUMDIMS], const DATATYPE& a_dataId)
{
#ifdef _DEBUG
    for(int index=0; index<NUMDIMS; ++index)
  {
    RTREE_ASSERT(a_min[index] <= a_max[index]);
  }
#endif //_DEBUG

    Branch branch;
    branch.m_data = a_dataId;
    branch.m_child = NULL;

    for(int axis=0; axis<NUMDIMS; ++axis)
    {
        branch.m_rect.m_min[axis] = a_min[axis];
        branch.m_rect.m_max[axis] = a_max[axis];
    }

    InsertRect(branch, root_gptr, 0);
}


RTREE_TEMPLATE
void RTREE_QUAL::Remove(const DATATYPE& a_dataId)
{
    RemoveRect(NULL, a_dataId, &m_root);
}


RTREE_TEMPLATE
void RTREE_QUAL::Remove(const ELEMTYPE a_min[NUMDIMS], const ELEMTYPE a_max[NUMDIMS], const DATATYPE& a_dataId)
{
#ifdef _DEBUG
    for(int index=0; index<NUMDIMS; ++index)
  {
    RTREE_ASSERT(a_min[index] <= a_max[index]);
  }
#endif //_DEBUG

    Rect rect;

    for(int axis=0; axis<NUMDIMS; ++axis)
    {
        rect.m_min[axis] = a_min[axis];
        rect.m_max[axis] = a_max[axis];
    }

    RemoveRect(&rect, a_dataId, &m_root);
}


RTREE_TEMPLATE
int RTREE_QUAL::Search(const ELEMTYPE a_min[NUMDIMS], const ELEMTYPE a_max[NUMDIMS], std::function<bool (const DATATYPE&)> callback) const
{
#ifdef _DEBUG
    for(int index=0; index<NUMDIMS; ++index)
  {
    RTREE_ASSERT(a_min[index] <= a_max[index]);
  }
#endif //_DEBUG

    Rect rect;

    for(int axis=0; axis<NUMDIMS; ++axis)
    {
        rect.m_min[axis] = a_min[axis];
        rect.m_max[axis] = a_max[axis];
    }
    auto root_ptr =  ddsm_->Get_Root_GCL(m_tree_id);

    // NOTE: May want to return search result another way, perhaps returning the number of found elements here.

    int foundCount = 0;
    SearchRect(root_ptr, &rect, foundCount, callback);

    return foundCount;
}

RTREE_TEMPLATE
size_t RTREE_QUAL::NNSearch(
        const ELEMTYPE a_min[NUMDIMS], const ELEMTYPE a_max[NUMDIMS],
        std::function<bool(const DATATYPE&, ELEMTYPE)> callback
) const
{
    // Create a search rectangle
    Rect rect;
    for (int axis = 0; axis < NUMDIMS; ++axis)
    {
        rect.m_min[axis] = a_min[axis];
        rect.m_max[axis] = a_max[axis];
    }

    // class to store branches in the priority queue
    struct QueueItem
    {
        QueueItem(Branch* branch, ELEMTYPE distance) :
                branch(branch),
                distance(distance)
        {}

        // Sort in the queue with the minimum distance
        // taking priority
        bool operator<(QueueItem const& a) const
        {
            return this->distance > a.distance;
        }

        Branch* branch;
        ELEMTYPE distance;
    };

    std::priority_queue<QueueItem> search_queue;
    auto root_ptr =  ddsm_->Get_Root_GCL(m_tree_id);
    {
        Shared_Guard a_guard(m_root, root_ptr, root_handle);

        // All branches in the root node are inserted into the priority queue.
        for (auto i = 0; i < m_root->m_count; ++i) {
            auto d = this->SquareDistance(rect, m_root->m_branch[i].m_rect);
            search_queue.emplace(m_root->m_branch + i, d);
        }
    }

    size_t foundCount = 0;

    // Until the queue is empty
    while (!search_queue.empty())
    {
        // Process the top item in the queue
        auto process = std::move(search_queue.top());
        search_queue.pop();

        if (process.branch->m_child)
        {
            // If the branch has children, add them all into the queue
            GlobalAddress* node_gptr = process.branch->m_child;
            Node* node = nullptr;
            Cache_Handle *node_handle = nullptr;
            Shared_Guard a_guard(node, *node_gptr, node_handle);
            for (auto i = 0; i < node->m_count; ++i)
            {
                auto d = this->SquareDistance(rect, node->m_branch[i].m_rect);
                search_queue.emplace(node->m_branch + i, d);
            }
        }
        else
        {
            // If this is a leaf, then we have found a minimum distance
            // Call the callback
            ++foundCount;
            if (!callback(process.branch->m_data, process.distance))
            {
                // If the user has flaged to stopped, then return
                // the number found.
                return foundCount;
            }
        }

    }

    // No more items to search
    return foundCount;
}


RTREE_TEMPLATE
int RTREE_QUAL::Count()
{
    int count = 0;
    CountRec(m_root, count);

    return count;
}



RTREE_TEMPLATE
void RTREE_QUAL::CountRec(Node* a_node, int& a_count)
{
    if(a_node->IsInternalNode())  // not a leaf node
    {
        for(int index = 0; index < a_node->m_count; ++index)
        {
            CountRec(a_node->m_branch[index].m_child, a_count);
        }
    }
    else // A leaf node
    {
        a_count += a_node->m_count;
    }
}


RTREE_TEMPLATE
bool RTREE_QUAL::Load(const char* a_fileName)
{
    RemoveAll(); // Clear existing tree

    RTFileStream stream;
    if(!stream.OpenRead(a_fileName))
    {
        return false;
    }

    bool result = Load(stream);

    stream.Close();

    return result;
}



RTREE_TEMPLATE
bool RTREE_QUAL::Load(RTFileStream& a_stream)
{
    // Write some kind of header
    int _dataFileId = ('R'<<0)|('T'<<8)|('R'<<16)|('E'<<24);
    int _dataSize = sizeof(DATATYPE);
    int _dataNumDims = NUMDIMS;
    int _dataElemSize = sizeof(ELEMTYPE);
    int _dataElemRealSize = sizeof(ELEMTYPEREAL);
    int _dataMaxNodes = TMAXNODES;
    int _dataMinNodes = TMINNODES;

    int dataFileId = 0;
    int dataSize = 0;
    int dataNumDims = 0;
    int dataElemSize = 0;
    int dataElemRealSize = 0;
    int dataMaxNodes = 0;
    int dataMinNodes = 0;

    a_stream.Read(dataFileId);
    a_stream.Read(dataSize);
    a_stream.Read(dataNumDims);
    a_stream.Read(dataElemSize);
    a_stream.Read(dataElemRealSize);
    a_stream.Read(dataMaxNodes);
    a_stream.Read(dataMinNodes);

    bool result = false;

    // Test if header was valid and compatible
    if(    (dataFileId == _dataFileId)
           && (dataSize == _dataSize)
           && (dataNumDims == _dataNumDims)
           && (dataElemSize == _dataElemSize)
           && (dataElemRealSize == _dataElemRealSize)
           && (dataMaxNodes == _dataMaxNodes)
           && (dataMinNodes == _dataMinNodes)
            )
    {
        // Recursively load tree
        result = LoadRec(m_root, a_stream);
    }

    return result;
}


RTREE_TEMPLATE
bool RTREE_QUAL::LoadRec(Node* a_node, RTFileStream& a_stream)
{
    a_stream.Read(a_node->m_level);
    a_stream.Read(a_node->m_count);

    if(a_node->IsInternalNode())  // not a leaf node
    {
        for(int index = 0; index < a_node->m_count; ++index)
        {
            Branch* curBranch = &a_node->m_branch[index];

            a_stream.ReadArray(curBranch->m_rect.m_min, NUMDIMS);
            a_stream.ReadArray(curBranch->m_rect.m_max, NUMDIMS);

            curBranch->m_child = AllocNode();
            LoadRec(curBranch->m_child, a_stream);
        }
    }
    else // A leaf node
    {
        for(int index = 0; index < a_node->m_count; ++index)
        {
            Branch* curBranch = &a_node->m_branch[index];

            a_stream.ReadArray(curBranch->m_rect.m_min, NUMDIMS);
            a_stream.ReadArray(curBranch->m_rect.m_max, NUMDIMS);

            a_stream.Read(curBranch->m_data);
        }
    }

    return true; // Should do more error checking on I/O operations
}


RTREE_TEMPLATE
void RTREE_QUAL::CopyRec(Node* current, Node* other)
{
    current->m_level = other->m_level;
    current->m_count = other->m_count;

    if(current->IsInternalNode())  // not a leaf node
    {
        for(int index = 0; index < current->m_count; ++index)
        {
            Branch* currentBranch = &current->m_branch[index];
            Branch* otherBranch = &other->m_branch[index];

            std::copy(otherBranch->m_rect.m_min,
                      otherBranch->m_rect.m_min + NUMDIMS,
                      currentBranch->m_rect.m_min);

            std::copy(otherBranch->m_rect.m_max,
                      otherBranch->m_rect.m_max + NUMDIMS,
                      currentBranch->m_rect.m_max);

            currentBranch->m_child = AllocNode();
            CopyRec(currentBranch->m_child, otherBranch->m_child);
        }
    }
    else // A leaf node
    {
        for(int index = 0; index < current->m_count; ++index)
        {
            Branch* currentBranch = &current->m_branch[index];
            Branch* otherBranch = &other->m_branch[index];

            std::copy(otherBranch->m_rect.m_min,
                      otherBranch->m_rect.m_min + NUMDIMS,
                      currentBranch->m_rect.m_min);

            std::copy(otherBranch->m_rect.m_max,
                      otherBranch->m_rect.m_max + NUMDIMS,
                      currentBranch->m_rect.m_max);

            currentBranch->m_data = otherBranch->m_data;
        }
    }
}


RTREE_TEMPLATE
bool RTREE_QUAL::Save(const char* a_fileName)
{
    RTFileStream stream;
    if(!stream.OpenWrite(a_fileName))
    {
        return false;
    }

    bool result = Save(stream);

    stream.Close();

    return result;
}


RTREE_TEMPLATE
bool RTREE_QUAL::Save(RTFileStream& a_stream)
{
    // Write some kind of header
    int dataFileId = ('R'<<0)|('T'<<8)|('R'<<16)|('E'<<24);
    int dataSize = sizeof(DATATYPE);
    int dataNumDims = NUMDIMS;
    int dataElemSize = sizeof(ELEMTYPE);
    int dataElemRealSize = sizeof(ELEMTYPEREAL);
    int dataMaxNodes = TMAXNODES;
    int dataMinNodes = TMINNODES;

    a_stream.Write(dataFileId);
    a_stream.Write(dataSize);
    a_stream.Write(dataNumDims);
    a_stream.Write(dataElemSize);
    a_stream.Write(dataElemRealSize);
    a_stream.Write(dataMaxNodes);
    a_stream.Write(dataMinNodes);

    // Recursively save tree
    bool result = SaveRec(m_root, a_stream);

    return result;
}


RTREE_TEMPLATE
bool RTREE_QUAL::SaveRec(Node* a_node, RTFileStream& a_stream)
{
    a_stream.Write(a_node->m_level);
    a_stream.Write(a_node->m_count);

    if(a_node->IsInternalNode())  // not a leaf node
    {
        for(int index = 0; index < a_node->m_count; ++index)
        {
            Branch* curBranch = &a_node->m_branch[index];

            a_stream.WriteArray(curBranch->m_rect.m_min, NUMDIMS);
            a_stream.WriteArray(curBranch->m_rect.m_max, NUMDIMS);

            SaveRec(curBranch->m_child, a_stream);
        }
    }
    else // A leaf node
    {
        for(int index = 0; index < a_node->m_count; ++index)
        {
            Branch* curBranch = &a_node->m_branch[index];

            a_stream.WriteArray(curBranch->m_rect.m_min, NUMDIMS);
            a_stream.WriteArray(curBranch->m_rect.m_max, NUMDIMS);

            a_stream.Write(curBranch->m_data);
        }
    }

    return true; // Should do more error checking on I/O operations
}


RTREE_TEMPLATE
void RTREE_QUAL::RemoveAll()
{
    // Delete all existing nodes
    Reset();

    m_root = AllocNode();
    m_root->m_level = 0;
}


RTREE_TEMPLATE
void RTREE_QUAL::Reset()
{
#ifdef RTREE_DONT_USE_MEMPOOLS
    // Delete all existing nodes
    RemoveAllRec(m_root);
#else // RTREE_DONT_USE_MEMPOOLS
    // Just reset memory pools.  We are not using complex types
  // EXAMPLE
#endif // RTREE_DONT_USE_MEMPOOLS
}


RTREE_TEMPLATE
void RTREE_QUAL::RemoveAllRec(Node* a_node)
{
            RTREE_ASSERT(a_node);
            RTREE_ASSERT(a_node->m_level >= 0);

    if(a_node->IsInternalNode()) // This is an internal node in the tree
    {
        for(int index=0; index < a_node->m_count; ++index)
        {
            RemoveAllRec(a_node->m_branch[index].m_child);
        }
    }
    FreeNode(a_node);
}


RTREE_TEMPLATE
Cache_Handle * RTREE_QUAL::AllocNode(int level)
{
    Node* newNode;
#ifdef RTREE_DONT_USE_MEMPOOLS
    GlobalAddress gptr = ddsm_->Allocate_Remote(Regular_Page);
    void* local_buff;
    Cache_Handle* handle;
    Exclusive_Guard guard(local_buff, gptr, handle);
    newNode = new(local_buff) Node;
#else // RTREE_DONT_USE_MEMPOOLS
    // EXAMPLE
#endif // RTREE_DONT_USE_MEMPOOLS
    InitNode(newNode);
    if (level != INT_MAX) {
        gptrs_mp->insert({-(level+1), std::move(guard)});
    }
    return handle;
}


RTREE_TEMPLATE
void RTREE_QUAL::FreeNode(Cache_Handle* node_h)
{
//            RTREE_ASSERT(node_h);

#ifdef RTREE_DONT_USE_MEMPOOLS
ddsm_->Deallocate_Remote(Regular_Page, node_h->gptr);
//    delete a_node;
#else // RTREE_DONT_USE_MEMPOOLS
    // EXAMPLE
#endif // RTREE_DONT_USE_MEMPOOLS
}


// Allocate space for a node in the list used in DeletRect to
// store Nodes that are too empty.
RTREE_TEMPLATE
typename RTREE_QUAL::ListNode* RTREE_QUAL::AllocListNode()
{
#ifdef RTREE_DONT_USE_MEMPOOLS
    return new ListNode;
#else // RTREE_DONT_USE_MEMPOOLS
    // EXAMPLE
#endif // RTREE_DONT_USE_MEMPOOLS
}


RTREE_TEMPLATE
void RTREE_QUAL::FreeListNode(ListNode* a_listNode)
{
#ifdef RTREE_DONT_USE_MEMPOOLS
    delete a_listNode;
#else // RTREE_DONT_USE_MEMPOOLS
    // EXAMPLE
#endif // RTREE_DONT_USE_MEMPOOLS
}


RTREE_TEMPLATE
void RTREE_QUAL::InitNode(Node* a_node)
{
    a_node->m_count = 0;
    a_node->m_level = -1;
}


RTREE_TEMPLATE
void RTREE_QUAL::InitRect(Rect* a_rect)
{
    for(int index = 0; index < NUMDIMS; ++index)
    {
        a_rect->m_min[index] = (ELEMTYPE)0;
        a_rect->m_max[index] = (ELEMTYPE)0;
    }
}


// Inserts a new data rectangle into the index structure.
// Recursively descends tree, propagates splits back up.
// Returns 0 if node was not split.  Old node updated.
// If node was split, returns 1 and sets the pointer pointed to by
// new_node to point to the new node.  Old node updated to become one of two.
// The level argument specifies the number of steps up from the leaf
// level to insert; e.g. a data rectangle goes in at level = 0.
RTREE_TEMPLATE
bool RTREE_QUAL::InsertRectRec(const Branch& a_branch, GlobalAddress a_node_gptr, Cache_Handle*& newnode_h_this, int a_level)
{
    Node* a_node = nullptr;
    LockCoupleFillMap(a_node_gptr, &a_node);
    RTREE_ASSERT(a_node && newnode_h_this);
    RTREE_ASSERT(a_level >= 0 && a_level <= a_node->m_level);
//    LockCoupleCleanMap(a_node);
    bool ret = false;

    // todo: need a lock coupling algorithm to guarantee the concurrency.
    // recurse until we reach the correct level for the new record. data records
    // will always be called with a_level == 0 (leaf)
    if(a_node->m_level > a_level)
    {
        // Still above level for insertion, go down tree recursively
        Cache_Handle* newnode_h_next = nullptr;
        // find the optimal branch for this record
        int index = PickBranch(&a_branch.m_rect, a_node);

        // recursively insert this record into the picked branch
        bool childWasSplit = InsertRectRec(a_branch, a_node->m_branch[index].m_child, newnode_h_next, a_level);

        if (!childWasSplit)
        {
            // Child was not split. Merge the bounding box of the new record with the
            // existing bounding box. Caution!!: the lock coupling may falsely release its guard, the modification below is not safe.
            a_node->m_branch[index].m_rect = CombineRect(&a_branch.m_rect, &(a_node->m_branch[index].m_rect));
//            return false;
        }
        else
        {
            Node* otherNode = (Node*)((ibv_mr*)newnode_h_next->value)->addr;
            // Child was split. The old branches are now re-partitioned to two nodes
            // so we have to re-calculate the bounding boxes of each node
            a_node->m_branch[index].m_rect = NodeCover(a_node->m_branch[index].m_child);
            Branch branch;
            branch.m_child = newnode_h_next->gptr;
            branch.m_rect = NodeCover(otherNode);
            gptrs_mp->erase(-(otherNode->m_level+1));
            // The old node is already a child of a_node. Now add the newly-created
            // node to a_node as well. a_node might be split because of that.
            ret = AddBranch(&branch, a_node, newnode_h_this);
        }
    }
    else if(a_node->m_level == a_level)
    {
        // We have reached level for insertion. Add rect, split if necessary
        ret =  AddBranch(&a_branch, a_node, newnode_h_this);
    }
    else
    {
        // Should never occur
                RTREE_ASSERT(0);
//        return false;
    }
//    if (ret) {
//        assert(gptrs_mp->count(a_node->m_level));
        // destroy the guard of current node.
        gptrs_mp->erase(a_node->m_level);
//    }
    return ret;
}

RTREE_TEMPLATE
void RTREE_QUAL::LockCoupleFillMap(GlobalAddress node_gptr, Node** node) {
    Cache_Handle* a_handle = nullptr;
    void* local_buff;
    Exclusive_Guard a_guard(local_buff, node_gptr, a_handle);
    *node = (Node*)local_buff;
    gptrs_mp->insert({(*node)->m_level, std::move(a_guard)});
}
RTREE_TEMPLATE
void RTREE_QUAL::LockCoupleCleanMap(Node* node) {
    // It seems lock coupling does not work for Rtree
    if (node->m_count < node-> MAXNODES - 1){
        // todo: clean up all the locks whose level is larger than this level in the gptrs_mp.
        int level  = node->m_level;
        while (gptrs_mp->count(level+1) > 0){
            gptrs_mp->erase(level+1);
            level++;
        }
        if (gptrs_mp->count(INT_MAX)){
            gptrs_mp->erase(INT_MAX); // remove the guard for catalog.
        }

    }
}


// Insert a data rectangle into an index structure.
// InsertRect provides for splitting the root;
// returns 1 if root was split, 0 if it was not.
// The level argument specifies the number of steps up from the leaf
// level to insert; e.g. a data rectangle goes in at level = 0.
// InsertRect2 does the recursion.
//
RTREE_TEMPLATE
bool RTREE_QUAL::InsertRect(const Branch& a_branch, int a_level)
{
#ifdef _DEBUG
    for(int index=0; index < NUMDIMS; ++index)
  {
    RTREE_ASSERT(a_branch.m_rect.m_min[index] <= a_branch.m_rect.m_max[index]);
  }
#endif //_DEBUG
    Cache_Handle* catal_h = nullptr;
    void* local_buff;
    Exclusive_Guard a_guard(local_buff, ddsm_->catalog_ptr, catal_h);
    gptrs_mp->insert({INT_MAX, std::move(a_guard)});
    CatalogPage* catlog = (CatalogPage*)((ibv_mr*)catal_h->value)->addr;
    root_gptr = catlog->root_gptrs[m_tree_id];
    Node* newNode;
    Cache_Handle* newNode_h = nullptr;
    if(InsertRectRec(a_branch, root_gptr, newNode_h, a_level))  // Root split
    {
        Node* a_root = nullptr;
        LockCoupleFillMap(root_gptr, &a_root);
        newNode = (Node*)((ibv_mr*)newNode_h->value)->addr;
        // Grow tree taller and new root
        Node* newRoot = nullptr;
        Cache_Handle* newroot_h = AllocNode();
        newRoot = (Node*)((ibv_mr*)newroot_h->value)->addr;
        newRoot->m_level = a_root->m_level + 1;

        Branch branch;

        // add old root node as a child of the new root
        branch.m_rect = NodeCover(a_root);
        branch.m_child = gptrs_mp[a_root->m_level].gptr;
        AddBranch(&branch, newRoot, NULL);

        // add the split node as a child of the new root
        branch.m_rect = NodeCover(newNode);
        branch.m_child = newNode_h->gptr;
        AddBranch(&branch, newRoot, NULL);

        catlog->root_gptrs[m_tree_id] = newNode_h->gptr;
        gptrs_mp->clear(); // clear the guard map. inluding the catalog guard , old root gaurd, old root sibling guard.
        return true;
    }

    return false;
}


// Find the smallest rectangle that includes all rectangles in branches of a node.
RTREE_TEMPLATE
typename RTREE_QUAL::Rect RTREE_QUAL::NodeCover(Node* a_node)
{
            RTREE_ASSERT(a_node);

    Rect rect = a_node->m_branch[0].m_rect;
    for(int index = 1; index < a_node->m_count; ++index)
    {
        rect = CombineRect(&rect, &(a_node->m_branch[index].m_rect));
    }

    return rect;
}


// Add a branch to a node.  Split the node if necessary.
// Returns 0 if node not split.  Old node updated.
// Returns 1 if node split, sets *new_node to address of new node.
// Old node updated, becomes one of two.
RTREE_TEMPLATE
bool RTREE_QUAL::AddBranch(const Branch* a_branch, Node* a_node, Cache_Handle*& a_newNode_h)
{
            RTREE_ASSERT(a_branch);
            RTREE_ASSERT(a_node);

    if(a_node->m_count < MAXNODES)  // Split won't be necessary
    {
        a_node->m_branch[a_node->m_count] = *a_branch;
        ++a_node->m_count;

        return false;
    }
    else
    {
                RTREE_ASSERT(a_newNode_h);

        SplitNode(a_node, a_branch, a_newNode_h);
        return true;
    }
}


// Disconnect a dependent node.
// Caller must return (or stop using iteration index) after this as count has changed
RTREE_TEMPLATE
void RTREE_QUAL::DisconnectBranch(Node* a_node, int a_index)
{
            RTREE_ASSERT(a_node && (a_index >= 0) && (a_index < MAXNODES));
            RTREE_ASSERT(a_node->m_count > 0);

    // Remove element by swapping with the last element to prevent gaps in array
    a_node->m_branch[a_index] = a_node->m_branch[a_node->m_count - 1];

    --a_node->m_count;
}


// Pick a branch.  Pick the one that will need the smallest increase
// in area to accomodate the new rectangle.  This will result in the
// least total area for the covering rectangles in the current node.
// In case of a tie, pick the one which was smaller before, to get
// the best resolution when searching.
RTREE_TEMPLATE
int RTREE_QUAL::PickBranch(const Rect* a_rect, Node* a_node)
{
            RTREE_ASSERT(a_rect && a_node);

    bool firstTime = true;
    ELEMTYPEREAL increase;
    ELEMTYPEREAL bestIncr = (ELEMTYPEREAL)-1;
    ELEMTYPEREAL area;
    ELEMTYPEREAL bestArea = (ELEMTYPEREAL)-1;
    int best = 0;
    Rect tempRect;

    for(int index=0; index < a_node->m_count; ++index)
    {
        Rect* curRect = &a_node->m_branch[index].m_rect;
        area = CalcRectVolume(curRect);
        tempRect = CombineRect(a_rect, curRect);
        increase = CalcRectVolume(&tempRect) - area;
        if((increase < bestIncr) || firstTime)
        {
            best = index;
            bestArea = area;
            bestIncr = increase;
            firstTime = false;
        }
        else if((increase == bestIncr) && (area < bestArea))
        {
            best = index;
            bestArea = area;
            bestIncr = increase;
        }
    }
    return best;
}


// Combine two rectangles into larger one containing both
RTREE_TEMPLATE
typename RTREE_QUAL::Rect RTREE_QUAL::CombineRect(const Rect* a_rectA, const Rect* a_rectB)
{
            RTREE_ASSERT(a_rectA && a_rectB);

    Rect newRect;

    for(int index = 0; index < NUMDIMS; ++index)
    {
        newRect.m_min[index] = RTREE_MIN(a_rectA->m_min[index], a_rectB->m_min[index]);
        newRect.m_max[index] = RTREE_MAX(a_rectA->m_max[index], a_rectB->m_max[index]);
    }

    return newRect;
}



// Split a node.
// Divides the nodes branches and the extra one between two nodes.
// Old node is one of the new ones, and one really new one is created.
// Tries more than one method for choosing a partition, uses best result.
RTREE_TEMPLATE
void RTREE_QUAL::SplitNode(Node* a_node, const Branch* a_branch, Cache_Handle*& a_newNode_h)
{
            RTREE_ASSERT(a_node);
            RTREE_ASSERT(a_branch);

    // Could just use local here, but member or external is faster since it is reused
    PartitionVars localVars;
    PartitionVars* parVars = &localVars;

    // Load all the branches into a buffer, initialize old node
    GetBranches(a_node, a_branch, parVars);

    // Find partition
    ChoosePartition(parVars, MINNODES);

    // Create a new node to hold (about) half of the branches
    a_newNode_h = AllocNode(a_node->m_level); // the guard of this new allocated node need to be extended.
    Node* newNode = (Node*)((ibv_mr*)a_newNode_h->value)->addr;
    newNode->m_level = a_node->m_level;

    // Put branches from buffer into 2 nodes according to the chosen partition
    a_node->m_count = 0;
    LoadNodes(a_node, *a_newNode_h, parVars);

            RTREE_ASSERT((a_node->m_count + (*a_node)->m_count) == parVars->m_total);
}


// Calculate the n-dimensional volume of a rectangle
RTREE_TEMPLATE
ELEMTYPEREAL RTREE_QUAL::RectVolume(Rect* a_rect)
{
            RTREE_ASSERT(a_rect);

    ELEMTYPEREAL volume = (ELEMTYPEREAL)1;

    for(int index=0; index<NUMDIMS; ++index)
    {
        volume *= a_rect->m_max[index] - a_rect->m_min[index];
    }

            RTREE_ASSERT(volume >= (ELEMTYPEREAL)0);

    return volume;
}


// The exact volume of the bounding sphere for the given Rect
RTREE_TEMPLATE
ELEMTYPEREAL RTREE_QUAL::RectSphericalVolume(Rect* a_rect)
{
            RTREE_ASSERT(a_rect);

    ELEMTYPEREAL sumOfSquares = (ELEMTYPEREAL)0;
    ELEMTYPEREAL radius;

    for(int index=0; index < NUMDIMS; ++index)
    {
        ELEMTYPEREAL halfExtent = ((ELEMTYPEREAL)a_rect->m_max[index] - (ELEMTYPEREAL)a_rect->m_min[index]) * (ELEMTYPEREAL)0.5;
        sumOfSquares += halfExtent * halfExtent;
    }

    radius = (ELEMTYPEREAL)sqrt(sumOfSquares);

    // Pow maybe slow, so test for common dims like 2,3 and just use x*x, x*x*x.
    if(NUMDIMS == 3)
    {
        return (radius * radius * radius * m_unitSphereVolume);
    }
    else if(NUMDIMS == 2)
    {
        return (radius * radius * m_unitSphereVolume);
    }
    else
    {
        return (ELEMTYPEREAL)(pow(radius, NUMDIMS) * m_unitSphereVolume);
    }
}


// Use one of the methods to calculate retangle volume
RTREE_TEMPLATE
ELEMTYPEREAL RTREE_QUAL::CalcRectVolume(Rect* a_rect)
{
#ifdef RTREE_USE_SPHERICAL_VOLUME
    return RectSphericalVolume(a_rect); // Slower but helps certain merge cases
#else // RTREE_USE_SPHERICAL_VOLUME
    return RectVolume(a_rect); // Faster but can cause poor merges
#endif // RTREE_USE_SPHERICAL_VOLUME
}


// Load branch buffer with branches from full node plus the extra branch.
RTREE_TEMPLATE
void RTREE_QUAL::GetBranches(Node* a_node, const Branch* a_branch, PartitionVars* a_parVars)
{
            RTREE_ASSERT(a_node);
            RTREE_ASSERT(a_branch);

            RTREE_ASSERT(a_node->m_count == MAXNODES);

    // Load the branch buffer
    for(int index=0; index < MAXNODES; ++index)
    {
        a_parVars->m_branchBuf[index] = a_node->m_branch[index];
    }
    a_parVars->m_branchBuf[MAXNODES] = *a_branch;
    a_parVars->m_branchCount = MAXNODES + 1;

    // Calculate rect containing all in the set
    a_parVars->m_coverSplit = a_parVars->m_branchBuf[0].m_rect;
    for(int index=1; index < MAXNODES+1; ++index)
    {
        a_parVars->m_coverSplit = CombineRect(&a_parVars->m_coverSplit, &a_parVars->m_branchBuf[index].m_rect);
    }
    a_parVars->m_coverSplitArea = CalcRectVolume(&a_parVars->m_coverSplit);
}


// Method #0 for choosing a partition:
// As the seeds for the two groups, pick the two rects that would waste the
// most area if covered by a single rectangle, i.e. evidently the worst pair
// to have in the same group.
// Of the remaining, one at a time is chosen to be put in one of the two groups.
// The one chosen is the one with the greatest difference in area expansion
// depending on which group - the rect most strongly attracted to one group
// and repelled from the other.
// If one group gets too full (more would force other group to violate min
// fill requirement) then other group gets the rest.
// These last are the ones that can go in either group most easily.
RTREE_TEMPLATE
void RTREE_QUAL::ChoosePartition(PartitionVars* a_parVars, int a_minFill)
{
            RTREE_ASSERT(a_parVars);

    bool firstTime;
    ELEMTYPEREAL biggestDiff;
    int group, chosen = 0, betterGroup = 0;

    InitParVars(a_parVars, a_parVars->m_branchCount, a_minFill);
    PickSeeds(a_parVars);

    while (((a_parVars->m_count[0] + a_parVars->m_count[1]) < a_parVars->m_total)
           && (a_parVars->m_count[0] < (a_parVars->m_total - a_parVars->m_minFill))
           && (a_parVars->m_count[1] < (a_parVars->m_total - a_parVars->m_minFill)))
    {
        firstTime = true;
        for(int index=0; index<a_parVars->m_total; ++index)
        {
            if(PartitionVars::NOT_TAKEN == a_parVars->m_partition[index])
            {
                Rect* curRect = &a_parVars->m_branchBuf[index].m_rect;
                Rect rect0 = CombineRect(curRect, &a_parVars->m_cover[0]);
                Rect rect1 = CombineRect(curRect, &a_parVars->m_cover[1]);
                ELEMTYPEREAL growth0 = CalcRectVolume(&rect0) - a_parVars->m_area[0];
                ELEMTYPEREAL growth1 = CalcRectVolume(&rect1) - a_parVars->m_area[1];
                ELEMTYPEREAL diff = growth1 - growth0;
                if(diff >= 0)
                {
                    group = 0;
                }
                else
                {
                    group = 1;
                    diff = -diff;
                }

                if(firstTime || diff > biggestDiff)
                {
                    firstTime = false;
                    biggestDiff = diff;
                    chosen = index;
                    betterGroup = group;
                }
                else if((diff == biggestDiff) && (a_parVars->m_count[group] < a_parVars->m_count[betterGroup]))
                {
                    chosen = index;
                    betterGroup = group;
                }
            }
        }
                RTREE_ASSERT(!firstTime);
        Classify(chosen, betterGroup, a_parVars);
    }

    // If one group too full, put remaining rects in the other
    if((a_parVars->m_count[0] + a_parVars->m_count[1]) < a_parVars->m_total)
    {
        if(a_parVars->m_count[0] >= a_parVars->m_total - a_parVars->m_minFill)
        {
            group = 1;
        }
        else
        {
            group = 0;
        }
        for(int index=0; index<a_parVars->m_total; ++index)
        {
            if(PartitionVars::NOT_TAKEN == a_parVars->m_partition[index])
            {
                Classify(index, group, a_parVars);
            }
        }
    }

            RTREE_ASSERT((a_parVars->m_count[0] + a_parVars->m_count[1]) == a_parVars->m_total);
            RTREE_ASSERT((a_parVars->m_count[0] >= a_parVars->m_minFill) &&
                         (a_parVars->m_count[1] >= a_parVars->m_minFill));
}


// Copy branches from the buffer into two nodes according to the partition.
RTREE_TEMPLATE
void RTREE_QUAL::LoadNodes(Node* a_nodeA, Node* a_nodeB, PartitionVars* a_parVars)
{
            RTREE_ASSERT(a_nodeA);
            RTREE_ASSERT(a_nodeB);
            RTREE_ASSERT(a_parVars);

    for(int index=0; index < a_parVars->m_total; ++index)
    {
                RTREE_ASSERT(a_parVars->m_partition[index] == 0 || a_parVars->m_partition[index] == 1);

        int targetNodeIndex = a_parVars->m_partition[index];
        Node* targetNodes[] = {a_nodeA, a_nodeB};

        // It is assured that AddBranch here will not cause a node split.
        bool nodeWasSplit = AddBranch(&a_parVars->m_branchBuf[index], targetNodes[targetNodeIndex], NULL);
                RTREE_ASSERT(!nodeWasSplit);
    }
}


// Initialize a PartitionVars structure.
RTREE_TEMPLATE
void RTREE_QUAL::InitParVars(PartitionVars* a_parVars, int a_maxRects, int a_minFill)
{
            RTREE_ASSERT(a_parVars);

    a_parVars->m_count[0] = a_parVars->m_count[1] = 0;
    a_parVars->m_area[0] = a_parVars->m_area[1] = (ELEMTYPEREAL)0;
    a_parVars->m_total = a_maxRects;
    a_parVars->m_minFill = a_minFill;
    for(int index=0; index < a_maxRects; ++index)
    {
        a_parVars->m_partition[index] = PartitionVars::NOT_TAKEN;
    }
}


RTREE_TEMPLATE
void RTREE_QUAL::PickSeeds(PartitionVars* a_parVars)
{
    bool firstTime;
    int seed0 = 0, seed1 = 0;
    ELEMTYPEREAL worst, waste;
    ELEMTYPEREAL area[MAXNODES+1];

    for(int index=0; index<a_parVars->m_total; ++index)
    {
        area[index] = CalcRectVolume(&a_parVars->m_branchBuf[index].m_rect);
    }

    firstTime = true;
    for(int indexA=0; indexA < a_parVars->m_total-1; ++indexA)
    {
        for(int indexB = indexA+1; indexB < a_parVars->m_total; ++indexB)
        {
            Rect oneRect = CombineRect(&a_parVars->m_branchBuf[indexA].m_rect, &a_parVars->m_branchBuf[indexB].m_rect);
            waste = CalcRectVolume(&oneRect) - area[indexA] - area[indexB];
            if(firstTime || waste > worst)
            {
                firstTime = false;
                worst = waste;
                seed0 = indexA;
                seed1 = indexB;
            }
        }
    }
            RTREE_ASSERT(!firstTime);

    Classify(seed0, 0, a_parVars);
    Classify(seed1, 1, a_parVars);
}


// Put a branch in one of the groups.
RTREE_TEMPLATE
void RTREE_QUAL::Classify(int a_index, int a_group, PartitionVars* a_parVars)
{
            RTREE_ASSERT(a_parVars);
            RTREE_ASSERT(PartitionVars::NOT_TAKEN == a_parVars->m_partition[a_index]);

    a_parVars->m_partition[a_index] = a_group;

    // Calculate combined rect
    if (a_parVars->m_count[a_group] == 0)
    {
        a_parVars->m_cover[a_group] = a_parVars->m_branchBuf[a_index].m_rect;
    }
    else
    {
        a_parVars->m_cover[a_group] = CombineRect(&a_parVars->m_branchBuf[a_index].m_rect, &a_parVars->m_cover[a_group]);
    }

    // Calculate volume of combined rect
    a_parVars->m_area[a_group] = CalcRectVolume(&a_parVars->m_cover[a_group]);

    ++a_parVars->m_count[a_group];
}



// Delete a data rectangle from an index structure.
// Pass in a pointer to a Rect, the tid of the record, ptr to ptr to root node.
// Returns 1 if record not found, 0 if success.
// RemoveRect provides for eliminating the root.
RTREE_TEMPLATE
bool RTREE_QUAL::RemoveRect(Rect* a_rect, const DATATYPE& a_id)
{
//            RTREE_ASSERT(a_root);
//            RTREE_ASSERT(*a_root);
    Cache_Handle* catal_h = nullptr;
    void* local_buff;
    Exclusive_Guard a_guard(local_buff, ddsm_->catalog_ptr, catal_h);
    gptrs_mp->insert({INT_MAX, std::move(a_guard)});
    CatalogPage* catlog = (CatalogPage*)((ibv_mr*)catal_h->value)->addr;
    root_gptr = catlog->root_gptrs[m_tree_id];
// todo lock coupling for remove. Is there difference between insert and remove? Need to check how to merge the node.
    ListNode* reInsertList = NULL;
    Rect new_child_rect;
    bool node_merged = false;
    if(!RemoveRectRec(a_rect, a_id, root_gptr, &reInsertList))
    {
        // Found and deleted a data item
        // Reinsert any branches from eliminated nodes
        while(reInsertList)
        {
            Node* tempNode = reInsertList->m_node;

            for(int index = 0; index < tempNode->m_count; ++index)
            {
                // TODO go over this code. should I use (tempNode->m_level - 1)?
                InsertRect(tempNode->m_branch[index],
                           tempNode->m_level);
            }

            ListNode* remLNode = reInsertList;
            reInsertList = reInsertList->m_next;

            FreeNode(remLNode->m_guard->handle_);
            FreeListNode(remLNode);
        }
        Node* a_root = nullptr;
        LockCoupleFillMap(root_gptr, &a_root);
        // Check for redundant root (not leaf, 1 child) and eliminate TODO replace
        // if with while? In case there is a whole branch of redundant roots...
        if((*a_root)->m_count == 1 && (*a_root)->IsInternalNode())
        {
            Node* tempNode = (*a_root)->m_branch[0].m_child;

                    RTREE_ASSERT(tempNode);
            FreeNode(gptrs_mp->at(a_root->m_level).handle_);
            catlog->root_gptrs[m_tree_id] = (*a_root)->m_branch[0].m_child;
        }
        gptrs_mp->clear();
        return false;
    }
    else
    {
        gptrs_mp->clear();
        return true;
    }
}


// Delete a rectangle from non-root part of an index structure.
// Called by RemoveRect.  Descends tree recursively,
// merges branches on the way back up.
// Returns 1 if record not found, 0 if success.
RTREE_TEMPLATE
bool RTREE_QUAL::RemoveRectRec(Rect* a_rect, const DATATYPE& a_id, GlobalAddress gptr, ListNode** a_listNode, Rect& new_rect, bool& need_merge)
{
    Node* a_node = nullptr;
    LockCoupleFillMap(gptr, &a_node);
    RTREE_ASSERT(a_node && a_listNode);
    RTREE_ASSERT(a_node->m_level >= 0);
    if(a_node->IsInternalNode())  // not a leaf node
    {
        for(int index = 0; index < a_node->m_count; ++index)
        {
            if(a_rect == NULL || Overlap(a_rect, &(a_node->m_branch[index].m_rect)))
            {
                Rect new_child_rect;
                bool node_merged = false;
                if(!RemoveRectRec(a_rect, a_id, a_node->m_branch[index].m_child, a_listNode, new_child_rect, node_merged))
                {

                    if(a_node->m_count < MINNODES && gptr != root_gptr)
                    {
                        need_merge = true;
                        ReInsert(a_node, a_listNode);// put next level child into delete list.
                    }
                    if(!node_merged)
                    {
                        // child was deleted, just resize parent rect. Caution: the lock coupling may falsely release its guard, the modification below is not safe.
                        a_node->m_branch[index].m_rect = new_child_rect;
                    }
                    else
                    {
                        // child was deleted, not enough entries in node, eliminate node
                        DisconnectBranch(a_node, index); // Must return after this call as count has changed
                    }
                    new_rect = NodeCover(a_node);
                    gptrs_mp->erase(a_node->m_level);
                    return false; // return false means that the node cover has been changed.
                }
            }
        }
        gptrs_mp->erase(a_node->m_level);
        return true;
    }
    else // A leaf node
    {
        for(int index = 0; index < a_node->m_count; ++index)
        {
            if(a_node->m_branch[index].m_data == a_id)
            {
                DisconnectBranch(a_node, index); // Must return after this call as count has changed
                new_rect = NodeCover(a_node);
                gptrs_mp->erase(a_node->m_level);
                if (a_node->m_count < MINNODES) {
                    need_merge = true;
                    ReInsert(a_node->m_branch[index].m_child, a_listNode);// put next level child into delete list.
                }
                return false;
            }
        }
        gptrs_mp->erase(a_node->m_level);
        return true;
    }
}


// Decide whether two rectangles overlap.
RTREE_TEMPLATE
bool RTREE_QUAL::Overlap(Rect* a_rectA, Rect* a_rectB) const
{
            RTREE_ASSERT(a_rectA && a_rectB);

    for(int index=0; index < NUMDIMS; ++index)
    {
        if (a_rectA->m_min[index] > a_rectB->m_max[index] ||
            a_rectB->m_min[index] > a_rectA->m_max[index])
        {
            return false;
        }
    }
    return true;
}

// Decide whether two rectangles overlap.
RTREE_TEMPLATE
ELEMTYPE RTREE_QUAL::SquareDistance(Rect const& a_rectA, Rect const& a_rectB) const
{
    ELEMTYPE dist{};

    for (int index = 0; index < NUMDIMS; ++index)
    {
        auto diff1 = a_rectA.m_min[index] - a_rectB.m_max[index];
        auto diff2 = a_rectA.m_max[index] - a_rectB.m_min[index];

        dist += diff1 * diff2 < 0 ? 0 : std::min(diff1 * diff1, diff2 * diff2);

    }
    return dist;
}

// Add a node to the reinsertion list.  All its branches will later
// be reinserted into the index structure.
RTREE_TEMPLATE
void RTREE_QUAL::ReInsert(Node* a_node, ListNode** a_listNode)
{
    ListNode* newListNode;
    newListNode = AllocListNode();
    newListNode->m_node = a_node;
    newListNode->m_next = *a_listNode;
    newListNode->m_guard = &gptrs_mp->at(a_node->m_level)
    *a_listNode = newListNode;
}


// Search in an index tree or subtree for all data retangles that overlap the argument rectangle.
RTREE_TEMPLATE
bool RTREE_QUAL::SearchRect(GlobalAddress node_gptr, Rect* a_rect, int& a_foundCount, std::function<bool (const DATATYPE&)> callback) const
{

    Cache_Handle* a_handle = nullptr;
    void* local_buff;
    Shared_Guard a_guard(local_buff, node_gptr, a_handle);
    Node* a_node = (Node*)local_buff;

    RTREE_ASSERT(a_node);
    RTREE_ASSERT(a_node->m_level >= 0);
    RTREE_ASSERT(a_rect);

    if(a_node->IsInternalNode())
    {
        // This is an internal node in the tree
        for(int index=0; index < a_node->m_count; ++index)
        {
            if(Overlap(a_rect, &a_node->m_branch[index].m_rect))
            {
                if(!SearchRect(a_node->m_branch[index].m_child, a_rect, a_foundCount, callback))
                {
                    // The callback indicated to stop searching
                    return false;
                }
            }
        }
    }
    else
    {
        // This is a leaf node
        for(int index=0; index < a_node->m_count; ++index)
        {
            if(Overlap(a_rect, &a_node->m_branch[index].m_rect))
            {
                DATATYPE& id = a_node->m_branch[index].m_data;
                ++a_foundCount;

                if(callback && !callback(id))
                {
                    return false; // Don't continue searching
                }
            }
        }
    }

    return true; // Continue searching
}


RTREE_TEMPLATE
std::vector<typename RTREE_QUAL::Rect> RTREE_QUAL::ListTree() const
{
            RTREE_ASSERT(m_root);
            RTREE_ASSERT(m_root->m_level >= 0);

    std::vector<Rect> treeList;

    std::vector<Node*> toVisit;
    toVisit.push_back(m_root);

    while (!toVisit.empty()) {
        Node* a_node = toVisit.back();
        toVisit.pop_back();
        if(a_node->IsInternalNode())
        {
            // This is an internal node in the tree
            for(int index=0; index < a_node->m_count; ++index)
            {
                treeList.push_back(a_node->m_branch[index].m_rect);
                toVisit.push_back(a_node->m_branch[index].m_child);
            }
        }
        else
        {
            // This is a leaf node
            for(int index=0; index < a_node->m_count; ++index)
            {
                treeList.push_back(a_node->m_branch[index].m_rect);
            }
        }
    }

    return treeList;
}
