#ifndef RTREE_H
#define RTREE_H

// NOTE This file compiles under MSVC 6 SP5 and MSVC .Net 2003 it may not work on other compilers without modification.

// NOTE These next few lines may be win32 specific, you may need to modify them to compile on other platform
#include <stdio.h>
#include <math.h>
#include <assert.h>
#include <stdlib.h>
#include "include/DDSM.h"
#include <algorithm>
#include <functional>
#include <vector>
#include <queue>

#define RTREE_ASSERT assert // RTree uses RTREE_ASSERT( condition )
#ifdef Min
  #define RTREE_MIN Min
#else
  #define RTREE_MIN std::min
#endif //Min
#ifdef Max
  #define RTREE_MAX Max
#else
  #define RTREE_MAX std::max
#endif //Max

//
// RTree.h
//

#define RTREE_TEMPLATE template<class DATATYPE, class ELEMTYPE, int NUMDIMS, class ELEMTYPEREAL, int TMAXNODES, int TMINNODES>
#define RTREE_QUAL RTree<DATATYPE, ELEMTYPE, NUMDIMS, ELEMTYPEREAL, TMAXNODES, TMINNODES>

#define RTREE_DONT_USE_MEMPOOLS // This version does not contain a fixed memory allocator, fill in lines with EXAMPLE to implement one.
#define RTREE_USE_SPHERICAL_VOLUME // Better split classification, may be slower on some systems
using namespace DSMEngine;
// Fwd decl
class RTFileStream;  // File I/O helper class, look below for implementation and notes.


/// \class RTree
/// Implementation of RTree, a multidimensional bounding rectangle tree.
/// Example usage: For a 3-dimensional tree use RTree<Object*, float, 3> myTree;
///
/// This modified, templated C++ version by Greg Douglas at Auran (http://www.auran.com)
///
/// DATATYPE Referenced data, should be int, void*, obj* etc. no larger than sizeof<void*> and simple type
/// ELEMTYPE Type of element such as int or float
/// NUMDIMS Number of dimensions such as 2 or 3
/// ELEMTYPEREAL Type of element that allows fractional and large values such as float or double, for use in volume calcs
///
/// NOTES: Inserting and removing data requires the knowledge of its constant Minimal Bounding Rectangle.
///        This version uses new/delete for nodes, I recommend using a fixed size allocator for efficiency.
///        Instead of using a callback function for returned results, I recommend and efficient pre-sized, grow-only memory
///        array similar to MFC CArray or STL Vector for returning search query result.
///
template<class DATATYPE, class ELEMTYPE, int NUMDIMS,
         class ELEMTYPEREAL = ELEMTYPE, int TMAXNODES = 8, int TMINNODES = TMAXNODES / 2>
class RTree
{
  static_assert(std::numeric_limits<ELEMTYPEREAL>::is_iec559, "'ELEMTYPEREAL' accepts floating-point types only");

protected:

  struct Node;  // Fwd decl.  Used by other internal structs and iterator

public:

  // These constant must be declared after Branch and before Node struct
  // Stuck up here for MSVC 6 compiler.  NSVC .NET 2003 is much happier.
  enum
  {
    MAXNODES = TMAXNODES,                         ///< Max elements in node
    MINNODES = TMINNODES,                         ///< Min elements in node
  };

public:

  RTree(uint16_t Btr_id,DDSM* ddsm);
  RTree(const RTree& other);
  virtual ~RTree();

  /// Insert entry
  /// \param a_min Min of bounding rect
  /// \param a_max Max of bounding rect
  /// \param a_dataId Positive Id of data.  Maybe zero, but negative numbers not allowed.
  void Insert(const ELEMTYPE a_min[NUMDIMS], const ELEMTYPE a_max[NUMDIMS], const DATATYPE& a_dataId);

  /// Remove entry (traverse the whole tree and removes every occurrence)
  /// \param a_dataId Positive Id of data.  Maybe zero, but negative numbers not allowed.
  void Remove(const DATATYPE& a_dataId);

  /// Remove entry (only if inside the given rect)
  /// \param a_min Min of bounding rect
  /// \param a_max Max of bounding rect
  /// \param a_dataId Positive Id of data.  Maybe zero, but negative numbers not allowed.
  void Remove(const ELEMTYPE a_min[NUMDIMS], const ELEMTYPE a_max[NUMDIMS], const DATATYPE& a_dataId);

  /// Find all within search rectangle
  /// \param a_min Min of search bounding rect
  /// \param a_max Max of search bounding rect
  /// \param a_searchResult Search result array.  Caller should set grow size. Function will reset, not append to array.
  /// \param a_resultCallback Callback function to return result.  Callback should return 'true' to continue searching
  /// \param a_context User context to pass as parameter to a_resultCallback
  /// \return Returns the number of entries found
  int Search(const ELEMTYPE a_min[NUMDIMS], const ELEMTYPE a_max[NUMDIMS], std::function<bool (const DATATYPE&)> callback) const;

  /// Find the nearest neighbors
  /// \param a_min Min of search bounding rect
  /// \param a_max Max of search bounding rect
  /// \param a_resultCallback Callback function to return result.  The Callback takes both the resulting data point and the calculated square distance. Callback should return 'true' to continue searching
  /// \return Returns the number of entries found
  size_t NNSearch(const ELEMTYPE a_min[NUMDIMS], const ELEMTYPE a_max[NUMDIMS], std::function<bool(const DATATYPE&, ELEMTYPE)> callback) const;


  /// Remove all entries from tree
  void RemoveAll();

  /// Count the data elements in this container.  This is slow as no internal counter is maintained.
  int Count();

  /// Load tree contents from file
  bool Load(const char* a_fileName);
  /// Load tree contents from stream
  bool Load(RTFileStream& a_stream);


  /// Save tree contents to file
  bool Save(const char* a_fileName);
  /// Save tree contents to stream
  bool Save(RTFileStream& a_stream);

  /// Iterator is not remove safe.
  class Iterator
  {
  private:

    enum { MAX_STACK = 32 }; //  Max stack size. Allows almost n^32 where n is number of branches in node

    struct StackElement
    {
      Node* m_node;
      int m_branchIndex;
    };

  public:

    Iterator()                                    { Init(); }

    ~Iterator()                                   { }

    /// Is iterator invalid
    bool IsNull()                                 { return (m_tos <= 0); }

    /// Is iterator pointing to valid data
    bool IsNotNull()                              { return (m_tos > 0); }

    /// Access the current data element. Caller must be sure iterator is not NULL first.
    DATATYPE& operator*()
    {
      RTREE_ASSERT(IsNotNull());
      StackElement& curTos = m_stack[m_tos - 1];
      return curTos.m_node->m_branch[curTos.m_branchIndex].m_data;
    }

    /// Access the current data element. Caller must be sure iterator is not NULL first.
    const DATATYPE& operator*() const
    {
      RTREE_ASSERT(IsNotNull());
      StackElement& curTos = m_stack[m_tos - 1];
      return curTos.m_node->m_branch[curTos.m_branchIndex].m_data;
    }

    /// Find the next data element
    bool operator++()                             { return FindNextData(); }

    /// Get the bounds for this node
    void GetBounds(ELEMTYPE a_min[NUMDIMS], ELEMTYPE a_max[NUMDIMS])
    {
      RTREE_ASSERT(IsNotNull());
      StackElement& curTos = m_stack[m_tos - 1];
      Branch& curBranch = curTos.m_node->m_branch[curTos.m_branchIndex];

      for(int index = 0; index < NUMDIMS; ++index)
      {
        a_min[index] = curBranch.m_rect.m_min[index];
        a_max[index] = curBranch.m_rect.m_max[index];
      }
    }

  private:

    /// Reset iterator
    void Init()                                   { m_tos = 0; }

    /// Find the next data element in the tree (For internal use only)
    bool FindNextData()
    {
      for(;;)
      {
        if(m_tos <= 0)
        {
          return false;
        }
        StackElement curTos = Pop(); // Copy stack top cause it may change as we use it

        if(curTos.m_node->IsLeaf())
        {
          // Keep walking through data while we can
          if(curTos.m_branchIndex+1 < curTos.m_node->m_count)
          {
            // There is more data, just point to the next one
            Push(curTos.m_node, curTos.m_branchIndex + 1);
            return true;
          }
          // No more data, so it will fall back to previous level
        }
        else
        {
          if(curTos.m_branchIndex+1 < curTos.m_node->m_count)
          {
            // Push sibling on for future tree walk
            // This is the 'fall back' node when we finish with the current level
            Push(curTos.m_node, curTos.m_branchIndex + 1);
          }
          // Since cur node is not a leaf, push first of next level to get deeper into the tree
          Node* nextLevelnode = curTos.m_node->m_branch[curTos.m_branchIndex].m_child;
          Push(nextLevelnode, 0);

          // If we pushed on a new leaf, exit as the data is ready at TOS
          if(nextLevelnode->IsLeaf())
          {
            return true;
          }
        }
      }
    }

    /// Push node and branch onto iteration stack (For internal use only)
    void Push(Node* a_node, int a_branchIndex)
    {
      m_stack[m_tos].m_node = a_node;
      m_stack[m_tos].m_branchIndex = a_branchIndex;
      ++m_tos;
      RTREE_ASSERT(m_tos <= MAX_STACK);
    }

    /// Pop element off iteration stack (For internal use only)
    StackElement& Pop()
    {
      RTREE_ASSERT(m_tos > 0);
      --m_tos;
      return m_stack[m_tos];
    }

    StackElement m_stack[MAX_STACK];              ///< Stack as we are doing iteration instead of recursion
    int m_tos;                                    ///< Top Of Stack index

    friend class RTree; // Allow hiding of non-public functions while allowing manipulation by logical owner
  };

  /// Get 'first' for iteration
  void GetFirst(Iterator& a_it)
  {
    a_it.Init();
    Node* first = m_root;
    while(first)
    {
      if(first->IsInternalNode() && first->m_count > 1)
      {
        a_it.Push(first, 1); // Descend sibling branch later
      }
      else if(first->IsLeaf())
      {
        if(first->m_count)
        {
          a_it.Push(first, 0);
        }
        break;
      }
      first = first->m_branch[0].m_child;
    }
  }

  /// Get Next for iteration
  void GetNext(Iterator& a_it)                    { ++a_it; }

  /// Is iterator NULL, or at end?
  bool IsNull(Iterator& a_it)                     { return a_it.IsNull(); }

  /// Get object at iterator position
  DATATYPE& GetAt(Iterator& a_it)                 { return *a_it; }

protected:

  /// Minimal bounding rectangle (n-dimensional)
  struct Rect
  {
    ELEMTYPE m_min[NUMDIMS];                      ///< Min dimensions of bounding box
    ELEMTYPE m_max[NUMDIMS];                      ///< Max dimensions of bounding box
  };

  /// May be data or may be another subtree
  /// The parents level determines this.
  /// If the parents level is 0, then this is data
  struct Branch
  {
    Rect m_rect;                                  ///< Bounds
//    Node* m_child = NULL;                         ///< Child node
    GlobalAddress m_child = GlobalAddress::Null();                ///< Global Address of Child node
    DATATYPE m_data;                              ///< Data Id
  };

  /// Node for each branch level
  struct Node
  {
    bool IsInternalNode()                         { return (m_level > 0); } // Not a leaf, but a internal node
    bool IsLeaf()                                 { return (m_level == 0); } // A leaf, contains data

    int m_count;                                  ///< Count
    int m_level;                                  ///< Leaf is zero, others positive
    Branch m_branch[MAXNODES];                    ///< Branch
  };

  /// A link list of nodes for reinsertion after a delete operation
  struct ListNode
  {
    ListNode* m_next;                             ///< Next in list
    Node* m_node;                                 ///< Node
    SELCC_Guard* m_guard;
  };

  /// Variables for finding a split partition
  struct PartitionVars
  {
    enum { NOT_TAKEN = -1 }; // indicates that position

    int m_partition[MAXNODES+1];
    int m_total;
    int m_minFill;
    int m_count[2];
    Rect m_cover[2];
    ELEMTYPEREAL m_area[2];

    Branch m_branchBuf[MAXNODES+1];
    int m_branchCount;
    Rect m_coverSplit;
    ELEMTYPEREAL m_coverSplitArea;
  };

  Cache_Handle * AllocNode(int level = INT_MAX);
  void FreeNode(Cache_Handle* node_h);
  void InitNode(Node* a_node);
  void InitRect(Rect* a_rect);
  bool InsertRectRec(const Branch& a_branch, GlobalAddress a_node_gptr, Cache_Handle*& newnode_h_this, int a_level);
  bool InsertRect(const Branch& a_branch, int a_level);
//  void LockCoupleFillMap(GlobalAddress node_gptr, Node** node);
  void LockCoupleFillMap(GlobalAddress node_gptr, Node** node);
  void LockCoupleCleanMap(Node* node);
  Rect NodeCover(Node* a_node);
  bool AddBranch(const Branch* a_branch, Node* a_node, Cache_Handle* & a_newNode_h);
  void DisconnectBranch(Node* a_node, int a_index);
  int PickBranch(const Rect* a_rect, Node* a_node);
  Rect CombineRect(const Rect* a_rectA, const Rect* a_rectB);
  void SplitNode(Node* a_node, const Branch* a_branch, Cache_Handle* & a_newNode_h);
  ELEMTYPEREAL RectSphericalVolume(Rect* a_rect);
  ELEMTYPEREAL RectVolume(Rect* a_rect);
  ELEMTYPEREAL CalcRectVolume(Rect* a_rect);
  void GetBranches(Node* a_node, const Branch* a_branch, PartitionVars* a_parVars);
  void ChoosePartition(PartitionVars* a_parVars, int a_minFill);
  void LoadNodes(Node* a_nodeA, Node* a_nodeB, PartitionVars* a_parVars);
  void InitParVars(PartitionVars* a_parVars, int a_maxRects, int a_minFill);
  void PickSeeds(PartitionVars* a_parVars);
  void Classify(int a_index, int a_group, PartitionVars* a_parVars);
  bool RemoveRect(Rect* a_rect, const DATATYPE& a_id);
  bool RemoveRectRec(Rect* a_rect, const DATATYPE& a_id, GlobalAddress gptr, ListNode** a_listNode, Rect& new_rect, bool& need_merge);
  ListNode* AllocListNode();
  void FreeListNode(ListNode* a_listNode);
  bool Overlap(Rect* a_rectA, Rect* a_rectB) const;
  ELEMTYPE SquareDistance(Rect const& a_rectA, Rect const& a_rectB) const;
  void ReInsert(Node* a_node, ListNode** a_listNode);
  bool SearchRect(GlobalAddress a_node_h, Rect* a_rect, int& a_foundCount, std::function<bool (const DATATYPE&)> callback) const;
  void RemoveAllRec(Node* a_node);
  void Reset();
  void CountRec(Node* a_node, int& a_count);

  bool SaveRec(Node* a_node, RTFileStream& a_stream);
  bool LoadRec(Node* a_node, RTFileStream& a_stream);
  void CopyRec(Node* current, Node* other);
  uint16_t m_tree_id;
  Cache_Handle* root_handle;                                    ///< Root of tree
  GlobalAddress root_gptr;                                    ///< Root of tree
  Node* m_root;                                    ///< Root of tree
  std::mutex root_mtx;
  ELEMTYPEREAL m_unitSphereVolume;                 ///< Unit sphere constant for required number of dimensions
  DSMEngine::DDSM* ddsm_;
  static thread_local std::map<int, SELCC_Guard>* gptrs_mp;
public:
  // return all the AABBs that form the RTree
  std::vector<Rect> ListTree() const;
};


// Because there is not stream support, this is a quick and dirty file I/O helper.
// Users will likely replace its usage with a Stream implementation from their favorite API.
class RTFileStream
{
  FILE* m_file;

public:


  RTFileStream()
  {
    m_file = NULL;
  }

  ~RTFileStream()
  {
    Close();
  }

  bool Open(const char* a_fileName, const char* mode)
  {
#if defined(_WIN32) && defined(__STDC_WANT_SECURE_LIB__)
    return fopen_s(&m_file, a_fileName, mode) == 0;
#else
    m_file = fopen(a_fileName, mode);
    return m_file != nullptr;
#endif
  }

  bool OpenRead(const char* a_fileName)
  {
    return this->Open(a_fileName, "rb");
  }

  bool OpenWrite(const char* a_fileName)
  {
    return this->Open(a_fileName, "wb");
  }

  void Close()
  {
    if(m_file)
    {
      fclose(m_file);
      m_file = NULL;
    }
  }

  template< typename TYPE >
  size_t Write(const TYPE& a_value)
  {
    RTREE_ASSERT(m_file);
    return fwrite((void*)&a_value, sizeof(a_value), 1, m_file);
  }

  template< typename TYPE >
  size_t WriteArray(const TYPE* a_array, int a_count)
  {
    RTREE_ASSERT(m_file);
    return fwrite((void*)a_array, sizeof(TYPE) * a_count, 1, m_file);
  }

  template< typename TYPE >
  size_t Read(TYPE& a_value)
  {
    RTREE_ASSERT(m_file);
    return fread((void*)&a_value, sizeof(a_value), 1, m_file);
  }

  template< typename TYPE >
  size_t ReadArray(TYPE* a_array, int a_count)
  {
    RTREE_ASSERT(m_file);
    return fread((void*)a_array, sizeof(TYPE) * a_count, 1, m_file);
  }
};


#include "RTree.tpp"
//#undef RTREE_TEMPLATE
//#undef RTREE_QUAL

#endif //RTREE_H

