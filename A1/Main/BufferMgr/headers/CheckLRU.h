#ifndef CHECK_LRU_H
#define CHECK_LRU_H

#include <list>

class CheckLRU {
public:
    CheckLRU(std::list<long> &lruList) : lruList(lruList) {}

    // Checks if the given page ID is at the front of the LRU list
    bool isMostRecentlyUsed(long pageID) {
        return !lruList.empty() && lruList.front() == pageID;
    }

    // Checks if the given page ID is at the back of the LRU list
    bool isLeastRecentlyUsed(long pageID) {
        return !lruList.empty() && lruList.back() == pageID;
    }

private:
    std::list<long> &lruList; 
};

#endif
