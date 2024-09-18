#ifndef PAGE_COMPARE_H
#define PAGE_COMPARE_H

#include "MyDB_Page.h"

class PageCompare {
public:
    // Compares two page pointers
    bool operator()(const MyDB_Page *page1, const MyDB_Page *page2) const {
        return page1 == page2; 
    }
};

#endif
