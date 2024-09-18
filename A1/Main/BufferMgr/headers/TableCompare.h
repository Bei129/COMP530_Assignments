#ifndef TABLE_COMPARE_H
#define TABLE_COMPARE_H

#include "MyDB_Table.h"

class TableCompare {
public:
    // Compare two table pointers
    bool operator()(const MyDB_Table *table1, const MyDB_Table *table2) const {
        return table1 == table2;
    }
};

#endif
