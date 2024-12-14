
#ifndef LOG_OP_CC
#define LOG_OP_CC

#include "MyDB_LogicalOps.h"
#include "Aggregate.h"
#include "BPlusSelection.h"
#include "RegularSelection.h"
#include "ScanJoin.h"
#include "SortMergeJoin.h"

string generateSelectionPredicate(vector<ExprTreePtr> exprs)
{
    string result = "";
    if (exprs.size() == 0) {
        return "bool[true]";
    }

    result = exprs[0]->toAttString();

    for (int i = 1; i < exprs.size(); i++) {
        result = "&& (" + result + ", " + exprs[i]->toAttString() + ")";
    }

    return result;
}

MyDB_TableReaderWriterPtr LogicalTableScan :: execute (map <string, MyDB_TableReaderWriterPtr> &allTableReaderWriters,
	map <string, MyDB_BPlusTreeReaderWriterPtr> &allBPlusReaderWriters) {
	
	// your code here!
	cout << "DEBUG: Scan start!" << endl;
	cout << "input schema: " << inputSpec->getSchema() << endl;
	cout << "output schema: " << outputSpec->getSchema() << endl;
	cout << "selection predicate: " << endl;
	for (auto x : selectionPred) {
		cout <<"  "<< x->toString() << endl;
	}

    string inputTableName = this->getInputTableName();
    MyDB_TableReaderWriterPtr inputTable = allTableReaderWriters[inputTableName];
    MyDB_TableReaderWriterPtr outputTable = make_shared<MyDB_TableReaderWriter>(outputSpec, inputTable->getBufferMgr());
    
    vector<string> projections;

    cout << "DEBUG: projections:" << endl;
    for (auto att : outputSpec->getSchema()->getAtts()) {
        string attName = att.first;
        size_t pos = attName.find('_');
        attName = attName.substr(pos + 1);
        projections.push_back("["+ attName +"]");
        cout << "[" + attName + "]" << endl;
    }

    string selectionPredStr = generateSelectionPredicate(selectionPred);
    cout << "DEBUG: selectionPredicate: " << endl;
    cout << "  " << selectionPredStr << endl << endl;

    RegularSelection regularSelection(inputTable, outputTable, selectionPredStr, projections);
    regularSelection.run();

    return outputTable;
}

MyDB_TableReaderWriterPtr LogicalJoin :: execute (map <string, MyDB_TableReaderWriterPtr> &allTableReaderWriters,
	map <string, MyDB_BPlusTreeReaderWriterPtr> &allBPlusReaderWriters) {

	// your code here!

	return nullptr;
}

#endif
