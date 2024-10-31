
#ifndef BPLUS_SELECTION_C                                        
#define BPLUS_SELECTION_C

#include "BPlusSelection.h"

BPlusSelection :: BPlusSelection (MyDB_BPlusTreeReaderWriterPtr input, MyDB_TableReaderWriterPtr output,
                MyDB_AttValPtr low, MyDB_AttValPtr high,
                string selectionPredicate, vector <string> projections) {
    this->input = input;
    this->output = output;
    this->low = low;
    this->high = high;
    this->selectionPredicate = selectionPredicate;
    this->projections = projections;
}

void BPlusSelection :: run () {
    MyDB_RecordPtr inputRec = input->getEmptyRecord();

    func predicate = inputRec->compileComputation(selectionPredicate);

    // and get the final set of computatoins that will be used to buld the output record
    vector <func> finalComputations;
    for (string s : projections) {
        finalComputations.push_back(inputRec->compileComputation(s));
    }

    MyDB_RecordPtr outputRec = output->getEmptyRecord();

    MyDB_RecordIteratorAltPtr iter = input->getRangeIteratorAlt(low, high);
    while (iter->advance()) {
        iter->getCurrent(inputRec);

        if (predicate()->toBool()) {

            // run all of the computations
            int i = 0;
            for (auto& f : finalComputations) {
                outputRec->getAtt(i++)->set(f());
            }

            outputRec->recordContentHasChanged();
            output->append(outputRec);
        }
    }
}

#endif
