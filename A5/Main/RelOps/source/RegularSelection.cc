
#ifndef REG_SELECTION_C                                        
#define REG_SELECTION_C

#include "RegularSelection.h"

RegularSelection :: RegularSelection (MyDB_TableReaderWriterPtr input, MyDB_TableReaderWriterPtr output,
                string selectionPredicate, vector <string> projections) {
    this->input = input;
    this->output = output;
    this->selectionPredicate = selectionPredicate;
    this->projections = projections;
}

void RegularSelection :: run () {
    MyDB_RecordPtr inputRec = input->getEmptyRecord();

    func predicate = inputRec->compileComputation(selectionPredicate);

    // and get the final set of computatoins that will be used to buld the output record
    vector <func> finalComputations;
    for (string s : projections) {
        finalComputations.push_back(inputRec->compileComputation(s));
    }

    MyDB_RecordPtr outputRec = output->getEmptyRecord();

    MyDB_RecordIteratorAltPtr iter = input->getIteratorAlt();
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
