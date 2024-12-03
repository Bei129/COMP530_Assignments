#ifndef SFW_QUERY_CC
#define SFW_QUERY_CC

#include <algorithm>
#include <cassert>
#include <cmath>
#include <limits>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <iostream>
#include <chrono>
#include <ctime>
#include <iomanip>
#include "ParserTypes.h"

#define DEBUG_MSG(str)                                                                      \
    do                                                                                      \
    {                                                                                       \
        auto now = std::chrono::system_clock::now();                                        \
        auto now_c = std::chrono::system_clock::to_time_t(now);                             \
        auto now_tm = std::localtime(&now_c);                                               \
        std::cerr << std::put_time(now_tm, "%T") << "  ";                                   \
        std::cerr << "DEBUG: " << __FILE__ << "(" << __LINE__ << "): " << str << std::endl; \
        std::cerr.flush();                                                                  \
    } while (false)


// 似乎有问题
void updateAttributesWithAlias(ExprTreePtr expr, const std::map<std::string, MyDB_TablePtr> &tablesWithAliases) {
    if (!expr) return;

    if (expr->isId()) {
        // 获取当前的表名和属性名
        std::string originalId = expr->getId();
        for (const auto &tableAlias : tablesWithAliases) {
            // 如果找到匹配的表，更新属性名
            if (originalId.find(tableAlias.first + "_") == 0) {
                // 生成新的属性名：前缀为别名
                std::string newId = tableAlias.second->getName() + "_" + originalId.substr(tableAlias.first.size() + 1);
                // 将新的属性名应用到表达式中
                expr = std::make_shared<Identifier>((char *)tableAlias.first.c_str(), (char *)newId.c_str());
                break;
            }
        }
    }

    if (expr->getLHS()) {
        updateAttributesWithAlias(expr->getLHS(), tablesWithAliases);
    }
    if (expr->getRHS()) {
        updateAttributesWithAlias(expr->getRHS(), tablesWithAliases);
    }
    if (expr->getChild()) {
        updateAttributesWithAlias(expr->getChild(), tablesWithAliases);
    }
}
	
// builds and optimizes a logical query plan for a SFW query, returning the logical query plan
pair<LogicalOpPtr, double> SFWQuery::optimizeQueryPlan(map<string, MyDB_TablePtr>& allTables) {
    DEBUG_MSG("Starting optimizeQueryPlan");

    cerr << "Debug: allTables size: " << allTables.size() << endl;
    for (const auto& tablePair : allTables) {
        cerr << "Table: " << tablePair.first;
        if (tablePair.second) {
            cerr << ", Name: " << tablePair.second->getName() << endl;
        } else {
            cerr << ", Ptr is null." << endl;
        }
    }

    cerr << "Debug: tablesToProcess size: " << tablesToProcess.size() << endl;
    for (const auto& pair : tablesToProcess) {
        cerr << "Table: " << pair.first << ", Alias: " << pair.second << endl;
    }

    for (auto& tableAliasPair : tablesToProcess) {
        string tableName = tableAliasPair.first;
        if (allTables.find(tableName) == allTables.end()) {
            cerr << "Error: Table " << tableName << " not found in allTables." << endl;
        } else {
            cerr << "Table " << tableName << " is valid in allTables." << endl;
        }
    }

    MyDB_SchemaPtr totSchema = make_shared<MyDB_Schema>();
    map<string, MyDB_TablePtr> tablesWithAliases;


    for (auto& tableAliasPair : this->tablesToProcess) {
        string tableName = tableAliasPair.first;
        string alias = tableAliasPair.second;

        MyDB_TablePtr aliasedTable = allTables[tableName]->alias(alias);
        tablesWithAliases[alias] = aliasedTable;

        for (auto& att : aliasedTable->getSchema()->getAtts()) {
            totSchema->appendAtt(att);
        }
    }

    vector<ExprTreePtr> allDisjunctions = this->allDisjunctions;

    // 更新所有谓词中的属性名，添加别名前缀
    for (auto &expr : allDisjunctions) {
        updateAttributesWithAlias(expr, tablesWithAliases);
        cerr << "Updated disjunction: " << expr->toString() << endl;
    }

    return optimizeQueryPlan(tablesWithAliases, totSchema, allDisjunctions);
}

// builds and optimizes a logical query plan for a SFW query, returning the logical query plan
pair <LogicalOpPtr, double> SFWQuery :: optimizeQueryPlan (map <string, MyDB_TablePtr> &allTables, 
	MyDB_SchemaPtr totSchema, vector <ExprTreePtr> &allDisjunctions) {

	LogicalOpPtr res = nullptr;
	// cost = 9e99;
	double cost = 9e99;

	// case where no joins
	if (allTables.size () == 1) {

		// some code here...
		// return make_pair (res, best);

        auto tableEntry = *allTables.begin();
        MyDB_TablePtr table = tableEntry.second;

        // 获取统计信息并计算选择成本
        MyDB_StatsPtr stats = make_shared<MyDB_Stats>(table);
        stats = stats->costSelection(allDisjunctions);

        vector<ExprTreePtr> predicatesForThisTable;

        // 过滤出仅与当前表相关的谓词
        for (auto &expr : allDisjunctions) {
            if (expr->referencesTable(table->getName())) {
                predicatesForThisTable.push_back(expr);
            }
        }

        res = make_shared<LogicalTableScan>(table, table, stats, predicatesForThisTable);
        cost = stats->getTupleCount();
        return make_pair(res, cost);
	}

	// we have at least one join
	// some code here...
	// return make_pair (res, best);

    // 多表情况，初始化最优计划和成本
    vector<string> tableNames;
    for (const auto &entry : allTables) {
        tableNames.push_back(entry.first);
    }

    // 遍历所有可能的左右表分组
    for (size_t i = 1; i < (1 << tableNames.size()) - 1; ++i) {
        map<string, MyDB_TablePtr> leftTables, rightTables;

        cerr << "Left Tables:" << endl;
        for (const auto& tablePair : leftTables) {
            cerr << "Alias: " << tablePair.first 
                << ", Table Name: " << tablePair.second->getName() << endl;
        }
        cerr << "Right Tables:" << endl;
        for (const auto& tablePair : rightTables) {
            cerr << "Alias: " << tablePair.first 
                << ", Table Name: " << tablePair.second->getName() << endl;
        }

        for (size_t j = 0; j < tableNames.size(); ++j) {
            if (i & (1 << j)) {
                leftTables[tableNames[j]] = allTables[tableNames[j]];
            } else {
                rightTables[tableNames[j]] = allTables[tableNames[j]];
            }
        }

        auto leftPlan = optimizeQueryPlan(leftTables, totSchema, allDisjunctions);
        auto rightPlan = optimizeQueryPlan(rightTables, totSchema, allDisjunctions);

        // 获取 Join 的统计信息
        MyDB_StatsPtr joinStats = leftPlan.first->getStats()->costJoin(allDisjunctions, rightPlan.first->getStats());

        MyDB_TablePtr joinTable = make_shared<MyDB_Table>("joinResult", "tempStorage");
        LogicalOpPtr joinOp = make_shared<LogicalJoin>(leftPlan.first, rightPlan.first, joinTable, allDisjunctions, joinStats);

        double totalCost = leftPlan.second + rightPlan.second + joinStats->getTupleCount();

        if (totalCost < cost) {
            res = joinOp;
            cost = totalCost;
        }
    }

    return make_pair(res, cost);

}

void SFWQuery :: print () {
	cout << "Selecting the following:\n";
	for (auto a : valuesToSelect) {
		cout << "\t" << a->toString () << "\n";
	}
	cout << "From the following:\n";
	for (auto a : tablesToProcess) {
		cout << "\t" << a.first << " AS " << a.second << "\n";
	}
	cout << "Where the following are true:\n";
	for (auto a : allDisjunctions) {
		cout << "\t" << a->toString () << "\n";
	}
	cout << "Group using:\n";
	for (auto a : groupingClauses) {
		cout << "\t" << a->toString () << "\n";
	}
}


SFWQuery :: SFWQuery (struct ValueList *selectClause, struct FromList *fromClause,
        struct CNF *cnf, struct ValueList *grouping) {
        valuesToSelect = selectClause->valuesToCompute;
        tablesToProcess = fromClause->aliases;
        allDisjunctions = cnf->disjunctions;
        groupingClauses = grouping->valuesToCompute;
}

SFWQuery::SFWQuery(ValueList* selectClause, FromList* fromClause, CNF* cnf) {
    // debug用
    // std::cerr << "Entering SFWQuery constructor.\n";

    // if (!selectClause) {
    //     std::cerr << "Error: selectClause is null.\n";
    //     exit(1);
    // }
    // if (!fromClause) {
    //     std::cerr << "Error: fromClause is null.\n";
    //     exit(1);
    // }
    // if (!cnf) {
    //     std::cerr << "Error: cnf is null.\n";
    //     exit(1);
    // }

    // std::cerr << "fromClause->aliases size: " << fromClause->aliases.size() << "\n";
    // for (const auto& aliasPair : fromClause->aliases) {
    //     std::cerr << "Table: " << aliasPair.first << ", Alias: " << aliasPair.second << "\n";
    // }

    // std::cerr << "selectClause->valuesToCompute size: " << selectClause->valuesToCompute.size() << "\n";
    // for (const auto& value : selectClause->valuesToCompute) {
    //     if (value) {
    //         std::cerr << "Value: " << value->toString() << "\n";
    //     } else {
    //         std::cerr << "Value: null pointer.\n";
    //     }
    // }

    // std::cerr << "cnf->disjunctions size: " << cnf->disjunctions.size() << "\n";
    // for (const auto& expr : cnf->disjunctions) {
    //     if (expr) {
    //         std::cerr << "Disjunction: " << expr->toString() << "\n";
    //     } else {
    //         std::cerr << "Disjunction: null pointer.\n";
    //     }
    // }

    valuesToSelect = selectClause->valuesToCompute;
    tablesToProcess = fromClause->aliases;
	allDisjunctions = cnf->disjunctions;
}


SFWQuery :: SFWQuery (struct ValueList *selectClause, struct FromList *fromClause) {
        valuesToSelect = selectClause->valuesToCompute;
        tablesToProcess = fromClause->aliases;
        allDisjunctions.push_back (make_shared <BoolLiteral> (true));
}

#endif
