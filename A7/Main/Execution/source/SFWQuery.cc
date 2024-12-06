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

int SFWQuery::tempTableId = 0;

ExprTreePtr updateAttributesWithAlias(ExprTreePtr expr, const std::map<std::string, MyDB_TablePtr> &tablesWithAliases) {
    if (!expr) return nullptr;

    if (expr->isId()) {
        std::string id = expr->getId(); // 获取原始的表名和属性名
        size_t pos = id.find('_');
        if (pos != std::string::npos) {
            std::string tableName = id.substr(0, pos);
            std::string attName = id.substr(pos + 1);

            // 查找匹配的表别名
            for (const auto &tableAlias : tablesWithAliases) {
                if (tableName == tableAlias.first) {
                    std::string newTableName = tableAlias.first;
                    std::string newAttName = attName;
                    return make_shared<Identifier>((char *)newTableName.c_str(), (char *)newAttName.c_str());
                }
            }
        }

        return expr;
    } 
	else if (dynamic_cast<EqOp*>(expr.get()) != nullptr) {
        ExprTreePtr newLHS = updateAttributesWithAlias(expr->getLHS(), tablesWithAliases);
        ExprTreePtr newRHS = updateAttributesWithAlias(expr->getRHS(), tablesWithAliases);
        return make_shared<EqOp>(newLHS, newRHS);
    } 
	else if (dynamic_cast<GtOp*>(expr.get()) != nullptr) {
        ExprTreePtr newLHS = updateAttributesWithAlias(expr->getLHS(), tablesWithAliases);
        ExprTreePtr newRHS = updateAttributesWithAlias(expr->getRHS(), tablesWithAliases);
        return make_shared<GtOp>(newLHS, newRHS);
    } 
	else if (dynamic_cast<LtOp*>(expr.get()) != nullptr) {
        ExprTreePtr newLHS = updateAttributesWithAlias(expr->getLHS(), tablesWithAliases);
        ExprTreePtr newRHS = updateAttributesWithAlias(expr->getRHS(), tablesWithAliases);
        return make_shared<LtOp>(newLHS, newRHS);
    } 
	else if (dynamic_cast<NeqOp*>(expr.get()) != nullptr) {
        ExprTreePtr newLHS = updateAttributesWithAlias(expr->getLHS(), tablesWithAliases);
        ExprTreePtr newRHS = updateAttributesWithAlias(expr->getRHS(), tablesWithAliases);
        return make_shared<NeqOp>(newLHS, newRHS);
    } 
	else if (dynamic_cast<OrOp*>(expr.get()) != nullptr) {
        ExprTreePtr newLHS = updateAttributesWithAlias(expr->getLHS(), tablesWithAliases);
        ExprTreePtr newRHS = updateAttributesWithAlias(expr->getRHS(), tablesWithAliases);
        return make_shared<OrOp>(newLHS, newRHS);
    } 
	else if (dynamic_cast<PlusOp*>(expr.get()) != nullptr) {
        ExprTreePtr newLHS = updateAttributesWithAlias(expr->getLHS(), tablesWithAliases);
        ExprTreePtr newRHS = updateAttributesWithAlias(expr->getRHS(), tablesWithAliases);
        return make_shared<PlusOp>(newLHS, newRHS);
    } 
	else if (dynamic_cast<MinusOp*>(expr.get()) != nullptr) {
        ExprTreePtr newLHS = updateAttributesWithAlias(expr->getLHS(), tablesWithAliases);
        ExprTreePtr newRHS = updateAttributesWithAlias(expr->getRHS(), tablesWithAliases);
        return make_shared<MinusOp>(newLHS, newRHS);
    } 
	else if (dynamic_cast<TimesOp*>(expr.get()) != nullptr) {
        ExprTreePtr newLHS = updateAttributesWithAlias(expr->getLHS(), tablesWithAliases);
        ExprTreePtr newRHS = updateAttributesWithAlias(expr->getRHS(), tablesWithAliases);
        return make_shared<TimesOp>(newLHS, newRHS);
    } 
	else if (dynamic_cast<DivideOp*>(expr.get()) != nullptr) {
        ExprTreePtr newLHS = updateAttributesWithAlias(expr->getLHS(), tablesWithAliases);
        ExprTreePtr newRHS = updateAttributesWithAlias(expr->getRHS(), tablesWithAliases);
        return make_shared<DivideOp>(newLHS, newRHS);
    } 
	else if (dynamic_cast<NotOp*>(expr.get()) != nullptr) {
        ExprTreePtr newChild = updateAttributesWithAlias(expr->getChild(), tablesWithAliases);
        return make_shared<NotOp>(newChild);
    } 
	else if (dynamic_cast<SumOp*>(expr.get()) != nullptr) {
        ExprTreePtr newChild = updateAttributesWithAlias(expr->getChild(), tablesWithAliases);
        return make_shared<SumOp>(newChild);
    } 
	else if (dynamic_cast<AvgOp*>(expr.get()) != nullptr) {
        ExprTreePtr newChild = updateAttributesWithAlias(expr->getChild(), tablesWithAliases);
        return make_shared<AvgOp>(newChild);
    } 
	else {
        return expr;
    }
}


// Helper function to check if an expression only references a single table
bool referencesOnlyTables(ExprTreePtr expr, const std::set<std::string> &aliases) {
    if (!expr) return true;

    if (expr->isId()) {
        std::string id = expr->getId();
        size_t pos = id.find('_');
        if (pos != std::string::npos) {
            std::string alias = id.substr(0, pos);
            return aliases.count(alias) > 0;
        }
        return false;
    }

    bool res = true;
    if (expr->getLHS()) res &= referencesOnlyTables(expr->getLHS(), aliases);
    if (expr->getRHS()) res &= referencesOnlyTables(expr->getRHS(), aliases);
    if (expr->getChild()) res &= referencesOnlyTables(expr->getChild(), aliases);
    return res;
}

// Check if expression is a join predicate (references tables from both sides)
bool isJoinPredicate(ExprTreePtr expr, const std::set<std::string> &aliasesLeft, const std::set<std::string> &aliasesRight) {
    if (!expr) return false;

    std::set<std::string> referencedAliases;

    // Helper function to collect all aliases in the expression
    std::function<void(ExprTreePtr)> collectAliases = [&](ExprTreePtr e) {
        if (!e) return;
        if (e->isId()) {
            std::string id = e->getId();
            size_t pos = id.find('_');
            if (pos != std::string::npos) {
                std::string alias = id.substr(0, pos);
                referencedAliases.insert(alias);
            }
        }
        if (e->getLHS()) collectAliases(e->getLHS());
        if (e->getRHS()) collectAliases(e->getRHS());
        if (e->getChild()) collectAliases(e->getChild());
    };

    collectAliases(expr);

    bool left = false, right = false;
    for (const auto &alias : referencedAliases) {
        if (aliasesLeft.count(alias)) left = true;
        if (aliasesRight.count(alias)) right = true;
    }

    return left && right;
}

// Define the memoization map type
typedef std::map<std::set<std::string>, std::pair<LogicalOpPtr, double>> MemoizationMap;
	
// builds and optimizes a logical query plan for a SFW query, returning the logical query plan
pair<LogicalOpPtr, double> SFWQuery::optimizeQueryPlan(map<string, MyDB_TablePtr>& allTables) {
    /*DEBUG_MSG("Starting optimizeQueryPlan");

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
    }*/

    MyDB_SchemaPtr totSchema = make_shared<MyDB_Schema>();
    map<string, MyDB_TablePtr> tablesWithAliases;

    for (auto& tableAliasPair : tablesToProcess) {
        string tableName = tableAliasPair.first;
        string alias = tableAliasPair.second;

        MyDB_TablePtr aliasedTable = allTables[tableName]->alias(alias);
        tablesWithAliases[alias] = aliasedTable;

        for (auto& att : aliasedTable->getSchema()->getAtts()) {
            totSchema->appendAtt(att);
        }
    }

    vector<ExprTreePtr> allDisjunctions = this->allDisjunctions;

    // Update the attribute names in all predicates, adding alias prefixes
    for (auto &expr : allDisjunctions) {
        //cerr << "Original disjunction: " << expr->toString() << endl;
        expr = updateAttributesWithAlias(expr, tablesWithAliases);
        //cerr << "Updated disjunction: " << expr->toString() << endl;
    }

    // Initialize the memoization map
    MemoizationMap memo;

    int allTablesNum = tablesToProcess.size();

    std::function<pair<LogicalOpPtr, double>(map<string, MyDB_TablePtr>&, MyDB_SchemaPtr&, vector<ExprTreePtr>&)> recursiveOptimize;

    recursiveOptimize = [&](map<string, MyDB_TablePtr>& currentTables, MyDB_SchemaPtr& totSchema, vector<ExprTreePtr>& currentDisjunctions) -> pair<LogicalOpPtr, double> {

        std::set<std::string> tableAliases;
        for (const auto &entry : currentTables) {
            tableAliases.insert(entry.first);
        }

        // Check if we have already computed the best plan for this set of tables
        if (memo.count(tableAliases)) {
            return memo[tableAliases];
        }

        LogicalOpPtr res = nullptr;
        double cost = 9e99;

        if (currentTables.size() == 1) {
            // Base case 
            auto tableEntry = *currentTables.begin();
            std::string alias = tableEntry.first;
            MyDB_TablePtr table = tableEntry.second;

            std::vector<ExprTreePtr> predicatesForThisTable;

            // Filter predicates relevant to this table
            std::set<std::string> aliases = { alias };
            MyDB_SchemaPtr outputScheme = make_shared<MyDB_Schema>();
            for (auto &expr : currentDisjunctions) {
                if (referencesOnlyTables(expr, aliases)) {
                    predicatesForThisTable.push_back(expr);
                }
                else {
                    for (const auto& inputAtt : table->getSchema()->getAtts())
                    {
                        /*cout << "For shchema: " << endl;
                        cout << "expr:" << expr->toString() << endl;
                        cout << "inputAtt.first: " << inputAtt.first << endl;*/
                        size_t pos = inputAtt.first.find('_');
                        string result = inputAtt.first.substr(pos + 1);
                        if (expr->referencesAtt(alias, result))
                        {
                            if (std::find(outputScheme->getAtts().begin(),
                                outputScheme->getAtts().end(),
                                std::make_pair(inputAtt.first, inputAtt.second))
                                == outputScheme->getAtts().end()) {
                                outputScheme->getAtts().emplace_back(inputAtt.first, inputAtt.second);
                            }
                        }
                    }
                }
            }
            for (auto& expr : groupingClauses) {
                for (const auto& inputAtt : table->getSchema()->getAtts())
                {
                    /*cout << "For shchema: " << endl;
                    cout << "expr:" << expr->toString() << endl;
                    cout << "inputAtt.first: " << inputAtt.first << endl;*/
                    size_t pos = inputAtt.first.find('_');
                    string result = inputAtt.first.substr(pos + 1);
                    if (expr->referencesAtt(alias, result))
                    {
                        if (std::find(outputScheme->getAtts().begin(),
                            outputScheme->getAtts().end(),
                            std::make_pair(inputAtt.first, inputAtt.second))
                            == outputScheme->getAtts().end()) {
                            outputScheme->getAtts().emplace_back(inputAtt.first, inputAtt.second);
                        }
                    }
                }
            }

            for (auto& expr : valuesToSelect)
            {
                //if (expr->hasAgg()) {
                    for (const auto& inputAtt : table->getSchema()->getAtts())
                    {
                        /*cout << "For agg shchema: " << endl;
                        cout << "expr:" << expr->toString() << endl;
                        cout << "inputAtt.first: " << inputAtt.first << endl;*/
                        size_t pos = inputAtt.first.find('_');
                        string result = inputAtt.first.substr(pos + 1);
                        if (expr->referencesAtt(alias, result))
                        {
                            if (std::find(outputScheme->getAtts().begin(),
                                outputScheme->getAtts().end(),
                                std::make_pair(inputAtt.first, inputAtt.second))
                                == outputScheme->getAtts().end()) {
                                outputScheme->getAtts().emplace_back(inputAtt.first, inputAtt.second);
                            }
                        }
                    }
                //}
            }

            MyDB_StatsPtr stats = make_shared<MyDB_Stats>(table);
            stats = stats->costSelection(predicatesForThisTable);

            std::string tempTableName = "tempTable" + std::to_string(tempTableId++);
            std::string fileName = "tempTableLoc" + std::to_string(tempTableId - 1) + ".bin";
            MyDB_TablePtr selectTable = std::make_shared<MyDB_Table>(tempTableName, fileName, outputScheme);

            res = make_shared<LogicalTableScan>(table, selectTable, stats, predicatesForThisTable);
            cost = stats->getTupleCount();

            // Store in memoization map
            memo[tableAliases] = make_pair(res, cost);
            return memo[tableAliases];
        }

        // We have at least one join
        vector<string> tableNames;
        for (const auto &entry : currentTables) {
            tableNames.push_back(entry.first);
        }

        for (size_t i = 1; i < (1 << tableNames.size()) - 1; ++i) {
            map<string, MyDB_TablePtr> leftTables, rightTables;

            for (size_t j = 0; j < tableNames.size(); ++j) {
                if (i & (1 << j)) {
                    leftTables[tableNames[j]] = currentTables[tableNames[j]];
                } else {
                    rightTables[tableNames[j]] = currentTables[tableNames[j]];
                }
            }

            //MyDB_SchemaPtr joinSchema = make_shared<MyDB_Schema>();
            MyDB_SchemaPtr joinSchema = totSchema;
            if (leftTables.size() + rightTables.size() == allTablesNum) {
                joinSchema = make_shared<MyDB_Schema>();
            }
            MyDB_SchemaPtr totSchema = make_shared<MyDB_Schema>();

            // Extract aliases
            std::set<std::string> leftAliases, rightAliases;
            for (const auto &entry : leftTables) leftAliases.insert(entry.first);
            for (const auto &entry : rightTables) rightAliases.insert(entry.first);

            std::vector<ExprTreePtr> leftPredicates, rightPredicates, joinPredicates, remainingPredicates;

            for (auto &expr : currentDisjunctions) {
                //cout << "lrj expr: " << expr->toString() << " " << referencesOnlyTables(expr, leftAliases) << referencesOnlyTables(expr, rightAliases) << isJoinPredicate(expr, leftAliases, rightAliases) << endl;
                if (referencesOnlyTables(expr, leftAliases)) {
                    leftPredicates.push_back(expr);
                } else if (referencesOnlyTables(expr, rightAliases)) {
                    rightPredicates.push_back(expr);
                } else if (isJoinPredicate(expr, leftAliases, rightAliases)) {
                    leftPredicates.push_back(expr);
                    rightPredicates.push_back(expr);
                    joinPredicates.push_back(expr);
                } else {
                    leftPredicates.push_back(expr);
                    rightPredicates.push_back(expr);
                    remainingPredicates.push_back(expr);
                }
            }

            for (auto leftTableName : leftTables)
            {
                for (auto att : leftTableName.second->getSchema()->getAtts())
                {
                    size_t pos = att.first.find('_');
                    string result = att.first.substr(pos + 1);
                    for (const auto& exp : valuesToSelect) {
                        if (exp->referencesAtt(leftTableName.first, result)) {
                            /*for (const auto& exp : currentDisjunctions) {
                                if (exp->referencesAtt(leftTableName.first, result)) {*/
                                    if (std::find(joinSchema->getAtts().begin(),
                                        joinSchema->getAtts().end(),
                                        std::make_pair(att.first, att.second))
                                        == joinSchema->getAtts().end()) {
                                        joinSchema->getAtts().emplace_back(att.first, att.second);
                                    }
                                    /*break;
                                }
                            }*/
                        }
                    }
                    if (leftTables.size() + rightTables.size() != allTablesNum) {
                        for (const auto& exp : joinPredicates) {
                            if (exp->referencesAtt(leftTableName.first, result)) {
                                if (std::find(joinSchema->getAtts().begin(),
                                    joinSchema->getAtts().end(),
                                    std::make_pair(att.first, att.second))
                                    == joinSchema->getAtts().end()) {
                                    totSchema->getAtts().emplace_back(att.first, att.second);
                                }
                            }
                        }
                    }
                }
            }

            for (auto rightTableName : rightTables)
            {
                for (auto att : rightTableName.second->getSchema()->getAtts())
                {
                    size_t pos = att.first.find('_');
                    string result = att.first.substr(pos + 1);
                    for (const auto& exp : valuesToSelect) {
                        //cout << "valuesToSelect expr: " << exp->toString() << endl;
                        if (exp->referencesAtt(rightTableName.first, result)) {
                            //for (const auto& exp : currentDisjunctions) {
                            //    //cout << "currentDisjunctions expr: " << exp->toString() << endl;
                            //    if (exp->referencesAtt(rightTableName.first, result)) {
                                    if (std::find(joinSchema->getAtts().begin(),
                                        joinSchema->getAtts().end(),
                                        std::make_pair(att.first, att.second))
                                        == joinSchema->getAtts().end()) {
                                        joinSchema->getAtts().emplace_back(att.first, att.second);
                                    }
                                    //break;
                                /*}
                            }*/
                        }
                    }

                }
            }

            // Recursively optimize left and right plans with their respective predicates
            auto leftPlan = recursiveOptimize(leftTables, totSchema, leftPredicates);
            auto rightPlan = recursiveOptimize(rightTables, totSchema, rightPredicates);

            // Compute join statistics using joinPredicates
            MyDB_StatsPtr joinStats = leftPlan.first->getStats()->costJoin(joinPredicates, rightPlan.first->getStats());

            if (!joinStats) {
                continue; 
            }

            double totalCost = leftPlan.second + rightPlan.second + joinStats->getTupleCount();

            // to change
            std::string tempTableName = "tempTable" + std::to_string(tempTableId++);
            std::string fileName = "tempTableLoc" + std::to_string(tempTableId - 1) + ".bin";
            MyDB_TablePtr joinTable = make_shared<MyDB_Table>(tempTableName, fileName, joinSchema);
            
            //cerr << "Debug: joinTable name: " << joinTable->getName() << endl;

            if (totalCost < cost) {
                res = make_shared<LogicalJoin>(leftPlan.first, rightPlan.first, joinTable, joinPredicates, joinStats);
                cost = totalCost;
            }
        }

        // Store the best plan in the memoization map
        memo[tableAliases] = make_pair(res, cost);
        return memo[tableAliases];
    };

    return recursiveOptimize(tablesWithAliases, totSchema, allDisjunctions);
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
        std::string alias = tableEntry.first;
        MyDB_TablePtr table = tableEntry.second;

        cerr << "Debug: Applying selection predicates to stats:" << endl;
        for (auto &pred : allDisjunctions) {
            cerr << "Predicate: " << pred->toString() << endl;
        }

        std::vector<ExprTreePtr> predicatesForThisTable;

        // Filter predicates relevant to this table
        std::set<std::string> aliases = { alias };
        for (auto &expr : allDisjunctions) {
            if (referencesOnlyTables(expr, aliases)) {
                predicatesForThisTable.push_back(expr);
            }
        }

        MyDB_StatsPtr stats = make_shared<MyDB_Stats>(table);
        stats = stats->costSelection(predicatesForThisTable);

        if (!stats) {
            cerr << "Error: costSelection returned null stats for table " << table->getName() << endl;
        } else {
            cerr << "Debug: costSelection returned stats with tuple count: " << stats->getTupleCount() << endl;
        }

        res = make_shared<LogicalTableScan>(table, table, stats, predicatesForThisTable);
        cost = stats->getTupleCount();
        return make_pair(res, cost);
	}

	// we have at least one join
	// some code here...
	// return make_pair (res, best);

    // 多表情况，初始化最优计划和成本
    cerr << "Debug: allTables contains the following keys:" << endl;
	for (const auto &entry : allTables) {
		cerr << "Key: " << entry.first << endl;
	}

	cerr << "Debug: Generating tableNames for allTables." << endl;
	vector<string> tableNames;
	for (const auto &entry : allTables) {
		tableNames.push_back(entry.first);
	}

	cerr << "Debug: tableNames size: " << tableNames.size() << endl;
	for (const auto &name : tableNames) {
		cerr << "TableName: " << name << endl;
	}

	// 遍历所有可能的左右表分组
	for (size_t i = 1; i < (1 << tableNames.size()) - 1; ++i) {
		map<string, MyDB_TablePtr> leftTables, rightTables;

		for (size_t j = 0; j < tableNames.size(); ++j) {
			if (i & (1 << j)) {
				leftTables[tableNames[j]] = allTables[tableNames[j]];
			} else {
				rightTables[tableNames[j]] = allTables[tableNames[j]];
			}
		}

		cerr << "Debug: Left Tables for current split:" << endl;
		for (const auto& tablePair : leftTables) {
			cerr << "Alias: " << tablePair.first << ", Table Name: " << tablePair.second->getName() << endl;
		}

		cerr << "Debug: Right Tables for current split:" << endl;
		for (const auto& tablePair : rightTables) {
			cerr << "Alias: " << tablePair.first << ", Table Name: " << tablePair.second->getName() << endl;
		}

		// Extract aliases
        std::set<std::string> leftAliases, rightAliases;
        for (const auto &entry : leftTables) leftAliases.insert(entry.first);
        for (const auto &entry : rightTables) rightAliases.insert(entry.first);

        // Partition predicates
        std::vector<ExprTreePtr> leftPredicates, rightPredicates, joinPredicates, remainingPredicates;

        for (auto &expr : allDisjunctions) {
            cout << "lrj expr: " << expr->toString() << " " << referencesOnlyTables(expr, leftAliases) << referencesOnlyTables(expr, rightAliases) << isJoinPredicate(expr, leftAliases, rightAliases) << endl;
            if (referencesOnlyTables(expr, leftAliases)) {
                leftPredicates.push_back(expr);
            } else if (referencesOnlyTables(expr, rightAliases)) {
                rightPredicates.push_back(expr);
            } else if (isJoinPredicate(expr, leftAliases, rightAliases)) {
                leftPredicates.push_back(expr);
                rightPredicates.push_back(expr);
                joinPredicates.push_back(expr);
            } else {
                remainingPredicates.push_back(expr);
            }
        }

        // Recursively optimize left and right plans with their respective predicates
        auto leftPlan = optimizeQueryPlan(leftTables, totSchema, leftPredicates);
        auto rightPlan = optimizeQueryPlan(rightTables, totSchema, rightPredicates);

        // Compute join statistics using joinPredicates
        MyDB_StatsPtr joinStats = leftPlan.first->getStats()->costJoin(joinPredicates, rightPlan.first->getStats());


		if (!joinStats) {
			cerr << "Error: costJoin returned nullptr for join between stats with tuple counts: " 
				 << leftPlan.first->getStats()->getTupleCount() << " and " 
				 << rightPlan.first->getStats()->getTupleCount() << endl;
		} else {
			cerr << "Debug: Join stats tuple count: " << joinStats->getTupleCount() << endl;
		}

        string tempTableName = "tempTable" + to_string(tempTableId);
        tempTableId++;
		MyDB_TablePtr joinTable = make_shared<MyDB_Table>(tempTableName, tempTableName+".bin");
        //cerr << "Debug: joinTable name: " << joinTable->getName() << endl;

		LogicalOpPtr joinOp = make_shared<LogicalJoin>(leftPlan.first, rightPlan.first, joinTable, allDisjunctions, joinStats);

		double totalCost = leftPlan.second + rightPlan.second + joinStats->getTupleCount();

		cerr << "Debug: TotalCost for current join: " << totalCost << endl;
		if (totalCost < cost) {
			cerr << "Debug: Found better plan with cost: " << totalCost << endl;
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
