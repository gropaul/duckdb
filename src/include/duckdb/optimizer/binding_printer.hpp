//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/binding_printer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_recursive_cte.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_explain.hpp"

namespace duckdb {
class LogicalOperator;
class Optimizer;

static string ListToString(const vector<idx_t> &list) {
	string result = "[";
	for (size_t i = 0; i < list.size(); ++i) {
		if (i > 0) {
			result += ",";
		}
		result += to_string(list[i]);
	}
	return result + "]";
}

static string EscapeForJson(const string &value) {
	string escaped = StringUtil::Replace(value, "\"", "\\\"");
	escaped = StringUtil::Replace(escaped, "\n", "\\n");
	escaped = StringUtil::Replace(escaped, "\r", "\\r");
	return escaped;
}

struct ExpressionInfo {
	string expression;
	string expression_type;
	string expression_class;
	string return_type;

	int table_index = -1;
	int column_index = -1;

	vector<ExpressionInfo> children;

	string ToJson() const {
		string json = "{\"expression\":\"" + EscapeForJson(expression) + "\"";
		json += ",\"expression_type\":\"" + EscapeForJson(expression_type) + "\"";
		json += ",\"expression_class\":\"" + EscapeForJson(expression_class) + "\"";
		json += ",\"return_type\":\"" + EscapeForJson(return_type) + "\"";
		if (table_index != -1) {
			json += ",\"table_index\":" + to_string(table_index);
		}
		if (column_index != -1) {
			json += ",\"column_index\":" + to_string(column_index);
		}
		if (!children.empty()) {
			json += ",\"children\":[";
			for (size_t i = 0; i < children.size(); ++i) {
				if (i > 0)
					json += ",";
				json += children[i].ToJson();
			}
			json += "]";
		}
		json += "}";
		return json;
	}
};

class ExpressionExtractor : LogicalOperatorVisitor {

	ExpressionInfo &info;

public:
	void VisitExpression(unique_ptr<Expression> *expression) override {
		this->info.expression = expression->get()->ToString();
		this->info.expression_type = ExpressionTypeToString(expression->get()->type);
		this->info.expression_class = ExpressionClassToString(expression->get()->expression_class);
		this->info.return_type = expression->get()->return_type.ToString();

		// if expression is BoundColumnRefExpression, get the binding information
		if (expression->get()->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
			auto &bound_ref = expression->get()->Cast<BoundColumnRefExpression>();
			this->info.table_index = static_cast<int>(bound_ref.binding.table_index);
			this->info.column_index = static_cast<int>(bound_ref.binding.column_index);
		} else if (expression->get()->GetExpressionType() == ExpressionType::BOUND_REF) {
			auto &bound_ref = expression->get()->Cast<BoundReferenceExpression>();
			this->info.table_index = static_cast<int>(-1);
			this->info.column_index = static_cast<int>(bound_ref.index);
		} else {
			this->info.table_index = -1;
			this->info.column_index = -1;
		}

		ExpressionIterator::EnumerateChildren(*expression->get(), [&](unique_ptr<Expression> &child) {
			this->info.children.emplace_back();
			auto &child_info = this->info.children.back();
			ExpressionExtractor child_extractor(child_info);
			child_extractor.VisitExpression(&child);
		});
	}

	explicit ExpressionExtractor(ExpressionInfo &info) : info(info) {
	}
};

struct JoinConditionInfo {
	ExpressionInfo left;
	ExpressionInfo right;
	ExpressionType comparison;

	explicit JoinConditionInfo(JoinCondition &condition) {
		ExpressionExtractor left_extractor(left);
		left_extractor.VisitExpression(&condition.left);
		ExpressionExtractor right_extractor(right);
		right_extractor.VisitExpression(&condition.right);
		comparison = condition.comparison;
	}

	string ToJson() const {
		string json = "{";
		json += "\"left\":" + left.ToJson() + ",";
		json += "\"right\":" + right.ToJson() + ",";
		json += "\"comparison\":\"" + ExpressionTypeToString(comparison) + "\"";
		json += "}";
		return json;
	}
};

struct ExtractedInfo {
	string operator_type;
	vector<idx_t> table_index;

	InsertionOrderPreservingMap<string> extra_info; // key-value pairs for extra information
	vector<ExpressionInfo> expressions;
	std::unordered_map<string, vector<ExpressionInfo>> other_expressions;
	vector<JoinConditionInfo> join_conditions;
	vector<ExtractedInfo> childrenInfo;

	string ToJson() const {
		string json = "{";

		json += "\"operator_type\":\"" + operator_type + "\",";
		json += "\"table_index\":[";
		for (size_t i = 0; i < table_index.size(); ++i) {
			if (i > 0)
				json += ",";
			json += to_string(table_index[i]);
		}
		json += "],";

		json += "\"extra_info\":{";
		for (auto it = extra_info.begin(); it != extra_info.end(); ++it) {
			if (it != extra_info.begin()) {
				json += ",";
			}
			json += "\"" + it->first + "\":\"" + EscapeForJson(it->second) + "\"";
		}
		json += "},";

		json += "\"expressions\":[";
		for (size_t i = 0; i < expressions.size(); ++i) {
			if (i > 0)
				json += ",";
			json += expressions[i].ToJson();
		}
		json += "]";

		for (const auto &pair : other_expressions) {
			json += ",\"" + pair.first + "\":[";
			for (size_t i = 0; i < pair.second.size(); ++i) {
				if (i > 0)
					json += ",";
				json += pair.second[i].ToJson();
			}
			json += "]";
		}

		// if there are join conditions, add them
		if (!join_conditions.empty()) {
			json += ",\"join_conditions\":[";
			for (size_t i = 0; i < join_conditions.size(); ++i) {
				if (i > 0)
					json += ",";
				json += join_conditions[i].ToJson();
			}
			json += "]";
		}

		if (!childrenInfo.empty()) {
			json += ",\"children\":[";
			for (size_t i = 0; i < childrenInfo.size(); ++i) {
				if (i > 0)
					json += ",";
				json += childrenInfo[i].ToJson();
			}
			json += "]";
		}

		json += "}";

		return json;
	}

	void Print(idx_t indent = 0) const {
		string indent_str(indent, ' ');
		printf("%sOperator Type: %s", indent_str.c_str(), operator_type.c_str());

		if (!table_index.empty()) {
			printf("%sTable Index: ", indent_str.c_str());
			for (const auto &index : table_index) {
				printf("%s%llu ", indent_str.c_str(), index);
			}
		}
		printf("\n");
		for (const auto &child_info : childrenInfo) {
			child_info.Print(indent + 2);
		}
	}
};

class BindingExtractor : LogicalOperatorVisitor {

	ExtractedInfo &info;

public:
	explicit BindingExtractor(ExtractedInfo &info) : info(info) {
	}

	/*
	if (projection_ids.empty()) {
	    for (auto &index : column_ids) {
	        types.push_back(GetColumnType(index));
	    }
	} else {
	    for (auto &proj_index : projection_ids) {
	        auto &index = column_ids[proj_index];
	        types.push_back(GetColumnType(index));
	    }
	}
	*/
	void EnumerateOpExpressions(LogicalOperator &op) {
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_GET: {
			auto &get = op.Cast<LogicalGet>();
			auto scan_count = get.GetColumnIds().size();
			for (idx_t i = 0; i < scan_count; i++) {
				const auto idx = get.projection_ids.empty() ? i : get.projection_ids[i];
				const auto col_idx = get.GetColumnIds()[idx].GetPrimaryIndex();

				info.expressions.emplace_back();
				auto &expression_info = info.expressions.back();
				expression_info.return_type = get.types[col_idx].ToString();
				expression_info.expression = get.names[col_idx];
				expression_info.expression_type = ExpressionTypeToString(ExpressionType::BOUND_COLUMN_REF);
				expression_info.expression_class = ExpressionClassToString(ExpressionClass::BOUND_COLUMN_REF);
			}
			break;
		}
		case LogicalOperatorType::LOGICAL_ORDER_BY: {
			auto &order = op.Cast<LogicalOrder>();
			vector<ExpressionInfo> order_expressions;
			for (auto &node : order.orders) {
				order_expressions.emplace_back();
				auto &order_expression = order_expressions.back();
				ExpressionExtractor extractor(order_expression);
				extractor.VisitExpression(&node.expression);
			}

			info.other_expressions["orders"] = std::move(order_expressions);
			break;
		}
		case LogicalOperatorType::LOGICAL_TOP_N: {
			auto &order = op.Cast<LogicalTopN>();
			vector<ExpressionInfo> order_expressions;
			for (auto &node : order.orders) {
				order_expressions.emplace_back();
				auto &order_expression = order_expressions.back();
				ExpressionExtractor extractor(order_expression);
				extractor.VisitExpression(&node.expression);
			}
			info.other_expressions["orders"] = std::move(order_expressions);

			break;
		}
		case LogicalOperatorType::LOGICAL_DISTINCT: {
			auto &distinct = op.Cast<LogicalDistinct>();

			vector<ExpressionInfo> distinct_targets;
			for (auto &target : distinct.distinct_targets) {
				distinct_targets.emplace_back();
				auto &expression = distinct_targets.back();
				ExpressionExtractor extractor(expression);
				extractor.VisitExpression(&target);
			}
			info.other_expressions["distinct_targets"] = std::move(distinct_targets);

			if (distinct.order_by) {
				vector<ExpressionInfo> order_expressions;
				for (auto &node : distinct.order_by->orders) {
					order_expressions.emplace_back();
					auto &order_expression = order_expressions.back();
					ExpressionExtractor extractor(order_expression);
					extractor.VisitExpression(&node.expression);
				}
				info.other_expressions["orders"] = std::move(order_expressions);
			}
			break;
		}
		case LogicalOperatorType::LOGICAL_RECURSIVE_CTE: {
			auto &rec = op.Cast<LogicalRecursiveCTE>();

			vector<ExpressionInfo> key_targets;
			for (auto &target : rec.key_targets) {
				key_targets.emplace_back();
				auto &order_expression = key_targets.back();
				ExpressionExtractor extractor(order_expression);
				extractor.VisitExpression(&target);
			}
			info.other_expressions["key_targets"] = std::move(key_targets);
			break;
		}
		case LogicalOperatorType::LOGICAL_INSERT: {
			//
			break;
		}
		case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN: {
			auto &join = op.Cast<LogicalDependentJoin>();

			vector<ExpressionInfo> duplicate_eliminated_columns;
			for (auto &expr : join.duplicate_eliminated_columns) {
				duplicate_eliminated_columns.emplace_back();
				auto &order_expression = duplicate_eliminated_columns.back();
				ExpressionExtractor extractor(order_expression);
				extractor.VisitExpression(&expr);
			}
			info.other_expressions["duplicate_eliminated_columns"] = std::move(duplicate_eliminated_columns);

			for (auto &cond : join.conditions) {
				info.join_conditions.emplace_back(cond);
			}

			vector<ExpressionInfo> arbitrary_expressions;
			for (auto &expr : join.arbitrary_expressions) {
				arbitrary_expressions.emplace_back();
				auto &expression = arbitrary_expressions.back();
				ExpressionExtractor extractor(expression);
				extractor.VisitExpression(&expr);
			}
			info.other_expressions["arbitrary_expressions"] = std::move(arbitrary_expressions);

			vector<ExpressionInfo> expression_children;
			for (auto &expr : join.expression_children) {
				expression_children.emplace_back();
				auto &expression = expression_children.back();
				ExpressionExtractor extractor(expression);
				extractor.VisitExpression(&expr);
			}
			info.other_expressions["expression_children"] = std::move(expression_children);
			break;
		}
		case LogicalOperatorType::LOGICAL_ASOF_JOIN:
		case LogicalOperatorType::LOGICAL_DELIM_JOIN:
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
			auto &join = op.Cast<LogicalComparisonJoin>();

			vector<ExpressionInfo> duplicate_eliminated_columns;
			for (auto &expr : join.duplicate_eliminated_columns) {
				duplicate_eliminated_columns.emplace_back();
				auto &order_expression = duplicate_eliminated_columns.back();
				ExpressionExtractor extractor(order_expression);
				extractor.VisitExpression(&expr);
			}

			// add left and right projection maps
			this->info.extra_info["left_projection_map"] = ListToString(join.left_projection_map);
			this->info.extra_info["right_projection_map"] = ListToString(join.right_projection_map);

			info.other_expressions["duplicate_eliminated_columns"] = std::move(duplicate_eliminated_columns);

			for (auto &cond : join.conditions) {
				info.join_conditions.emplace_back(cond);
			}

			vector<ExpressionInfo> predicate;
			if (join.predicate) {
				predicate.emplace_back();
				auto &predicate_info = predicate.back();
				ExpressionExtractor extractor(predicate_info);
				extractor.VisitExpression(&join.predicate);
			}
			info.other_expressions["predicate"] = std::move(predicate);

			break;
		}
		case LogicalOperatorType::LOGICAL_ANY_JOIN: {
			auto &join = op.Cast<LogicalAnyJoin>();
			vector<ExpressionInfo> conditions;
			if (join.condition) {
				conditions.emplace_back();
				auto &condition_info = conditions.back();
				ExpressionExtractor extractor(condition_info);
				extractor.VisitExpression(&join.condition);
			}
			info.other_expressions["condition"] = std::move(conditions);
			break;
		}
		case LogicalOperatorType::LOGICAL_LIMIT: {
			auto &limit = op.Cast<LogicalLimit>();

			vector<ExpressionInfo> limits;
			if (limit.limit_val.GetExpression()) {
				limits.emplace_back();
				auto &condition_info = limits.back();
				ExpressionExtractor extractor(condition_info);
				extractor.VisitExpression(&limit.limit_val.GetExpression());
			}
			info.other_expressions["limit"] = std::move(limits);

			vector<ExpressionInfo> offset;
			if (limit.offset_val.GetExpression()) {
				offset.emplace_back();
				auto &condition_info = offset.back();
				ExpressionExtractor extractor(condition_info);
				extractor.VisitExpression(&limit.offset_val.GetExpression());
			}
			info.other_expressions["offset"] = std::move(offset);
			break;
		}
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
			auto &aggr = op.Cast<LogicalAggregate>();

			vector<ExpressionInfo> groups;
			for (auto &group : aggr.groups) {
				groups.emplace_back();
				auto &info = groups.back();
				ExpressionExtractor extractor(info);
				extractor.VisitExpression(&group);
			}
			info.other_expressions["groups"] = std::move(groups);

			break;
		}
		default:
			break;
		}
		for (auto &expression : op.expressions) {
			this->info.expressions.emplace_back();
			auto &expression_info = this->info.expressions.back();
			ExpressionExtractor extractor(expression_info);
			extractor.VisitExpression(&expression);
		}
	}

	void VisitOperator(LogicalOperator &op) override {
		this->info.operator_type = LogicalOperatorToString(op.type);
		this->info.table_index = op.GetTableIndex();
		this->info.extra_info = op.ParamsToString();

		VisitOperatorExpressions(op);

		for (auto &child : op.children) {
			this->info.childrenInfo.emplace_back();
			auto &child_info = this->info.childrenInfo.back();
			BindingExtractor child_extractor(child_info);
			child_extractor.VisitOperator(*child);
		}

		EnumerateOpExpressions(op);
	}
	ExtractedInfo GetExtractedInfo() {
		return info;
	}
};

class BindingPrinter : public LogicalOperatorVisitor {

	string plan;

	void VisitOperator(LogicalOperator &op) override {
		if (op.type == LogicalOperatorType::LOGICAL_EXPLAIN) {
			LogicalExplain &explain = op.Cast<LogicalExplain>();
			ExtractedInfo info;

			BindingExtractor extractor(info);
			extractor.VisitOperator(*op.children[0]);

			string json = "[" + info.ToJson() + "]";
			explain.logical_plan_opt_detailed = json;
			plan = json;
		}
	}

public:
	//! Optimize PROJECTION + LIMIT to LIMIT + Projection
	string Visit(unique_ptr<LogicalOperator> &op) {
		this->VisitOperator(*op);
		return plan;
	}

	string Visit(LogicalOperator &op) {
		this->VisitOperator(op);
		return plan;
	}
};

} // namespace duckdb
