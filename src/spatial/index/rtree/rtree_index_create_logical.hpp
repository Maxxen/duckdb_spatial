#pragma once
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

class LogicalCreateRTreeIndex final : public LogicalExtensionOperator {
public:
	// Info for index creation
	unique_ptr<CreateIndexInfo> info;

	//! The table to create the index for
	TableCatalogEntry &table;

	//! Unbound expressions to be used in the optimizer
	vector<unique_ptr<Expression>> unbound_expressions;

public:
	LogicalCreateRTreeIndex(unique_ptr<CreateIndexInfo> info_p, vector<unique_ptr<Expression>> expressions_p,
	                        TableCatalogEntry &table_p);
	void ResolveTypes() override;
	void ResolveColumnBindings(ColumnBindingResolver &res, vector<ColumnBinding> &bindings) override;

	// Actually create and plan the index creation
	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override;

	void Serialize(Serializer &writer) const override {
		LogicalExtensionOperator::Serialize(writer);
		writer.WritePropertyWithDefault(300, "operator_type", string("logical_rtree_create_index"));
		writer.WritePropertyWithDefault<unique_ptr<CreateIndexInfo>>(400, "info", info);
		writer.WritePropertyWithDefault<vector<unique_ptr<Expression>>>(401, "unbound_expressions",
		                                                                unbound_expressions);
	}

	string GetExtensionName() const override {
		return "duckdb_spatial";
	}
};

class LogicalCreateRTreeIndexOperatorExtension final : public OperatorExtension {
public:
	LogicalCreateRTreeIndexOperatorExtension() {
		Bind = [](ClientContext &, Binder &, OperatorExtensionInfo *, SQLStatement &) -> BoundStatement {
			// For some reason all operator extensions require this callback to be implemented
			// even though it is useless for us as we construct this operator through the optimizer instead.
			BoundStatement result;
			result.plan = nullptr;
			return result;
		};
	}

	std::string GetName() override {
		return "duckdb_spatial";
	}
	unique_ptr<LogicalExtensionOperator> Deserialize(Deserializer &reader) override {
		const auto operator_type = reader.ReadPropertyWithDefault<string>(300, "operator_type");
		// We only have one extension operator type right now
		if (operator_type != "logical_rtree_create_index") {
			throw SerializationException("This version of the spatial extension does not support operator type '%s!",
			                             operator_type);
		}
		auto create_info = reader.ReadPropertyWithDefault<unique_ptr<CreateInfo>>(400, "info");
		auto unbound_expressions =
		    reader.ReadPropertyWithDefault<vector<unique_ptr<Expression>>>(401, "unbound_expressions");

		auto info = unique_ptr_cast<CreateInfo, CreateIndexInfo>(std::move(create_info));

		// We also need to rebind the table
		auto &context = reader.Get<ClientContext &>();
		const auto &catalog = info->catalog;
		const auto &schema = info->schema;
		const auto &table_name = info->table;
		auto &table_entry = Catalog::GetEntry<TableCatalogEntry>(context, catalog, schema, table_name);

		// Return the new operator
		return make_uniq<LogicalCreateRTreeIndex>(std::move(info), std::move(unbound_expressions), table_entry);
	}
};

} // namespace duckdb