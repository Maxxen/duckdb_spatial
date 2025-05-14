#pragma once

#include "duckdb.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"

#include <duckdb/catalog/default/default_functions.hpp>

namespace duckdb {

//------------------------------------------------------------------------------
// Function Builder
//------------------------------------------------------------------------------

class ScalarFunctionBuilder;
class AggregateFunctionBuilder;
class MacroFunctionBuilder;

class FunctionBuilder {
public:
	template <class CALLBACK>
	static void RegisterScalar(DatabaseInstance &db, const char *name, CALLBACK &&callback);

	template <class CALLBACK>
	static void RegisterAggregate(DatabaseInstance &db, const char *name, CALLBACK &&callback);

	template <class CALLBACK>
	static void RegisterMacro(DatabaseInstance &db, const char *name, CALLBACK &&callback);

	// TODO:
	static void AddTableFunctionDocs(DatabaseInstance &db, const char *name, const char *desc, const char *example,
	                                 const InsertionOrderPreservingMap<string> &tags);

	static string RemoveIndentAndTrailingWhitespace(const char *str);

private:
	static void Register(DatabaseInstance &db, const char *name, ScalarFunctionBuilder &builder);
	static void Register(DatabaseInstance &db, const char *name, AggregateFunctionBuilder &builder);
	static void Register(DatabaseInstance &db, const char *name, MacroFunctionBuilder &builder);
};

//------------------------------------------------------------------------------
// Scalar Function Variant Builder
//------------------------------------------------------------------------------

class ScalarFunctionVariantBuilder {
	friend class ScalarFunctionBuilder;

public:
	void AddParameter(const char *name, const LogicalType &type);
	void SetReturnType(LogicalType type);
	void SetFunction(scalar_function_t fn);
	void SetInit(init_local_state_t init);
	void SetBind(bind_scalar_function_t bind);
	void SetDescription(const string &desc);
	void SetExample(const string &ex);

private:
	explicit ScalarFunctionVariantBuilder() : function({}, LogicalTypeId::INVALID, nullptr) {
	}

	ScalarFunction function;
	FunctionDescription description = {};
};

inline void ScalarFunctionVariantBuilder::AddParameter(const char *name, const LogicalType &type) {
	function.arguments.emplace_back(type);
	description.parameter_names.emplace_back(name);
	description.parameter_types.emplace_back(type);
}

inline void ScalarFunctionVariantBuilder::SetReturnType(LogicalType type) {
	function.return_type = std::move(type);
}

inline void ScalarFunctionVariantBuilder::SetFunction(scalar_function_t fn) {
	function.function = fn;
}

inline void ScalarFunctionVariantBuilder::SetInit(init_local_state_t init) {
	function.init_local_state = init;
}

inline void ScalarFunctionVariantBuilder::SetBind(bind_scalar_function_t bind) {
	function.bind = bind;
}

inline void ScalarFunctionVariantBuilder::SetDescription(const string &desc) {
	description.description = desc;
}

inline void ScalarFunctionVariantBuilder::SetExample(const string &ex) {
	description.examples.emplace_back(ex);
}

//------------------------------------------------------------------------------
// Scalar Function Builder
//------------------------------------------------------------------------------

class ScalarFunctionBuilder {
	friend class FunctionBuilder;

public:
	template <class CALLBACK>
	void AddVariant(CALLBACK &&callback);
	void SetTag(const string &key, const string &value);
	void SetDescription(const string &desc);
	void SetExample(const string &ex);

private:
	explicit ScalarFunctionBuilder(const char *name) : set(name) {
	}

	ScalarFunctionSet set;
	vector<FunctionDescription> descriptions = {};
	InsertionOrderPreservingMap<string> tags = {};

	// If not set by a variant
	string default_description;
	string default_example;
};

inline void ScalarFunctionBuilder::SetDescription(const string &desc) {
	default_description = FunctionBuilder::RemoveIndentAndTrailingWhitespace(desc.c_str());
}

inline void ScalarFunctionBuilder::SetExample(const string &ex) {
	default_example = FunctionBuilder::RemoveIndentAndTrailingWhitespace(ex.c_str());
}

inline void ScalarFunctionBuilder::SetTag(const string &key, const string &value) {
	tags[key] = value;
}

template <class CALLBACK>
void ScalarFunctionBuilder::AddVariant(CALLBACK &&callback) {
	ScalarFunctionVariantBuilder builder;

	callback(builder);

	// A return type is required
	if (builder.function.return_type.id() == LogicalTypeId::INVALID) {
		throw InternalException("Return type not set in ScalarFunctionBuilder::AddVariant");
	}

	// Add the new variant to the set
	set.AddFunction(std::move(builder.function));

	// Add the description
	descriptions.emplace_back(std::move(builder.description));
}

//------------------------------------------------------------------------------
// Macro
//------------------------------------------------------------------------------
class MacroFunctionBuilder {
	friend class FunctionBuilder;

public:
	void AddDefinition(const vector<string> &parameters, const string &body, const char *desc = nullptr,
	                   const char *example = nullptr) {
		macros.push_back({parameters, body, desc, example});
	}

private:
	struct MacroDef {
		vector<string> parameters;
		string body;
		const char *description;
		const char *example;
	};

	vector<MacroDef> macros;
};

//------------------------------------------------------------------------------
// Aggregate
//------------------------------------------------------------------------------

class AggregateFunctionBuilder {
	friend class FunctionBuilder;

public:
	void SetTag(const string &key, const string &value);
	void SetDescription(const string &desc);
	void SetExample(const string &ex);
	void SetFunction(const AggregateFunction &function);

private:
	explicit AggregateFunctionBuilder(const char *name) : set(name) {
	}
	string description;
	string example;
	InsertionOrderPreservingMap<string> tags;
	AggregateFunctionSet set;
};

inline void AggregateFunctionBuilder::SetFunction(const AggregateFunction &function) {
	set.AddFunction(function);
}

inline void AggregateFunctionBuilder::SetDescription(const string &desc) {
	description = desc;
}
inline void AggregateFunctionBuilder::SetExample(const string &ex) {
	example = ex;
}
inline void AggregateFunctionBuilder::SetTag(const string &key, const string &value) {
	tags[key] = value;
}

//------------------------------------------------------------------------------
// Function Builder Methods
//------------------------------------------------------------------------------

template <class CALLBACK>
void FunctionBuilder::RegisterScalar(DatabaseInstance &db, const char *name, CALLBACK &&callback) {
	ScalarFunctionBuilder builder(name);
	callback(builder);

	Register(db, name, builder);
}

template <class CALLBACK>
void FunctionBuilder::RegisterAggregate(DatabaseInstance &db, const char *name, CALLBACK &&callback) {
	AggregateFunctionBuilder builder(name);
	callback(builder);
	Register(db, name, builder);
}

template <class CALLBACK>
void FunctionBuilder::RegisterMacro(DatabaseInstance &db, const char *name, CALLBACK &&callback) {
	MacroFunctionBuilder builder;
	callback(builder);
	Register(db, name, builder);
}

} // namespace duckdb