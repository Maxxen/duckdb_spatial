#include "spatial/util/function_builder.hpp"

#include "duckdb/catalog/catalog_entry/function_entry.hpp"
#include "duckdb/main/extension_util.hpp"

#include <duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp>

namespace duckdb {

string FunctionBuilder::RemoveIndentAndTrailingWhitespace(const char *ptr) {
	string tmp;
	// Replace all tabs with 4 spaces in ptr
	for (const char *text = ptr; *text; text++) {
		if (*text == '\t') {
			tmp.append("    ");
		} else {
			tmp += *text;
		}
	}

	auto text = tmp.c_str();

	string result;
	// Skip any empty first newlines if present
	while (*text == '\n') {
		text++;
	}

	// Track indent length
	auto indent_start = text;
	while (isspace(*text) && *text != '\n') {
		text++;
	}
	auto indent_len = text - indent_start;
	while (*text) {
		result += *text;
		if (*text++ == '\n') {
			// Remove all indentation, but only if it matches the first line's indentation
			bool matched_indent = true;
			for (auto i = 0; i < indent_len; i++) {
				if (*text != indent_start[i]) {
					matched_indent = false;
					break;
				}
			}
			if (matched_indent) {
				auto remaining_indent = indent_len;
				while (*text && remaining_indent > 0) {
					text++;
					remaining_indent--;
				}
			}
		}
	}

	// Also remove any trailing whitespace
	result.erase(result.find_last_not_of(" \n\r\t") + 1);
	return result;
}

void FunctionBuilder::Register(DatabaseInstance &db, const char *name, ScalarFunctionBuilder &builder) {
	// Register the function
	ExtensionUtil::RegisterFunction(db, std::move(builder.set));

	// Also add the parameter names. We need to access the catalog entry for this.
	auto &catalog = Catalog::GetSystemCatalog(db);
	auto transaction = CatalogTransaction::GetSystemTransaction(db);
	auto &schema = catalog.GetSchema(transaction, DEFAULT_SCHEMA);
	auto catalog_entry = schema.GetEntry(transaction, CatalogType::SCALAR_FUNCTION_ENTRY, name);
	if (!catalog_entry) {
		// This should not happen, we just registered the function
		throw InternalException("Function with name \"%s\" not found in FunctionBuilder::AddScalar", name);
	}

	auto &func_entry = catalog_entry->Cast<FunctionEntry>();

	// Insert all descriptions
	for (auto &desc : builder.descriptions) {

		// Add default description if none is set
		if (desc.description.empty()) {
			desc.description = builder.default_description;
		} else {
			desc.description = RemoveIndentAndTrailingWhitespace(desc.description.c_str());
		}

		// Add default example if none is set
		if (desc.examples.empty()) {
			desc.examples.push_back(builder.default_example);
		} else {
			for (auto &ex : desc.examples) {
				ex = RemoveIndentAndTrailingWhitespace(ex.c_str());
			}
		}

		func_entry.descriptions.push_back(desc);
	}

	if (!builder.tags.empty()) {
		func_entry.tags = std::move(builder.tags);
	}
}

void FunctionBuilder::Register(DatabaseInstance &db, const char *name, AggregateFunctionBuilder &builder) {
	// Register the function
	ExtensionUtil::RegisterFunction(db, std::move(builder.set));

	// Also add the parameter names. We need to access the catalog entry for this.
	auto &catalog = Catalog::GetSystemCatalog(db);
	auto transaction = CatalogTransaction::GetSystemTransaction(db);
	auto &schema = catalog.GetSchema(transaction, DEFAULT_SCHEMA);
	auto catalog_entry = schema.GetEntry(transaction, CatalogType::AGGREGATE_FUNCTION_ENTRY, name);
	if (!catalog_entry) {
		// This should not happen, we just registered the function
		throw InternalException("Function with name \"%s\" not found in FunctionBuilder::AddAggregate", name);
	}

	auto &func_entry = catalog_entry->Cast<FunctionEntry>();

	// Insert all descriptions
	const auto descr = RemoveIndentAndTrailingWhitespace(builder.description.c_str());
	const auto exampl = RemoveIndentAndTrailingWhitespace(builder.example.c_str());
	FunctionDescription function_description;
	function_description.description = descr;
	function_description.examples.push_back(exampl);
	func_entry.descriptions.push_back(function_description);

	if (!builder.tags.empty()) {
		func_entry.tags = std::move(builder.tags);
	}
}

void FunctionBuilder::Register(DatabaseInstance &db, const char *name, MacroFunctionBuilder &builder) {
	// Register the function
	vector<DefaultMacro> macros;
	vector<FunctionDescription> descriptions;

	for (auto &def : builder.macros) {
		DefaultMacro macro = {};
		macro.schema = DEFAULT_SCHEMA;
		macro.name = name;
		macro.named_parameters[0].name = nullptr;
		macro.named_parameters[0].default_value = nullptr;
		macro.macro = def.body.c_str();
		for (idx_t i = 0; i < def.parameters.size(); i++) {
			if (i >= 8) {
				throw InternalException("Too many parameters in macro!");
			}
			macro.parameters[i] = def.parameters[i].c_str();
		}
		macro.parameters[def.parameters.size()] = nullptr;
		macros.push_back(macro);

		FunctionDescription function_description;
		if (def.description) {
			function_description.description = RemoveIndentAndTrailingWhitespace(def.description);
		}
		if (def.example) {
			function_description.examples.push_back(RemoveIndentAndTrailingWhitespace(def.example));
		}
		descriptions.push_back(function_description);
	}

	const auto macro_ptr = array_ptr<const DefaultMacro>(macros.data(), macros.size());
	const auto info = DefaultFunctionGenerator::CreateInternalMacroInfo(macro_ptr);
	info->descriptions = descriptions;

	ExtensionUtil::RegisterFunction(db, *info);
}

void FunctionBuilder::AddTableFunctionDocs(DatabaseInstance &db, const char *name, const char *desc,
                                           const char *example, const unordered_map<string, string> &tags) {

	auto &catalog = Catalog::GetSystemCatalog(db);
	auto transaction = CatalogTransaction::GetSystemTransaction(db);
	auto &schema = catalog.GetSchema(transaction, DEFAULT_SCHEMA);
	auto catalog_entry = schema.GetEntry(transaction, CatalogType::TABLE_FUNCTION_ENTRY, name);
	if (!catalog_entry) {
		// This should not happen, we just registered the function
		throw InternalException("Function with name \"%s\" not found in FunctionBuilder::AddScalar", name);
	}

	auto &func_entry = catalog_entry->Cast<FunctionEntry>();
	FunctionDescription function_description;
	function_description.description = RemoveIndentAndTrailingWhitespace(desc);
	function_description.examples.push_back(RemoveIndentAndTrailingWhitespace(example));
	func_entry.descriptions.push_back(function_description);
	func_entry.tags.insert(tags.begin(), tags.end());
}

} // namespace duckdb