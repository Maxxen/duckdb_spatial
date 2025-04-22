#include "gpkg_module.hpp"

#include "duckdb/storage/storage_extension.hpp"

#include <duckdb/main/database.hpp>
#include <duckdb/parser/parsed_data/create_schema_info.hpp>

namespace duckdb {

namespace {

//======================================================================================================================
// GPKG Database
//======================================================================================================================
class GPKGDB {
public:
	void Execute(const string &query) { }
	idx_t RunPragma(const string &pragma) { }
	// TODO:
};

//======================================================================================================================
// GPKG Schema
//======================================================================================================================
class GPKGSchemaEntry final : public SchemaCatalogEntry {
public:
	GPKGSchemaEntry(Catalog &catalog, CreateSchemaInfo &info);
	~GPKGSchemaEntry() override = default;
public:
	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
										   TableCatalogEntry &table) override;
	optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;
	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;
	optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
												   CreateTableFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
												  CreateCopyFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
													CreatePragmaFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) override;
	optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;
	void Alter(CatalogTransaction transaction, AlterInfo &info) override;
	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void DropEntry(ClientContext &context, DropInfo &info) override;
	optional_ptr<CatalogEntry> GetEntry(CatalogTransaction transaction, CatalogType type, const string &name) override;
};

//======================================================================================================================
// GPKG Catalog
//======================================================================================================================

class GPKGCatalog final : public Catalog {
public:
	static constexpr auto TYPE = "GeoPackage";

	GPKGCatalog(AttachedDatabase &db, const string &path);

// Storage Callbacks
	~GPKGCatalog() override = default;

	void Initialize(bool load_builtin) override;

	string GetCatalogType() override { return TYPE; }

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;
	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;
	optional_ptr<SchemaCatalogEntry> GetSchema(CatalogTransaction transaction, const string &schema_name, OnEntryNotFound if_not_found, QueryErrorContext error_context) override;
	unique_ptr<PhysicalOperator> PlanInsert(ClientContext &context, LogicalInsert &op, unique_ptr<PhysicalOperator> plan) override;
	unique_ptr<PhysicalOperator> PlanCreateTableAs(ClientContext &context, LogicalCreateTable &op, unique_ptr<PhysicalOperator> plan) override;
	unique_ptr<PhysicalOperator> PlanDelete(ClientContext &context, LogicalDelete &op, unique_ptr<PhysicalOperator> plan) override;
	unique_ptr<PhysicalOperator> PlanUpdate(ClientContext &context, LogicalUpdate &op, unique_ptr<PhysicalOperator> plan) override;
	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table, unique_ptr<LogicalOperator> plan) override;

// Database management
	DatabaseSize GetDatabaseSize(ClientContext &context) override;
	bool InMemory() override;
	string GetDBPath() override;

// GPKG database management
	//! Returns a reference to the in-memory database (if any)
	GPKGDB *GetInMemoryDatabase();
	//! Release the in-memory database (if there is any)
	void ReleaseInMemoryDatabase();
private:
	void DropSchema(ClientContext &context, DropInfo &info) override;
private:
	//! The path to the database
	string path;
	//! Whether or not the database is in-memory
	bool in_memory;
	//! In-memory database, if any
	GPKGDB in_memory_db;
	//! Lock for accessing the in-memory database
	mutex in_memory_lock;
	//! Whether or not there are active transactions on the in-memory database
	bool active_in_memory;
	//! The main schema of the database
	unique_ptr<GPKGSchemaEntry> main_schema;
};

//======================================================================================================================
// GPKG Transaction
//======================================================================================================================

class GPKGTransaction final : public Transaction {
public:
	GPKGTransaction(GPKGCatalog &gpkg_catalog, TransactionManager &manager, ClientContext &context);
	~GPKGTransaction() override;

	void Start();
	void Commit();
	void Rollback();

	GPKGDB &GetDB();
	optional_ptr<CatalogEntry> GetCatalogEntry(const string &entry_name);
	void DropEntry(CatalogType type, const string &table_name, bool cascade);
	void ClearTableEntry(const string &table_name);

	static GPKGTransaction &Get(ClientContext &context, Catalog &catalog);
private:
	GPKGCatalog &gpkg_catalog;
	case_insensitive_map_t<unique_ptr<CatalogEntry>> catalog_entries;

	GPKGDB *db;
	GPKGDB owned_db;
};

GPKGTransaction::GPKGTransaction(GPKGCatalog &gpkg_catalog_p, TransactionManager &manager, ClientContext &context)
	: Transaction(manager, context), gpkg_catalog(gpkg_catalog_p) {
	if(gpkg_catalog_p.InMemory()) {
		// TODO:
	} else {
		// TODO:
	}
}

GPKGTransaction::~GPKGTransaction() {
	gpkg_catalog.ReleaseInMemoryDatabase();
}

void GPKGTransaction::Start() {
	db->Execute("BEGIN TRANSACTION");
}

void GPKGTransaction::Commit() {
	db->Execute("COMMIT");
}

void GPKGTransaction::Rollback() {
	db->Execute("ROLLBACK");
}

GPKGDB &GPKGTransaction::GetDB() {
	return *db;
}

GPKGTransaction &GPKGTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<GPKGTransaction>();
}

optional_ptr<CatalogEntry> GPKGTransaction::GetCatalogEntry(const string &entry_name) {
	// TODO:
	return nullptr;
}

void GPKGTransaction::ClearTableEntry(const string &table_name) {
	catalog_entries.erase(table_name);
}

void GPKGTransaction::DropEntry(CatalogType type, const string &table_name, bool cascade) {
	// TODO:
}

//======================================================================================================================
// GPKG Transaction Manager
//======================================================================================================================

class GPKGTransactionManager final : public TransactionManager {
public:
	GPKGTransactionManager(AttachedDatabase &db, GPKGCatalog &gpkg_catalog);
	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;
	void Checkpoint(ClientContext &context, bool force) override;
	void Checkpoint(ClientContext &context) { Checkpoint(context, false); }
private:
	GPKGCatalog &catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<GPKGTransaction>> transactions;
};

GPKGTransactionManager::GPKGTransactionManager(AttachedDatabase &db, GPKGCatalog &gpkg_catalog)
	: TransactionManager(db), catalog(gpkg_catalog) {
}

Transaction &GPKGTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<GPKGTransaction>(catalog, *this, context);
	transaction->Start();
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData GPKGTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &gpkg_transaction = transaction.Cast<GPKGTransaction>();
	gpkg_transaction.Commit();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return ErrorData();
}

void GPKGTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &gpkg_transaction = transaction.Cast<GPKGTransaction>();
	gpkg_transaction.Rollback();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

void GPKGTransactionManager::Checkpoint(ClientContext &context, bool force) {
	auto &transaction = GPKGTransaction::Get(context, catalog);
	auto &db = transaction.GetDB();
	db.Execute("PRAGMA wal_checkpoint");
}


//======================================================================================================================
// GPKG Schema (Implementation
//======================================================================================================================
GPKGSchemaEntry::GPKGSchemaEntry(Catalog &catalog, CreateSchemaInfo &info)
	: SchemaCatalogEntry(catalog, info) {
}

optional_ptr<CatalogEntry> GPKGSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	// TODO:
	throw NotImplementedException("GPKG databases do not support creating tables yet");
}

optional_ptr<CatalogEntry> GPKGSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
									   TableCatalogEntry &table) {
	// TODO:
	throw NotImplementedException("GPKG databases do not support creating indexes yet");
}

optional_ptr<CatalogEntry> GPKGSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	// TODO:
	throw NotImplementedException("GPKG databases do not support creating views yet");
}

optional_ptr<CatalogEntry> GPKGSchemaEntry::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
	throw BinderException("GPKG databases do not support creating sequences");
}

optional_ptr<CatalogEntry> GPKGSchemaEntry::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
	throw BinderException("GPKG databases do not support creating functions");
}

optional_ptr<CatalogEntry> GPKGSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
											   CreateTableFunctionInfo &info) {
	throw BinderException("GPKG databases do not support creating table functions");
}

optional_ptr<CatalogEntry> GPKGSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
											  CreateCopyFunctionInfo &info) {
	throw BinderException("GPKG databases do not support creating copy functions");
}

optional_ptr<CatalogEntry> GPKGSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
												CreatePragmaFunctionInfo &info) {
	throw BinderException("GPKG databases do not support creating pragma functions");
}

optional_ptr<CatalogEntry> GPKGSchemaEntry::CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) {
	throw BinderException("GPKG databases do not support creating collations");
}

optional_ptr<CatalogEntry> GPKGSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw BinderException("GPKG databases do not support creating types");
}


void GPKGSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {

}

void GPKGSchemaEntry::Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	// TODO:
}

void GPKGSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	// TODO:
}

void GPKGSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	// TODO:
}

optional_ptr<CatalogEntry> GPKGSchemaEntry::GetEntry(CatalogTransaction transaction, CatalogType type, const string &name) {
	// TODO:
	return nullptr;
}

//======================================================================================================================
// GPKG Catalog (Implementation)
//======================================================================================================================

GPKGCatalog::GPKGCatalog(AttachedDatabase &db, const string &path)
  : Catalog(db), path(path), in_memory(path == ":memory:"), active_in_memory(false) {

	if(InMemory()) {
		// TODO:
		in_memory_db = GPKGDB();
	}
}

void GPKGCatalog::Initialize(bool load_builtin) {
	CreateSchemaInfo info;
	main_schema = make_uniq<GPKGSchemaEntry>(*this, info);
}


optional_ptr<CatalogEntry> GPKGCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	throw BinderException("GPKG databases do not support creating new schemas");
}

void GPKGCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	callback(*main_schema);
}

optional_ptr<SchemaCatalogEntry> GPKGCatalog::GetSchema(CatalogTransaction transaction, const string &schema_name, OnEntryNotFound if_not_found, QueryErrorContext error_context) {
	if(schema_name == DEFAULT_SCHEMA || schema_name.empty()) {
		return main_schema.get();
	}
	if(if_not_found == OnEntryNotFound::RETURN_NULL) {
		return nullptr;
	}
	throw BinderException("GPKG databases only have a single schema - \"%s\"", DEFAULT_SCHEMA);
}

unique_ptr<PhysicalOperator> GPKGCatalog::PlanInsert(ClientContext &context, LogicalInsert &op, unique_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("GPKG databases do not support inserting data yet");
}

unique_ptr<PhysicalOperator> GPKGCatalog::PlanCreateTableAs(ClientContext &context, LogicalCreateTable &op, unique_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("GPKG databases do not support creating tables yet");
}

unique_ptr<PhysicalOperator> GPKGCatalog::PlanDelete(ClientContext &context, LogicalDelete &op, unique_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("GPKG databases do not support deleting data yet");
}

unique_ptr<PhysicalOperator> GPKGCatalog::PlanUpdate(ClientContext &context, LogicalUpdate &op, unique_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("GPKG databases do not support updating data yet");
}

unique_ptr<LogicalOperator> GPKGCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table, unique_ptr<LogicalOperator> plan) {
	throw NotImplementedException("GPKG databases do not support creating indexes yet");
}

DatabaseSize GPKGCatalog::GetDatabaseSize(ClientContext &context) {
	DatabaseSize result;
	auto &transaction = GPKGTransaction::Get(context, *this);
	auto &db = transaction.GetDB();
	result.total_blocks = db.RunPragma("page_count");
	result.block_size = db.RunPragma("page_size");
	result.free_blocks = db.RunPragma("freelist_count");
	result.used_blocks = result.total_blocks - result.free_blocks;
	result.bytes = result.total_blocks * result.block_size;
	result.wal_size = idx_t(-1);
	return result;
}

bool GPKGCatalog::InMemory() {
	return in_memory;
}

string GPKGCatalog::GetDBPath() {
	return path;
}

GPKGDB* GPKGCatalog::GetInMemoryDatabase() {
	if(!InMemory()) {
		throw InternalException("GetInMemoryDatabase() called on a non-in-memory database");
	}
	lock_guard<mutex> l(in_memory_lock);
	if(active_in_memory) {
		throw TransactionException("Only a single transaction can be active on an in-memory GPKG database at a time");
	}
	active_in_memory = true;
	return &in_memory_db;
}

void GPKGCatalog::ReleaseInMemoryDatabase() {
	if(!InMemory()) {
		return;
	}
	lock_guard<mutex> l(in_memory_lock);
	if(!active_in_memory) {
		throw InternalException("ReleaseInMemoryDatabase called but there is no active transaction on an in-memory database");
	}
	active_in_memory = false;
}

void GPKGCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	throw BinderException("GPKG databases do not support dropping schemas");
}


//======================================================================================================================
// GPKG Storage Extension
//======================================================================================================================
class GPKGStorageExtension final : public StorageExtension {
public:
	static unique_ptr<Catalog> Attach(StorageExtensionInfo *storage_info, ClientContext &context, AttachedDatabase &db,
		const string &name, AttachInfo &info, AccessMode access_mode) {

		return make_uniq_base<Catalog, GPKGCatalog>(db, info.path);
	}

	static unique_ptr<TransactionManager> CreateTransactionManager(StorageExtensionInfo *storage_info,
																		 AttachedDatabase &db, Catalog &catalog) {
		auto &gpkg_catalog = catalog.Cast<GPKGCatalog>();

		return make_uniq_base<TransactionManager, GPKGTransactionManager>(db, gpkg_catalog);
	}

	GPKGStorageExtension() {
		attach = Attach;
		create_transaction_manager = CreateTransactionManager;
	}
};

} // namespace

} // namespace duckdb

//######################################################################################################################
// Register Module
//######################################################################################################################

namespace duckdb {

void GPKGModule::Register(DatabaseInstance &db) {
	// Register the GPKG module with the database instance
	auto &config = DBConfig::GetConfig(db);

	// TODO: This is shit, but requires fixing in core DuckDB
	config.storage_extensions["spatial"] = make_uniq<GPKGStorageExtension>();
}

} // namespace duckdb
