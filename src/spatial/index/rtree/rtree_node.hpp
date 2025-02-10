#pragma once

#include "spatial/geometry/bbox.hpp"
#include "duckdb/execution/index/index_pointer.hpp"

#include <algorithm>

namespace duckdb {

//-------------------------------------------------------------
// RTree Pointer
//-------------------------------------------------------------

enum class RTreeNodeType : uint8_t {
	ROW_ID = 0,
	LEAF_PAGE = 1,
	BRANCH_PAGE = 2,
};

class RTreePointer final : public IndexPointer {
	static constexpr idx_t AND_ROW_ID = 0x00FFFFFFFFFFFFFF;

public:
	RTreePointer() = default;
	explicit RTreePointer(const IndexPointer &ptr) : IndexPointer(ptr) {
	}

	//! Get the RowID from this pointer
	row_t GetRowId() const {
		return static_cast<row_t>(Get());
	}

	void SetRowId(const row_t row_id) {
		Set(UnsafeNumericCast<idx_t>(row_id));
	}

	RTreeNodeType GetType() const {
		return static_cast<RTreeNodeType>(GetMetadata());
	}

	bool IsRowId() const {
		return GetType() == RTreeNodeType::ROW_ID;
	}
	bool IsLeafPage() const {
		return GetType() == RTreeNodeType::LEAF_PAGE;
	}
	bool IsBranchPage() const {
		return GetType() == RTreeNodeType::BRANCH_PAGE;
	}
	bool IsPage() const {
		return IsLeafPage() || IsBranchPage();
	}
	bool IsSet() const {
		return Get() != 0;
	}

	//! Assign operator
	RTreePointer &operator=(const IndexPointer &ptr) {
		Set(ptr.Get());
		return *this;
	}
};

using RTreeBounds = Box2D<float>;

struct RTreeEntry {
	RTreePointer pointer;
	RTreeBounds bounds;
	RTreeEntry() = default;
	RTreeEntry(const RTreePointer &pointer_p, const RTreeBounds &bounds_p) : pointer(pointer_p), bounds(bounds_p) {
	}
	void Clear() {
		pointer.Set(0);
	}
};

struct alignas(RTreeEntry) RTreeNode {
	// Delete constructor too
	RTreeNode() = delete;
	// Delete copy
	RTreeNode(const RTreeNode &) = delete;
	RTreeNode &operator=(const RTreeNode &) = delete;
	// Delete move
	RTreeNode(RTreeNode &&) = delete;
	RTreeNode &operator=(RTreeNode &&) = delete;

public:
	idx_t GetCount() const {
		return count;
	}

	RTreeBounds GetBounds() const {
		RTreeBounds result;
		for (idx_t i = 0; i < count; i++) {
			auto &entry = begin()[i];
			result.Union(entry.bounds);
		}
		return result;
	}

	void Clear() {
		count = 0;
	}

	void PushEntry(const RTreeEntry &entry) {
		begin()[count++] = entry;
	}

	// Swap the last entry with the entry at the given index and decrement the count
	// This does NOT preserve the order of the entries
	RTreeEntry SwapRemove(const idx_t idx) {
		D_ASSERT(idx < count);
		const auto result = begin()[idx];
		begin()[idx] = begin()[--count];
		return result;
	}

	// Remove the entry at the given index, decrement the count and compact the entries
	// by copying all entries after the removed entry one position to the left
	// This preserves the order of the entries
	RTreeEntry CompactRemove(const idx_t idx) {
		D_ASSERT(idx < count);
		const auto result = begin()[idx];
		for (idx_t i = idx + 1; i < count; i++) {
			begin()[i - 1] = begin()[i];
		}
		count--;
		return result;
	}

	void Verify(const idx_t capacity) const {
#ifdef DEBUG
		D_ASSERT(count <= capacity);
		if (count != 0) {
			if (begin()[0].pointer.GetType() == RTreeNodeType::ROW_ID) {
				// This is a leaf node, rowids should be sorted
				const auto ok = std::is_sorted(begin(), end(), [](const RTreeEntry &a, const RTreeEntry &b) {
					return a.pointer.GetRowId() < b.pointer.GetRowId();
				});
				D_ASSERT(ok);
			}
		}
#endif
	}

	void SortEntriesByXMin() {
		std::sort(begin(), end(),
		          [&](const RTreeEntry &a, const RTreeEntry &b) { return a.bounds.min.x < b.bounds.min.x; });
	}

	void SortEntriesByRowId() {
		std::sort(begin(), end(), [&](const RTreeEntry &a, const RTreeEntry &b) {
			return a.pointer.GetRowId() < b.pointer.GetRowId();
		});
	}

public: // Collection interface
	RTreeEntry *begin() {
		// This is slightly unsafe, but the entries are always laid out after the RTreeNode in memory
		// inside the same buffer. This is a performance optimization to avoid an additional pointer indirection.
		return reinterpret_cast<RTreeEntry *>(this + 1);
	}
	RTreeEntry *end() {
		return begin() + count;
	}
	const RTreeEntry *begin() const {
		return reinterpret_cast<const RTreeEntry *>(this + 1);
	}
	const RTreeEntry *end() const {
		return begin() + count;
	}

	RTreeEntry &operator[](const idx_t idx) {
		D_ASSERT(idx < count);
		return begin()[idx];
	}

	const RTreeEntry &operator[](const idx_t idx) const {
		D_ASSERT(idx < count);
		return begin()[idx];
	}

private:
	uint32_t count;

public:
	// We got 20 bytes for the future
	// make this public so compiler stops warning about unused fields
	uint8_t _unused[20] = {};
};

} // namespace duckdb