/*
 * Copyright 2021 Database Group, Nagoya University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef BW_TREE_COMPONENT_DELTA_RECORD_HPP
#define BW_TREE_COMPONENT_DELTA_RECORD_HPP

#include <atomic>
#include <optional>
#include <utility>
#include <vector>

#include "common.hpp"
#include "metadata.hpp"

namespace dbgroup::index::bw_tree::component
{
/**
 * @brief A class to represent delta records in Bw-tree.
 *
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 * @tparam Comp a comparetor class for keys.
 */
template <class Key, class Comp>
class DeltaRecord
{
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using NodeStack = std::vector<std::atomic_uintptr_t *>;

 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct a new delta record for leaf-insert/modify/delete or split.
   *
   * @tparam T a target payload class.
   * @param delta_type
   * @param key
   * @param key_length
   * @param payload
   * @param payload_length
   */
  template <class T>
  DeltaRecord(  //
      const DeltaType delta_type,
      const Key &key,
      const size_t key_length,
      const T &payload,
      const size_t payload_length)
      : delta_type_{delta_type}
  {
    auto [key_len, pay_len, rec_len] = Align<Key, T>(key_length, payload_length);
    auto offset = sizeof(DeltaRecord);

    offset = SetData(offset, payload, pay_len);
    offset = SetData(offset, key, key_len);
    meta_ = Metadata{offset, key_len, rec_len};
  }

  /**
   * @brief Construct a new delta record for split deltas.
   *
   * @param split_delta
   */
  DeltaRecord(  //
      const uintptr_t node,
      const uintptr_t sib_page,
      const uintptr_t next)
      : delta_type_{DeltaType::kSplit}, next_{next}
  {
    auto *delta = reinterpret_cast<DeltaRecord *>(node);
    auto offset = sizeof(DeltaRecord);

    // set a sibling node
    offset = SetData(offset, sib_page, sizeof(uintptr_t));

    // copy a lowest key
    auto key_len = delta->meta_.GetKeyLength();
    offset -= key_len;
    memcpy(ShiftAddr(this, offset), delta->GetKeyAddr(delta->meta_), key_len);
    meta_ = Metadata{offset, key_len, key_len + sizeof(uintptr_t)};
  }

  /**
   * @brief Construct a new delta record for index-entries.
   *
   * @param split_delta
   */
  DeltaRecord(const DeltaRecord *delta)
      : node_type_{NodeType::kInternal}, delta_type_{DeltaType::kInsert}
  {
    auto offset = sizeof(DeltaRecord);

    // copy a highest key from a sibling node
    auto *sib_page = delta->template GetPayload<std::atomic<DeltaRecord *> *>();
    auto *sib_node = sib_page->load(std::memory_order_relaxed);
    high_key_meta_ = sib_node->high_key_meta_;
    auto rec_len = high_key_meta_.GetKeyLength();
    if (key_len > 0) {
      offset -= key_len;
      memcpy(ShiftAddr(this, offset), sib_node->GetKeyAddr(high_key_meta_), key_len);
      high_key_meta_.SetOffset(offset);
    }

    // copy contents of a split delta
    meta_ = delta->meta_;
    rec_len = meta_.GetTotalLength();
    offset -= rec_len;
    memcpy(ShiftAddr(this, offset), delta->GetKeyAddr(meta_), rec_len);
    meta_.SetOffset(offset);
  }

  constexpr DeltaRecord(const DeltaRecord &) = default;
  constexpr auto operator=(const DeltaRecord &) -> DeltaRecord & = default;
  constexpr DeltaRecord(DeltaRecord &&) = default;
  constexpr auto operator=(DeltaRecord &&) -> DeltaRecord & = default;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  constexpr ~DeltaRecord() = default;

  /*####################################################################################
   * Public getters/setters
   *##################################################################################*/

  /**
   * @retval true if this is a leaf node.
   * @retval false if this is an internal node.
   */
  [[nodiscard]] constexpr auto
  IsLeaf() const  //
      -> bool
  {
    return node_type_;
  }

  /**
   * @retval true if this is a base node.
   * @retval false if this is a delta record.
   */
  [[nodiscard]] constexpr auto
  IsBaseNode() const  //
      -> bool
  {
    return delta_type_ == DeltaType::kNotDelta;
  }

  [[nodescard]] constexpr auto
  HasPayload() const  //
      -> bool
  {
    return delta_type_ == DeltaType::kInsert || delta_type_ == DeltaType::kModify;
  }

  [[nodiscard]] constexpr auto
  GetNext() const  //
      -> uintptr_t
  {
    return next_;
  }

  /**
   * @param meta metadata of a corresponding record.
   * @return a key in a target record.
   */
  [[nodiscard]] auto
  GetKey() const  //
      -> Key
  {
    if constexpr (IsVariableLengthData<Key>()) {
      return reinterpret_cast<Key>(GetKeyAddr(meta_));
    } else {
      Key key{};
      memcpy(&key, GetKeyAddr(meta_), sizeof(Key));
      return key;
    }
  }

  /**
   * @tparam Payload a class of payload.
   * @return a payload in this record.
   */
  template <class T>
  [[nodiscard]] auto
  GetPayload() const  //
      -> T
  {
    if constexpr (IsVariableLengthData<T>()) {
      return reinterpret_cast<T>(GetPayloadAddr());
    } else {
      T payload{};
      memcpy(&payload, GetPayloadAddr(), sizeof(T));
      return payload;
    }
  }

  /**
   * @brief Copy a target payload to a specified reference.
   *
   * @param out_payload a reference to be copied a target payload.
   */
  template <class T>
  [[nodiscard]] auto
  CopyPayload() const  //
      -> T
  {
    if constexpr (IsVariableLengthData<T>()) {
      const auto pay_len = meta_.GetPayloadLength();
      auto payload = reinterpret_cast<T>(::operator new(pay_len));
      memcpy(payload, GetPayloadAddr(), pay_len);
      return payload;
    } else {
      T payload{};
      memcpy(&payload, GetPayloadAddr(), sizeof(T));
      return payload;
    }
  }

  void
  SetNext(uintptr_t next)
  {
    next_ = next;
  }

  /*####################################################################################
   * Public utilities
   *##################################################################################*/

  static auto
  SearchChildNode(  //
      const Key &key,
      const bool closed,
      const std::atomic_uintptr_t *&page_id)  //
      -> std::pair<uintptr_t, DeltaRC>
  {
    DeltaRC delta_chain_length = 0;
    uintptr_t child_page{};
    auto ptr = page_id->load(std::memory_order_acquire);
    DeltaRecord *cur_rec = reinterpret_cast<DeltaRecord *>(ptr);

    // traverse a delta chain
    while (true) {
      switch (cur_rec->delta_type_) {
        case DeltaType::kInsert:
          auto &&sep_key = cur_rec->GetKey();
          if (Comp{}(sep_key, key) || (!closed && !Comp{key, sep_key})) {
            // this index term delta directly indicates a child node
            child_page = cur_rec->template GetPayload<uintptr_t>();
            return {child_page, kRecordFound};
          }
          break;

        case DeltaType::kSplit:
          sep_key = cur_rec->GetKey();
          if (Comp{}(sep_key, key) || (!closed && !Comp{key, sep_key})) {
            // a sibling node includes a target key
            page_id = cur_rec->template GetPayload<std::atomic_uintptr_t *>();
            ptr = page_id->load(std::memory_order_acquire);
            cur_rec = reinterpret_cast<DeltaRecord *>(ptr);
            delta_chain_length = 0;
            continue;
          }
          break;

        case DeltaType::kNotDelta:
          const auto &high_key = cur_rec->GetHighKey();
          if (high_key && (Comp{}(*high_key, key) || (!closed && !Comp{key, *high_key}))) {
            // a sibling node includes a target key
            page_id = reinterpret_cast<std::atomic_uintptr_t *>(cur_rec->next_);
            ptr = page_id->load(std::memory_order_acquire);
            cur_rec = reinterpret_cast<DeltaRecord *>(ptr);
            delta_chain_length = 0;
            continue;
          }
          // reach a base page
          return {ptr, delta_chain_length};

        case DeltaType::kDelete:
          // ...not implemented yet
          break;

        case DeltaType::kMerge:
          // ...not implemented yet
          break;

        case DeltaType::kRemoveNode:
          // ...not implemented yet
          break;

        default:
          break;
      }

      // go to the next delta record or base node
      ptr = cur_rec->next_;
      cur_rec = reinterpret_cast<DeltaRecord *>(ptr);
      ++delta_chain_length;
    }
  }

  auto
  SearchRecord(  //
      const Key &key,
      NodeStack &stack)  //
      -> std::pair<uintptr_t, DeltaRC>
  {
    auto *page_id = stack.back();
    auto ptr = page_id->load(std::memory_order_acquire);
    DeltaRecord *cur_rec = reinterpret_cast<DeltaRecord *>(ptr);
    DeltaRC delta_chain_length = 0;

    // traverse a delta chain
    while (true) {
      switch (cur_rec->delta_type_) {
        case DeltaType::kInsert:
        case DeltaType::kModify:
          auto &&rec_key = cur_rec->GetKey();
          if (!Comp{}(key, rec_key) && !Comp{}(rec_key, key)) {
            // this delta record contains a target key
            return {ptr, kRecordFound};
          }
          break;

        case DeltaType::kDelete:
          rec_key = cur_rec->GetKey();
          if (!Comp{}(key, rec_key) && !Comp{}(rec_key, key)) {
            // a target key is deleted
            return {ptr, kRecordDeleted};
          }
          break;

        case DeltaType::kSplit:
          rec_key = cur_rec->GetKey();
          if (Comp{}(rec_key, key)) {
            // a sibling node may include a target key
            page_id = cur_rec->template GetPayload<std::atomic_uintptr_t *>();
            ptr = page_id->load(std::memory_order_acquire);
            cur_rec = reinterpret_cast<DeltaRecord *>(ptr);
            delta_chain_length = 0;

            // swap a current node in a stack
            *stack.rbegin() = page_id;
            continue;
          }
          break;

        case DeltaType::kNotDelta:
          const auto &high_key = cur_rec->GetHighKey();
          if (high_key && Comp{}(*high_key, key)) {
            // a sibling node includes a target key
            page_id = reinterpret_cast<std::atomic_uintptr_t *>(cur_rec->next_);
            ptr = page_id->load(std::memory_order_acquire);
            cur_rec = reinterpret_cast<DeltaRecord *>(ptr);
            delta_chain_length = 0;

            // swap a current node in a stack
            *stack.rbegin() = page_id;
            continue;
          }
          // reach a base page
          return {ptr, delta_chain_length};

        case DeltaType::kMerge:
          // ...not implemented yet
          break;

        case DeltaType::kRemoveNode:
          // ...not implemented yet
          break;

        default:
          break;
      }

      // go to the next delta record or base node
      ptr = cur_rec->next_;
      cur_rec = reinterpret_cast<DeltaRecord *>(ptr);
      ++delta_chain_length;
    }
  }

  static auto
  Validate(  //
      const Key &key,
      const bool closed,
      const uintptr_t prev_head,
      NodeStack &stack)  //
      -> std::pair<uintptr_t, DeltaRC>
  {
    auto *page_id = stack.back();
    auto head = page_id->load(std::memory_order_acquire);
    auto ptr = head;
    DeltaRecord *cur_rec = reinterpret_cast<DeltaRecord *>(ptr);
    DeltaRC delta_chain_length = 0;

    // traverse a delta chain
    while (ptr != prev_head) {
      switch (cur_rec->delta_type_) {
        case DeltaType::kSplit:
          const auto &sep_key = cur_rec->GetKey();
          if (Comp{}(sep_key, key) || (!closed && !Comp{key, sep_key})) {
            // there may be incomplete split
            return {ptr, kSplitMayIncomplete};
          }
          break;

        case DeltaType::kNotDelta:
          const auto &high_key = cur_rec->GetHighKey();
          if (high_key && (Comp{}(*high_key, key) || (!closed && !Comp{key, *high_key}))) {
            // a sibling node includes a target key
            page_id = reinterpret_cast<std::atomic_uintptr_t *>(cur_rec->next_);
            head = page_id->load(std::memory_order_acquire);
            ptr = head;
            cur_rec = reinterpret_cast<DeltaRecord *>(ptr);
            delta_chain_length = 0;

            // swap a current node in a stack
            *stack.rbegin() = page_id;
            continue;
          }
          // reach a base page
          return {head, delta_chain_length};

        case DeltaType::kMerge:
          // ...not implemented yet
          break;

        case DeltaType::kRemoveNode:
          // ...not implemented yet
          break;

        default:
          break;
      }

      // go to the next delta record or base node
      ptr = cur_rec->next_;
      cur_rec = reinterpret_cast<DeltaRecord *>(ptr);
      ++delta_chain_length;
    }
  }

  static auto
  Sort(  //
      uintptr_t ptr,
      std::vector<std::pair<Key, uintptr_t>> &records)  //
      -> std::tuple<uintptr_t, std::optional<Key>, Metadata, std::atomic_uintptr_t *>
  {
    DeltaRecord *cur_rec = reinterpret_cast<DeltaRecord *>(ptr);
    std::optional<Key> high_key = std::nullopt;
    Metadata high_meta{};
    std::atomic_uintptr_t *sib_node = nullptr;
    int64_t size_diff = 0;

    // traverse and sort a delta chain
    while (true) {
      switch (cur_rec->delta_type_) {
        case DeltaType::kInsert:
        case DeltaType::kModify:
        case DeltaType::kDelete:
          // check whether this record is in a target node
          auto &&key = cur_rec->GetKey();
          if (!high_key || !Comp{}(*high_key, key)) {
            // check uniqueness
            auto it = records.cbegin();
            const auto it_end = records.cend();
            for (; it != it_end; ++it) {
              if (!Comp{}(key, it->first)) break;
            }
            if (it == it_end) {
              records.emplace_back(std::make_pair(std::move(key), ptr));
            } else if (Comp{}(it->first, key)) {
              records.insert(it, std::make_pair(std::move(key), ptr));
            }
          }

          // update the page size
          if (cur_rec->delta_type_ == DeltaType::kInsert) {
            size_diff += cur_rec->meta_.GetTotalLength() + sizeof(Metadata);
          } else if (cur_rec->delta_type_ == DeltaType::kDelete) {
            size_diff -= cur_rec->meta_.GetTotalLength() + sizeof(Metadata);
          } else if constexpr (IsVariableLengthData<Payload>()) {
            size_diff += cur_rec->meta_.GetPayloadLength();
          }
          break;

        case DeltaType::kSplit:
          if (!high_key) {
            // the first split delta has a highest key and a sibling node
            high_meta = cur_rec->meta_;
            high_key = cur_rec->GetHighKey();
            sib_node = cur_rec->template GetPayload<std::atomic_uintptr_t *>();
          }
          break;

        case DeltaType::kNotDelta:
          if (!high_key) {
            // if there are no SMOs, a base node has a highest key and a sibling node
            high_meta = cur_rec->high_key_meta_;
            high_key = cur_rec->GetHighKey();
            sib_node = reinterpret_cast<std::atomic_uintptr_t *>(cur_rec->next_);
          }
          return {ptr, high_key, high_meta, sib_node, size_diff};

        case DeltaType::kMerge:
          // ...not implemented yet
          break;

        default:
          break;
      }

      // go to the next delta record or base node
      ptr = cur_rec->next_;
      cur_rec = reinterpret_cast<DeltaRecord *>(ptr);
    }
  }

 private:
  /*####################################################################################
   * Internal getters/setters
   *##################################################################################*/

  /**
   * @param meta metadata of a corresponding record.
   * @return an address of a target key.
   */
  [[nodiscard]] constexpr auto
  GetKeyAddr(const Metadata meta) const  //
      -> void *
  {
    return ShiftAddr(this, meta.GetOffset());
  }

  /**
   * @return a highest key in a target record.
   */
  [[nodiscard]] constexpr auto
  GetHighKey() const  //
      -> std::optional<Key>
  {
    const auto key_len = high_key_meta_.GetKeyLength();
    if (key_len == 0) return std::nullopt;

    if constexpr (IsVariableLengthData<Key>()) {
      return reinterpret_cast<Key>(GetKeyAddr(high_key_meta_));
    } else {
      Key key{};
      memcpy(&key, GetKeyAddr(high_key_meta_), sizeof(Key));
      return key;
    }
  }

  /**
   * @return an address of a target payload.
   */
  [[nodiscard]] constexpr auto
  GetPayloadAddr() const  //
      -> void *
  {
    return ShiftAddr(this, meta_.GetOffset() + meta_.GetKeyLength());
  }

  /**
   * @brief Set a target data directly.
   *
   * @tparam T a class of data.
   * @param offset an offset to set a target data.
   * @param payload a target payload to be set.
   * @param payload_length the length of a target payload.
   */
  template <class T>
  auto
  SetData(  //
      size_t offset,
      const T &data,
      [[maybe_unused]] const size_t data_len)  //
      -> size_t
  {
    if constexpr (IsVariableLengthData<T>()) {
      offset -= data_len;
      memcpy(ShiftAddr(this, offset), data, data_len);
    } else {
      offset -= sizeof(T);
      memcpy(ShiftAddr(this, offset), &data, sizeof(T));
    }

    return offset;
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// a flag to indicate whether this node is a leaf or internal node.
  uint16_t node_type_ : 1;

  /// a flag for indicating the types of a delta record.
  uint16_t delta_type_ : 3;

  /// a blank block for alignment.
  uint64_t : 0;

  /// the pointer to the next node.
  uintptr_t next_{0};

  /// metadata of an embedded record
  Metadata meta_{};

  /// metadata of a highest key
  Metadata high_key_meta_{};

  /// an actual data block for records
  std::byte data_block_[GetMaxDeltaSize<Key, Payload>()]{};
};

}  // namespace dbgroup::index::bw_tree::component

#endif  // BW_TREE_COMPONENT_DELTA_RECORD_HPP
