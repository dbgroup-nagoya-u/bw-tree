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
template <class Key, class Payload, class Comp>
class DeltaRecord
{
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

    offset = SetPayload(offset, payload, pay_len);
    offset = SetKey(offset, key, key_len);
    meta_ = Metadata{offset, key_len, rec_len};
  }

  /**
   * @brief Construct a new delta record for internal-insert/delete or merging.
   *
   * @param delta_type
   * @param low_key
   * @param high_key
   * @param sib_node
   */
  DeltaRecord(  //
      const DeltaType delta_type,
      const Key &low_key,
      const size_t low_key_len,
      const std::optional<std::pair<const Key &, size_t>> &high_key,
      const uintptr_t sib_node)
      : delta_type_{delta_type}
  {
    constexpr auto kPtrLength = sizeof(uintptr_t);
    auto offset = sizeof(DeltaRecord);

    if (high_key) {
      const auto &[h_key, h_key_len] = high_key.value();
      auto [key_len, pay_len, rec_len] = Align<Key, uintptr_t>(h_key_len, kPtrLength);

      auto tmp_offset = SetKey(offset, h_key, key_len);
      high_key_meta_ = Metadata{tmp_offset, key_len, rec_len};

      offset -= rec_len;
    } else {
      high_key_meta_ = Metadata{0, 0, 0};
    }

    auto [key_len, pay_len, rec_len] = Align<Key, uintptr_t>(low_key_len, kPtrLength);

    offset = SetPayload(offset, sib_node, pay_len);
    offset = SetKey(offset, low_key, key_len);
    meta_ = Metadata{offset, key_len, rec_len};
  }

  constexpr DeltaRecord(const DeltaRecord &) = default;
  constexpr auto operator=(const DeltaRecord &) -> DeltaRecord & = default;
  constexpr DeltaRecord(DeltaRecord &&) = default;
  constexpr auto operator=(DeltaRecord &&) -> DeltaRecord & = default;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  constexpr ~DeltaRecord() = default;

  /**
   * @tparam Payload a class of payload.
   * @return a payload in this record.
   */
  template <class T>
  [[nodiscard]] constexpr auto
  GetPayload() const  //
      -> T
  {
    if constexpr (IsVariableLengthData<T>()) {
      return reinterpret_cast<T>(GetPayloadAddr());
    } else {
      return *reinterpret_cast<T *>(GetPayloadAddr());
    }
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
          const auto &sep_key = cur_rec->GetKey(cur_rec->meta_);
          if (Comp{}(sep_key, key) || (!closed && !Comp{key, sep_key})) {
            // this index term delta directly indicates a child node
            child_page = cur_rec->template GetPayload<uintptr_t>();
            return {child_page, kChildFound};
          }
          break;

        case DeltaType::kSplit:
          const auto &sep_key = cur_rec->GetKey(cur_rec->meta_);
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
          if (cur_rec->high_key_meta_.GetKeyLength() > 0) {
            // this base node has a sibling node
            const auto &high_key = cur_rec->GetKey(cur_rec->high_key_meta_);
            if (Comp{}(high_key, key) || (!closed && !Comp{key, high_key})) {
              // a sibling node includes a target key
              page_id = reinterpret_cast<std::atomic_uintptr_t *>(cur_rec->next_);
              ptr = page_id->load(std::memory_order_acquire);
              cur_rec = reinterpret_cast<DeltaRecord *>(ptr);
              delta_chain_length = 0;
              continue;
            }
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

      ++delta_chain_length;
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
   * @param meta metadata of a corresponding record.
   * @return a key in a target record.
   */
  [[nodiscard]] constexpr auto
  GetKey(const Metadata meta) const  //
      -> Key
  {
    if constexpr (IsVariableLengthData<Key>()) {
      return reinterpret_cast<Key>(GetKeyAddr(meta));
    } else {
      return *reinterpret_cast<Key *>(GetKeyAddr(meta));
    }
  }

  /**
   * @param meta metadata of a corresponding record.
   * @return an address of a target payload.
   */
  [[nodiscard]] constexpr auto
  GetPayloadAddr() const  //
      -> void *
  {
    return ShiftAddr(this, meta_.GetOffset() + meta_.GetKeyLength());
  }

  /**
   * @brief Set a target key.
   *
   * @param offset an offset to set a target key.
   * @param key a target key to be set.
   * @param key_length the length of a target key.
   */
  auto
  SetKey(  //
      size_t offset,
      const Key &key,
      const size_t key_length)  //
      -> size_t
  {
    offset -= key_length;
    if constexpr (IsVariableLengthData<Key>()) {
      memcpy(ShiftAddr(this, offset), key, key_length);
    } else {
      memcpy(ShiftAddr(this, offset), &key, sizeof(Key));
    }

    return offset;
  }

  /**
   * @brief Set a target payload directly.
   *
   * @tparam Payload a class of payload.
   * @param offset an offset to set a target payload.
   * @param payload a target payload to be set.
   * @param payload_length the length of a target payload.
   */
  template <class T>
  auto
  SetPayload(  //
      size_t offset,
      const T &payload,
      const size_t payload_length)  //
      -> size_t
  {
    offset -= payload_length;
    if constexpr (IsVariableLengthData<T>()) {
      memcpy(ShiftAddr(this, offset), payload, payload_length);
    } else {
      memcpy(ShiftAddr(this, offset), &payload, sizeof(T));
    }

    return offset;
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

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
