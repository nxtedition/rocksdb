// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#include <vector>

#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/write_batch.h"

namespace ROCKSDB_NAMESPACE {

class WalFile;
using VectorWalPtr = std::vector<std::unique_ptr<WalFile>>;
// DEPRECATED old name
using VectorLogPtr = VectorWalPtr;

enum WalFileType {
  /* Indicates that WAL file is in archive directory. WAL files are moved from
   * the main db directory to archive directory once they are not live and stay
   * there until cleaned up. Files are cleaned depending on archive size
   * (Options::WAL_size_limit_MB) and time since last cleaning
   * (Options::WAL_ttl_seconds).
   */
  kArchivedLogFile = 0,

  /* Indicates that WAL file is live and resides in the main db directory */
  kAliveLogFile = 1
};

class WalFile {
 public:
  WalFile() {}
  virtual ~WalFile() {}

  // Returns log file's pathname relative to the main db dir
  // Eg. For a live-log-file = /000003.log
  //     For an archived-log-file = /archive/000003.log
  virtual std::string PathName() const = 0;

  // Primary identifier for log file.
  // This is directly proportional to creation time of the log file
  virtual uint64_t LogNumber() const = 0;

  // Log file can be either alive or archived
  virtual WalFileType Type() const = 0;

  // Starting sequence number of writebatch written in this log file
  virtual SequenceNumber StartSequence() const = 0;

  // The position of the last flushed write to the file (which for
  // recycled WAL files is typically less than the full file size).
  virtual uint64_t SizeFileBytes() const = 0;
};

// DEPRECATED old name for WalFile. (Confusing with "Logger" etc.)
using LogFile = WalFile;

struct BatchResult {
  SequenceNumber sequence = 0;

  // The WAL file number containing this batch. Together with
  // wal_record_offset, this identifies the batch while the WAL is retained.
  uint64_t wal_file_number = 0;

  // The physical byte offset of the first fragment of this logical WAL record.
  uint64_t wal_record_offset = 0;

  // The first physical byte offset after this logical WAL record.
  uint64_t wal_record_end = 0;

  // XXH3 checksum of the logical WAL record contents, or zero when
  // ReadOptions::include_wal_record_checksum_ is false.
  uint64_t wal_record_checksum = 0;

  std::unique_ptr<WriteBatch> writeBatchPtr;

  // Add empty __ctor and __dtor for the rule of five
  // However, preserve the original semantics and prohibit copying
  // as the std::unique_ptr member does not copy.
  BatchResult() {}

  ~BatchResult() {}

  BatchResult(const BatchResult&) = delete;

  BatchResult& operator=(const BatchResult&) = delete;

  BatchResult(BatchResult&& bResult) noexcept
      : sequence(bResult.sequence),
        wal_file_number(bResult.wal_file_number),
        wal_record_offset(bResult.wal_record_offset),
        wal_record_end(bResult.wal_record_end),
        wal_record_checksum(bResult.wal_record_checksum),
        writeBatchPtr(std::move(bResult.writeBatchPtr)) {}

  BatchResult& operator=(BatchResult&& bResult) noexcept {
    if (this == &bResult) {
      return *this;
    }
    sequence = bResult.sequence;
    wal_file_number = bResult.wal_file_number;
    wal_record_offset = bResult.wal_record_offset;
    wal_record_end = bResult.wal_record_end;
    wal_record_checksum = bResult.wal_record_checksum;
    writeBatchPtr = std::move(bResult.writeBatchPtr);
    return *this;
  }
};

// A TransactionLogIterator is used to iterate over the transactions in a db.
// One run of the iterator is continuous, i.e. the iterator will stop at the
// beginning of any gap in sequences
class TransactionLogIterator {
 public:
  TransactionLogIterator() {}
  virtual ~TransactionLogIterator() {}

  // An iterator is either positioned at a WriteBatch or not valid.
  // This method returns true if the iterator is valid.
  // Can read data from a valid iterator.
  virtual bool Valid() = 0;

  // Moves the iterator to the next WriteBatch. At a clean WAL tail, callers
  // may append more writes and call Next() again to continue tailing.
  // REQUIRES: Valid() is true, or Valid() is false and status() is OK.
  virtual void Next() = 0;

  // Returns OK if the iterator is valid or stopped at a clean WAL tail.
  // Returns the error when something has gone wrong.
  virtual Status status() = 0;

  // If valid return's the current write_batch and the sequence number of the
  // earliest transaction contained in the batch.
  // ONLY use if Valid() is true and status() is OK.
  virtual BatchResult GetBatch() = 0;

  // The read options for TransactionLogIterator.
  struct ReadOptions {
    // If true, all data read from underlying storage will be
    // verified against corresponding checksums.
    // Default: true
    bool verify_checksums_;

    // If true, calculate and populate BatchResult::wal_record_checksum.
    // Default: false
    bool include_wal_record_checksum_;

    ReadOptions()
        : verify_checksums_(true), include_wal_record_checksum_(false) {}

    explicit ReadOptions(bool verify_checksums)
        : verify_checksums_(verify_checksums),
          include_wal_record_checksum_(false) {}
  };
};
}  // namespace ROCKSDB_NAMESPACE
