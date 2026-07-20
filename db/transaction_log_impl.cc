//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/transaction_log_impl.h"

#include <cinttypes>

#include "db/write_batch_internal.h"
#include "file/sequence_file_reader.h"
#include "util/coding.h"
#include "util/defer.h"
#include "util/xxhash.h"

namespace ROCKSDB_NAMESPACE {

TransactionLogIteratorImpl::TransactionLogIteratorImpl(
    const std::string& dir, const ImmutableDBOptions* options,
    const TransactionLogIterator::ReadOptions& read_options,
    const EnvOptions& soptions, const SequenceNumber seq,
    std::unique_ptr<VectorWalPtr> files, VersionSet const* const versions,
    const bool seq_per_batch, const std::shared_ptr<IOTracer>& io_tracer)
    : dir_(dir),
      options_(options),
      read_options_(read_options),
      soptions_(soptions),
      starting_sequence_number_(seq),
      files_(std::move(files)),
      versions_(versions),
      seq_per_batch_(seq_per_batch),
      io_tracer_(io_tracer),
      started_(false),
      is_valid_(false),
      current_file_index_(0),
      current_batch_seq_(0),
      current_last_seq_(0),
      current_batch_wal_file_number_(0),
      current_batch_wal_record_offset_(0),
      current_batch_wal_record_end_(0),
      current_record_wal_file_number_(0),
      current_record_wal_record_offset_(0),
      current_record_wal_record_end_(0),
      has_unpublished_record_(false),
      unpublished_record_wal_file_number_(0),
      unpublished_record_wal_record_offset_(0),
      unpublished_record_wal_record_end_(0) {
  assert(files_ != nullptr);
  assert(versions_ != nullptr);
  assert(!seq_per_batch_);
  current_status_.PermitUncheckedError();  // Clear on start
  reporter_.env = options_->env;
  reporter_.info_log = options_->info_log.get();
  SeekToStartSequence();  // Seek till starting sequence
}

Status TransactionLogIteratorImpl::OpenLogFile(
    const WalFile* log_file,
    std::unique_ptr<SequentialFileReader>* file_reader) {
  FileSystemPtr fs(options_->fs, io_tracer_);
  std::unique_ptr<FSSequentialFile> file;
  std::string fname;
  Status s;
  EnvOptions optimized_env_options = fs->OptimizeForLogRead(soptions_);
  if (log_file->Type() == kArchivedLogFile) {
    fname = ArchivedLogFileName(dir_, log_file->LogNumber());
    s = fs->NewSequentialFile(fname, optimized_env_options, &file, nullptr);
  } else {
    fname = LogFileName(dir_, log_file->LogNumber());
    s = fs->NewSequentialFile(fname, optimized_env_options, &file, nullptr);
    if (!s.ok()) {
      //  If cannot open file in DB directory.
      //  Try the archive dir, as it could have moved in the meanwhile.
      fname = ArchivedLogFileName(dir_, log_file->LogNumber());
      s = fs->NewSequentialFile(fname, optimized_env_options, &file, nullptr);
    }
  }
  if (s.ok()) {
    file_reader->reset(new SequentialFileReader(std::move(file), fname,
                                                io_tracer_, options_->listeners,
                                                options_->rate_limiter.get()));
  }
  return s;
}

BatchResult TransactionLogIteratorImpl::GetBatch() {
  assert(is_valid_);  //  cannot call in a non valid state.
  BatchResult result;
  result.sequence = current_batch_seq_;
  result.writeBatchPtr = std::move(current_batch_);
  return result;
}

Status TransactionLogIteratorImpl::GetBatchPosition(
    TransactionLogPositionV1* position, bool include_checksum) const {
  if (position == nullptr) {
    return Status::InvalidArgument("Transaction log position is null");
  }
  if (!is_valid_ || !current_status_.ok() || current_batch_ == nullptr) {
    return Status::InvalidArgument(
        "Transaction log iterator is not positioned at a batch");
  }

  position->wal_file_number = current_batch_wal_file_number_;
  position->wal_record_offset = current_batch_wal_record_offset_;
  position->wal_record_end = current_batch_wal_record_end_;
  position->wal_record_checksum = 0;
  if (include_checksum) {
    const Slice contents = WriteBatchInternal::Contents(current_batch_.get());
    position->wal_record_checksum =
        XXH3_64bits(contents.data(), contents.size());
  }
  return Status::OK();
}

Status GetTransactionLogIteratorBatchPositionV1(
    const TransactionLogIterator* iterator,
    TransactionLogPositionV1* position, bool include_checksum) {
  if (iterator == nullptr) {
    return Status::InvalidArgument("Transaction log iterator is null");
  }
  return static_cast<const TransactionLogIteratorImpl*>(iterator)
      ->GetBatchPosition(position, include_checksum);
}

Status TransactionLogIteratorImpl::status() { return current_status_; }

bool TransactionLogIteratorImpl::Valid() { return started_ && is_valid_; }

bool TransactionLogIteratorImpl::IsRecordPublished(const Slice& record) const {
  if (record.size() < WriteBatchInternal::kHeader) {
    // Let the caller preserve the existing corruption-reporting behavior.
    return true;
  }

  const uint64_t count =
      DecodeFixed32(record.data() + sizeof(SequenceNumber));
  if (count == 0) {
    return true;
  }

  const SequenceNumber sequence = DecodeFixed64(record.data());
  const SequenceNumber published_sequence = versions_->LastSequence();
  return sequence <= published_sequence &&
         count - 1 <= published_sequence - sequence;
}

bool TransactionLogIteratorImpl::RestrictedRead(Slice* record) {
  if (has_unpublished_record_) {
    Slice unpublished_record(unpublished_record_);
    if (!IsRecordPublished(unpublished_record)) {
      return false;
    }

    scratch_ = std::move(unpublished_record_);
    has_unpublished_record_ = false;
    *record = Slice(scratch_);
    current_record_wal_file_number_ = unpublished_record_wal_file_number_;
    current_record_wal_record_offset_ = unpublished_record_wal_record_offset_;
    current_record_wal_record_end_ = unpublished_record_wal_record_end_;
    return true;
  }

  if (!current_log_reader_->ReadRecord(
          record, &scratch_,
          WALRecoveryMode::kTolerateCorruptedTailRecords)) {
    return false;
  }

  current_record_wal_file_number_ = current_log_reader_->GetLogNumber();
  current_record_wal_record_offset_ = current_log_reader_->LastRecordOffset();
  current_record_wal_record_end_ = current_log_reader_->LastRecordEnd();

  if (!IsRecordPublished(*record)) {
    unpublished_record_.assign(record->data(), record->size());
    has_unpublished_record_ = true;
    unpublished_record_wal_file_number_ = current_record_wal_file_number_;
    unpublished_record_wal_record_offset_ = current_record_wal_record_offset_;
    unpublished_record_wal_record_end_ = current_record_wal_record_end_;
    return false;
  }

  return true;
}

void TransactionLogIteratorImpl::SeekToStartSequence(uint64_t start_file_index,
                                                     bool strict) {
  Slice record;
  started_ = false;
  is_valid_ = false;
  // Check invariant of TransactionLogIterator when SeekToStartSequence()
  // succeeds.
  const Defer defer([this]() {
    if (is_valid_) {
      assert(current_status_.ok());
      if (starting_sequence_number_ > current_batch_seq_) {
        assert(current_batch_seq_ < current_last_seq_);
        assert(current_last_seq_ >= starting_sequence_number_);
      }
    }
  });
  if (files_->size() <= start_file_index) {
    return;
  } else if (!current_status_.ok()) {
    return;
  }
  if (!has_unpublished_record_) {
    Status s =
        OpenLogReader(files_->at(static_cast<size_t>(start_file_index)).get());
    if (!s.ok()) {
      current_status_ = s;
      reporter_.Info(current_status_.ToString().c_str());
      return;
    }
  }
  while (RestrictedRead(&record)) {
    if (record.size() < WriteBatchInternal::kHeader) {
      reporter_.Corruption(record.size(),
                           Status::Corruption("very small log record"));
      continue;
    }
    UpdateCurrentWriteBatch(record);
    if (current_last_seq_ >= starting_sequence_number_) {
      if (strict && current_batch_seq_ != starting_sequence_number_) {
        current_status_ = Status::Corruption(
            "Gap in sequence number. Could not "
            "seek to required sequence number");
        reporter_.Info(current_status_.ToString().c_str());
        return;
      } else if (strict) {
        reporter_.Info(
            "Could seek required sequence number. Iterator will "
            "continue.");
      }
      is_valid_ = true;
      started_ = true;  // set started_ as we could seek till starting sequence
      return;
    } else {
      is_valid_ = false;
    }
  }

  if (has_unpublished_record_) {
    current_status_ = Status::OK();
    return;
  }

  // Could not find start sequence in first file. Normally this must be the
  // only file. Otherwise log the error and let the iterator return next entry
  // If strict is set, we want to seek exactly till the start sequence and it
  // should have been present in the file we scanned above
  if (strict) {
    current_status_ = Status::Corruption(
        "Gap in sequence number. Could not "
        "seek to required sequence number");
    reporter_.Info(current_status_.ToString().c_str());
  } else if (files_->size() != 1) {
    current_status_ = Status::Corruption(
        "Start sequence was not found, "
        "skipping to the next available");
    reporter_.Info(current_status_.ToString().c_str());
    // Let NextImpl find the next available entry. started_ remains false
    // because we don't want to check for gaps while moving to start sequence
    NextImpl(true);
  }
}

void TransactionLogIteratorImpl::Next() {
  if (!current_status_.ok()) {
    return;
  }
  return NextImpl(false);
}

void TransactionLogIteratorImpl::NextImpl(bool internal) {
  Slice record;
  is_valid_ = false;
  if (!internal && !started_) {
    // Runs every time until we can seek to the start sequence
    SeekToStartSequence();
  }
  while (true) {
    assert(current_log_reader_);
    if (current_log_reader_->IsEOF()) {
      current_log_reader_->UnmarkEOF();
    }
    while (RestrictedRead(&record)) {
      if (record.size() < WriteBatchInternal::kHeader) {
        reporter_.Corruption(record.size(),
                             Status::Corruption("very small log record"));
        continue;
      } else {
        // started_ should be true if called by application
        assert(internal || started_);
        // started_ should be false if called internally
        assert(!internal || !started_);
        UpdateCurrentWriteBatch(record);
        if (internal && !started_) {
          started_ = true;
        }
        return;
      }
    }

    if (has_unpublished_record_) {
      // A complete record has reached the WAL, but its sequence range is not
      // published yet. Keep the iterator resumable at this exact reader
      // position instead of opening another WAL or reporting a false gap.
      is_valid_ = false;
      current_status_ = Status::OK();
      return;
    }

    // Open the next file
    if (current_file_index_ < files_->size() - 1) {
      ++current_file_index_;
      Status s = OpenLogReader(files_->at(current_file_index_).get());
      if (!s.ok()) {
        is_valid_ = false;
        current_status_ = s;
        return;
      }
    } else {
      is_valid_ = false;
      if (current_last_seq_ == versions_->LastSequence()) {
        current_status_ = Status::OK();
      } else {
        const char* msg = "Create a new iterator to fetch the new tail.";
        current_status_ = Status::TryAgain(msg);
      }
      return;
    }
  }
}

bool TransactionLogIteratorImpl::IsBatchExpected(
    const WriteBatch* batch, const SequenceNumber expected_seq) {
  assert(batch);
  SequenceNumber batchSeq = WriteBatchInternal::Sequence(batch);
  if (batchSeq != expected_seq) {
    std::ostringstream oss;
    oss << "Discontinuity in log records. " << "Got seq=" << batchSeq << ", "
        << "Expected seq=" << expected_seq << ", "
        << "Last flushed seq=" << versions_->LastSequence() << ". "
        << "Log iterator will reseek the correct batch.";

    reporter_.Info(oss.str().c_str());
    return false;
  }
  return true;
}

void TransactionLogIteratorImpl::UpdateCurrentWriteBatch(const Slice& record) {
  std::unique_ptr<WriteBatch> batch(new WriteBatch());
  Status s = WriteBatchInternal::SetContents(batch.get(), record);
  s.PermitUncheckedError();  // TODO: What should we do with this error?

  SequenceNumber expected_seq = current_last_seq_ + 1;
  // If the iterator has started, then confirm that we get continuous batches
  if (started_ && !IsBatchExpected(batch.get(), expected_seq)) {
    // Seek to the batch having expected sequence number
    if (expected_seq < files_->at(current_file_index_)->StartSequence()) {
      // Expected batch must lie in the previous log file
      // Avoid underflow.
      if (current_file_index_ != 0) {
        current_file_index_--;
      }
    }
    starting_sequence_number_ = expected_seq;
    // currentStatus_ will be set to Ok if reseek succeeds
    // Note: this is still ok in seq_pre_batch_ && two_write_queuesp_ mode
    // that allows gaps in the WAL since it will still skip over the gap.
    current_status_ = Status::NotFound("Gap in sequence numbers");
    // In seq_per_batch_ mode, gaps in the seq are possible so the strict mode
    // should be disabled
    return SeekToStartSequence(current_file_index_, !seq_per_batch_);
  }

  current_batch_seq_ = WriteBatchInternal::Sequence(batch.get());
  assert(!seq_per_batch_);
  current_last_seq_ =
      current_batch_seq_ + WriteBatchInternal::Count(batch.get()) - 1;
  // currentBatchSeq_ can only change here
  assert(current_last_seq_ <= versions_->LastSequence());

  current_batch_ = std::move(batch);
  current_batch_wal_file_number_ = current_record_wal_file_number_;
  current_batch_wal_record_offset_ = current_record_wal_record_offset_;
  current_batch_wal_record_end_ = current_record_wal_record_end_;
  is_valid_ = true;
  current_status_ = Status::OK();
}

Status TransactionLogIteratorImpl::OpenLogReader(const WalFile* log_file) {
  std::unique_ptr<SequentialFileReader> file;
  Status s = OpenLogFile(log_file, &file);
  if (!s.ok()) {
    return s;
  }
  assert(file);
  current_log_reader_.reset(
      new log::Reader(options_->info_log, std::move(file), &reporter_,
                      read_options_.verify_checksums_, log_file->LogNumber()));
  return Status::OK();
}
}  // namespace ROCKSDB_NAMESPACE
