//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// Introduction of SyncPoint effectively disabled building and running this test
// in Release build.
// which is a pity, it is a good test

#include <atomic>
#include <chrono>
#include <thread>

#include "db/db_test_util.h"
#include "env/mock_env.h"
#include "port/stack_trace.h"
#include "util/atomic.h"
#include "util/defer.h"

namespace ROCKSDB_NAMESPACE {

class DBTestXactLogIterator : public DBTestBase {
 public:
  DBTestXactLogIterator()
      : DBTestBase("db_log_iter_test", /*env_do_fsync=*/true) {}

  std::unique_ptr<TransactionLogIterator> OpenTransactionLogIter(
      const SequenceNumber seq, const bool include_wal_record_checksum = false) {
    std::unique_ptr<TransactionLogIterator> iter;
    TransactionLogIterator::ReadOptions read_options;
    read_options.include_wal_record_checksum_ = include_wal_record_checksum;
    Status status = dbfull()->GetUpdatesSince(seq, &iter, read_options);
    EXPECT_OK(status);
    EXPECT_TRUE(iter->Valid());
    return iter;
  }
};

namespace {
SequenceNumber ReadRecords(std::unique_ptr<TransactionLogIterator>& iter,
                           int& count, bool expect_ok = true) {
  count = 0;
  SequenceNumber lastSequence = 0;
  BatchResult res;
  while (iter->Valid()) {
    res = iter->GetBatch();
    EXPECT_TRUE(res.sequence > lastSequence);
    ++count;
    lastSequence = res.sequence;
    EXPECT_OK(iter->status());
    iter->Next();
  }
  if (expect_ok) {
    EXPECT_OK(iter->status());
  } else {
    EXPECT_NOK(iter->status());
  }
  return res.sequence;
}

void ExpectRecords(const int expected_no_records,
                   std::unique_ptr<TransactionLogIterator>& iter) {
  int num_records;
  ReadRecords(iter, num_records);
  ASSERT_EQ(num_records, expected_no_records);
}
}  // anonymous namespace

TEST_F(DBTestXactLogIterator, TransactionLogIterator) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);
    ASSERT_OK(Put(0, "key1", DummyString(1024)));
    ASSERT_OK(Put(1, "key2", DummyString(1024)));
    ASSERT_OK(Put(1, "key2", DummyString(1024)));
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 3U);
    {
      auto iter = OpenTransactionLogIter(0);
      ExpectRecords(3, iter);
    }
    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    env_->SleepForMicroseconds(2 * 1000 * 1000);
    {
      ASSERT_OK(Put(0, "key4", DummyString(1024)));
      ASSERT_OK(Put(1, "key5", DummyString(1024)));
      ASSERT_OK(Put(0, "key6", DummyString(1024)));
    }
    {
      auto iter = OpenTransactionLogIter(0);
      ExpectRecords(6, iter);
    }
  } while (ChangeCompactOptions());
}

#ifndef NDEBUG  // sync point is not included with DNDEBUG build
TEST_F(DBTestXactLogIterator, TransactionLogIteratorRace) {
  static const int LOG_ITERATOR_RACE_TEST_COUNT = 2;
  static const char* sync_points[LOG_ITERATOR_RACE_TEST_COUNT][4] = {
      {"WalManager::GetSortedWalFiles:1", "WalManager::PurgeObsoleteFiles:1",
       "WalManager::PurgeObsoleteFiles:2", "WalManager::GetSortedWalFiles:2"},
      {"WalManager::GetSortedWalsOfType:1", "WalManager::PurgeObsoleteFiles:1",
       "WalManager::PurgeObsoleteFiles:2",
       "WalManager::GetSortedWalsOfType:2"}};
  for (int test = 0; test < LOG_ITERATOR_RACE_TEST_COUNT; ++test) {
    // Setup sync point dependency to reproduce the race condition of
    // a log file moved to archived dir, in the middle of GetSortedWalFiles
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
        {sync_points[test][0], sync_points[test][1]},
        {sync_points[test][2], sync_points[test][3]},
    });

    do {
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearTrace();
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
      Options options = OptionsForLogIterTest();
      DestroyAndReopen(options);
      ASSERT_OK(Put("key1", DummyString(1024)));
      ASSERT_OK(dbfull()->Flush(FlushOptions()));
      ASSERT_OK(Put("key2", DummyString(1024)));
      ASSERT_OK(dbfull()->Flush(FlushOptions()));
      ASSERT_OK(Put("key3", DummyString(1024)));
      ASSERT_OK(dbfull()->Flush(FlushOptions()));
      ASSERT_OK(Put("key4", DummyString(1024)));
      ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 4U);
      ASSERT_OK(dbfull()->FlushWAL(false));

      {
        auto iter = OpenTransactionLogIter(0);
        ExpectRecords(4, iter);
      }

      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
      // trigger async flush, and log move. Well, log move will
      // wait until the GetSortedWalFiles:1 to reproduce the race
      // condition
      FlushOptions flush_options;
      flush_options.wait = false;
      ASSERT_OK(dbfull()->Flush(flush_options));

      // "key5" would be written in a new memtable and log
      ASSERT_OK(Put("key5", DummyString(1024)));
      ASSERT_OK(dbfull()->FlushWAL(false));
      {
        // this iter would miss "key4" if not fixed
        auto iter = OpenTransactionLogIter(0);
        ExpectRecords(5, iter);
      }
    } while (ChangeCompactOptions());
  }
}

TEST_F(DBTestXactLogIterator, TransactionLogIteratorCheckWhenArchive) {
  RelaxedAtomic<bool> callback_hit{};
  do {
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearTrace();
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(options);
    ColumnFamilyHandle* cf;
    auto s = dbfull()->CreateColumnFamily(ColumnFamilyOptions(), "CF", &cf);
    ASSERT_TRUE(s.ok());

    ASSERT_OK(dbfull()->Put(WriteOptions(), cf, "key1", DummyString(1024)));

    ASSERT_OK(dbfull()->Put(WriteOptions(), "key2", DummyString(1024)));

    ASSERT_OK(dbfull()->Flush(FlushOptions()));

    ASSERT_OK(dbfull()->Put(WriteOptions(), "key3", DummyString(1024)));

    ASSERT_OK(dbfull()->Flush(FlushOptions()));

    ASSERT_OK(dbfull()->Put(WriteOptions(), "key4", DummyString(1024)));
    ASSERT_OK(dbfull()->Flush(FlushOptions()));

    callback_hit.StoreRelaxed(false);
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "WalManager::PurgeObsoleteFiles:1", [&](void*) {
          auto iter = OpenTransactionLogIter(0);
          ExpectRecords(4, iter);
          callback_hit.StoreRelaxed(true);
        });

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
    ASSERT_OK(dbfull()->Flush(FlushOptions(), cf));
    // Try lots of things to ensure callback is triggered
    ASSERT_OK(dbfull()->TEST_SwitchWAL());
    ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
    ASSERT_OK(dbfull()->TEST_WaitForPurge());
    delete cf;
    ASSERT_TRUE(callback_hit.LoadRelaxed());
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
    Close();
  } while (ChangeCompactOptions());
}
#endif

TEST_F(DBTestXactLogIterator, TransactionLogIteratorStallAtLastRecord) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(options);
    ASSERT_OK(Put("key1", DummyString(1024)));
    auto iter = OpenTransactionLogIter(0);
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    iter->Next();
    ASSERT_TRUE(!iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_OK(Put("key2", DummyString(1024)));
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
  } while (ChangeCompactOptions());
}

TEST_F(DBTestXactLogIterator, TransactionLogIteratorCheckAfterRestart) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(options);
    ASSERT_OK(Put("key1", DummyString(1024)));
    ASSERT_OK(Put("key2", DummyString(1023)));
    ASSERT_OK(dbfull()->Flush(FlushOptions()));
    Reopen(options);
    auto iter = OpenTransactionLogIter(0);
    ExpectRecords(2, iter);
  } while (ChangeCompactOptions());
}

TEST_F(DBTestXactLogIterator, TransactionLogIteratorCorruptedLog) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(options);

    for (int i = 0; i < 1024; i++) {
      ASSERT_OK(Put("key" + std::to_string(i), DummyString(10)));
    }

    ASSERT_OK(Flush());
    ASSERT_OK(db_->FlushWAL(false));

    // Corrupt this log to create a gap
    ASSERT_OK(db_->DisableFileDeletions());

    VectorLogPtr wal_files;
    ASSERT_OK(db_->GetSortedWalFiles(wal_files));
    ASSERT_FALSE(wal_files.empty());

    const auto logfile_path = dbname_ + "/" + wal_files.front()->PathName();
    ASSERT_OK(test::TruncateFile(env_, logfile_path,
                                 wal_files.front()->SizeFileBytes() / 2));

    ASSERT_OK(db_->EnableFileDeletions());

    // Insert a new entry to a new log file
    ASSERT_OK(Put("key1025", DummyString(10)));
    ASSERT_OK(db_->FlushWAL(false));

    // Try to read from the beginning. Should stop before the gap and read less
    // than 1025 entries
    auto iter = OpenTransactionLogIter(0);
    int count = 0;
    SequenceNumber last_sequence_read = ReadRecords(iter, count, false);
    ASSERT_LT(last_sequence_read, 1025U);

    // Try to read past the gap, should be able to seek to key1025
    auto iter2 = OpenTransactionLogIter(last_sequence_read + 1);
    ExpectRecords(1, iter2);
  } while (ChangeCompactOptions());
}

TEST_F(DBTestXactLogIterator, TransactionLogIteratorBatchOperations) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);
    WriteBatch batch;
    ASSERT_OK(batch.Put(handles_[1], "key1", DummyString(1024)));
    ASSERT_OK(batch.Put(handles_[0], "key2", DummyString(1024)));
    ASSERT_OK(batch.Put(handles_[1], "key3", DummyString(1024)));
    ASSERT_OK(batch.Delete(handles_[0], "key2"));
    ASSERT_OK(dbfull()->Write(WriteOptions(), &batch));
    ASSERT_OK(Flush(1));
    ASSERT_OK(Flush(0));
    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    ASSERT_OK(Put(1, "key4", DummyString(1024)));
    auto iter = OpenTransactionLogIter(3);
    ExpectRecords(2, iter);
  } while (ChangeCompactOptions());
}

TEST_F(DBTestXactLogIterator, TransactionLogIteratorBlobs) {
  Options options = OptionsForLogIterTest();
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);
  {
    WriteBatch batch;
    ASSERT_OK(batch.Put(handles_[1], "key1", DummyString(1024)));
    ASSERT_OK(batch.Put(handles_[0], "key2", DummyString(1024)));
    ASSERT_OK(batch.PutLogData(Slice("blob1")));
    ASSERT_OK(batch.Put(handles_[1], "key3", DummyString(1024)));
    ASSERT_OK(batch.PutLogData(Slice("blob2")));
    ASSERT_OK(batch.Delete(handles_[0], "key2"));
    ASSERT_OK(dbfull()->Write(WriteOptions(), &batch));
    ReopenWithColumnFamilies({"default", "pikachu"}, options);
  }

  auto res = OpenTransactionLogIter(0)->GetBatch();
  struct Handler : public WriteBatch::Handler {
    std::string seen;
    Status PutCF(uint32_t cf, const Slice& key, const Slice& value) override {
      seen += "Put(" + std::to_string(cf) + ", " + key.ToString() + ", " +
              std::to_string(value.size()) + ")";
      return Status::OK();
    }
    Status MergeCF(uint32_t cf, const Slice& key, const Slice& value) override {
      seen += "Merge(" + std::to_string(cf) + ", " + key.ToString() + ", " +
              std::to_string(value.size()) + ")";
      return Status::OK();
    }
    void LogData(const Slice& blob) override {
      seen += "LogData(" + blob.ToString() + ")";
    }
    Status DeleteCF(uint32_t cf, const Slice& key) override {
      seen += "Delete(" + std::to_string(cf) + ", " + key.ToString() + ")";
      return Status::OK();
    }
  } handler;
  ASSERT_OK(res.writeBatchPtr->Iterate(&handler));
  ASSERT_EQ(
      "Put(1, key1, 1024)"
      "Put(0, key2, 1024)"
      "LogData(blob1)"
      "Put(1, key3, 1024)"
      "LogData(blob2)"
      "Delete(0, key2)",
      handler.seen);
}

TEST_F(DBTestXactLogIterator,
       TransactionLogIteratorLogDataOnlyTailAndWalPosition) {
  Options options = OptionsForLogIterTest();
  DestroyAndReopen(options);
  ASSERT_OK(Put("key1", "value1"));

  auto default_iter = OpenTransactionLogIter(0);
  ASSERT_EQ(default_iter->GetBatch().wal_record_checksum, 0U);

  auto iter = OpenTransactionLogIter(0, true);
  BatchResult initial = iter->GetBatch();
  ASSERT_EQ(initial.writeBatchPtr->Count(), 1U);
  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());

  WriteBatch first_log_data;
  ASSERT_OK(first_log_data.PutLogData(Slice("log-data-1")));
  ASSERT_OK(dbfull()->Write(WriteOptions(), &first_log_data));

  iter->Next();
  ASSERT_TRUE(iter->Valid());
  BatchResult first = iter->GetBatch();
  ASSERT_EQ(first.sequence, 2U);
  ASSERT_EQ(first.writeBatchPtr->Count(), 0U);
  ASSERT_GT(first.wal_file_number, 0U);
  ASSERT_LT(first.wal_record_offset, first.wal_record_end);

  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());

  WriteBatch second_log_data;
  ASSERT_OK(second_log_data.PutLogData(Slice("log-data-2")));
  ASSERT_OK(dbfull()->Write(WriteOptions(), &second_log_data));

  iter->Next();
  ASSERT_TRUE(iter->Valid());
  BatchResult second = iter->GetBatch();
  ASSERT_EQ(second.sequence, 2U);
  ASSERT_EQ(second.writeBatchPtr->Count(), 0U);
  ASSERT_EQ(first.wal_file_number, second.wal_file_number);
  ASSERT_LT(first.wal_record_offset, second.wal_record_offset);
  ASSERT_LE(first.wal_record_end, second.wal_record_offset);
  ASSERT_NE(first.wal_record_checksum, second.wal_record_checksum);

  const uint64_t first_file_number = first.wal_file_number;
  const uint64_t first_offset = first.wal_record_offset;
  const uint64_t first_end = first.wal_record_end;
  const uint64_t first_checksum = first.wal_record_checksum;
  const uint64_t second_file_number = second.wal_file_number;
  const uint64_t second_offset = second.wal_record_offset;
  const uint64_t second_end = second.wal_record_end;
  const uint64_t second_checksum = second.wal_record_checksum;

  BatchResult move_constructed(std::move(first));
  ASSERT_EQ(move_constructed.wal_file_number, first_file_number);
  ASSERT_EQ(move_constructed.wal_record_offset, first_offset);
  ASSERT_EQ(move_constructed.wal_record_end, first_end);
  ASSERT_EQ(move_constructed.wal_record_checksum, first_checksum);

  BatchResult moved;
  moved = std::move(second);
  ASSERT_EQ(moved.wal_file_number, second_file_number);
  ASSERT_EQ(moved.wal_record_offset, second_offset);
  ASSERT_EQ(moved.wal_record_end, second_end);
  ASSERT_EQ(moved.wal_record_checksum, second_checksum);

  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 1U);

  auto replay = OpenTransactionLogIter(0, true);
  ASSERT_EQ(replay->GetBatch().writeBatchPtr->Count(), 1U);
  replay->Next();
  ASSERT_TRUE(replay->Valid());
  BatchResult replay_first = replay->GetBatch();
  ASSERT_EQ(replay_first.wal_file_number, first_file_number);
  ASSERT_EQ(replay_first.wal_record_offset, first_offset);
  ASSERT_EQ(replay_first.wal_record_end, first_end);
  ASSERT_EQ(replay_first.wal_record_checksum, first_checksum);

  replay->Next();
  ASSERT_TRUE(replay->Valid());
  BatchResult replay_second = replay->GetBatch();
  ASSERT_EQ(replay_second.wal_file_number, second_file_number);
  ASSERT_EQ(replay_second.wal_record_offset, second_offset);
  ASSERT_EQ(replay_second.wal_record_end, second_end);
  ASSERT_EQ(replay_second.wal_record_checksum, second_checksum);
  replay->Next();
  ASSERT_FALSE(replay->Valid());
  ASSERT_OK(replay->status());
}

TEST_F(DBTestXactLogIterator,
       TransactionLogIteratorLogDataOnlyAtInitialSequence) {
  Options options = OptionsForLogIterTest();
  DestroyAndReopen(options);

  WriteBatch first_log_data;
  ASSERT_OK(first_log_data.PutLogData(Slice("log-data-1")));
  ASSERT_OK(dbfull()->Write(WriteOptions(), &first_log_data));
  WriteBatch second_log_data;
  ASSERT_OK(second_log_data.PutLogData(Slice("log-data-2")));
  ASSERT_OK(dbfull()->Write(WriteOptions(), &second_log_data));
  ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 0U);

  auto iter = OpenTransactionLogIter(0, true);
  BatchResult first = iter->GetBatch();
  ASSERT_EQ(first.sequence, 1U);
  ASSERT_EQ(first.writeBatchPtr->Count(), 0U);

  iter->Next();
  ASSERT_TRUE(iter->Valid());
  BatchResult second = iter->GetBatch();
  ASSERT_EQ(second.sequence, 1U);
  ASSERT_EQ(second.writeBatchPtr->Count(), 0U);
  ASSERT_EQ(first.wal_file_number, second.wal_file_number);
  ASSERT_LT(first.wal_record_offset, second.wal_record_offset);
  ASSERT_NE(first.wal_record_checksum, second.wal_record_checksum);

  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
}

#ifndef NDEBUG
TEST_F(DBTestXactLogIterator,
       TransactionLogIteratorDoesNotExposeUnpublishedConsumingBatch) {
  Options options = OptionsForLogIterTest();
  DestroyAndReopen(options);
  ASSERT_OK(Put("key1", "value1"));

  auto iter = OpenTransactionLogIter(0);
  ASSERT_EQ(iter->GetBatch().writeBatchPtr->Count(), 1U);
  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());

  std::atomic<bool> wal_written{false};
  std::atomic<bool> allow_publish{false};
  std::atomic<bool> publish_wait_timed_out{false};
  std::thread writer;
  Defer cleanup([&]() {
    allow_publish.store(true, std::memory_order_release);
    if (writer.joinable()) {
      writer.join();
    }
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::WriteImpl:AfterWriteWAL", [&](void*) {
        wal_written.store(true, std::memory_order_release);
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (!allow_publish.load(std::memory_order_acquire) &&
               std::chrono::steady_clock::now() < deadline) {
          std::this_thread::yield();
        }
        publish_wait_timed_out.store(
            !allow_publish.load(std::memory_order_acquire),
            std::memory_order_release);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Status write_status;
  writer = std::thread([&]() { write_status = Put("key2", "value2"); });
  const auto wal_write_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (!wal_written.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < wal_write_deadline) {
    std::this_thread::yield();
  }
  ASSERT_TRUE(wal_written.load(std::memory_order_acquire));

  iter->Next();
  const bool exposed_before_publish = iter->Valid();
  const Status status_before_publish = iter->status();

  allow_publish.store(true, std::memory_order_release);
  writer.join();

  ASSERT_OK(write_status);
  ASSERT_FALSE(publish_wait_timed_out.load(std::memory_order_acquire));
  ASSERT_FALSE(exposed_before_publish);
  ASSERT_OK(status_before_publish);
  ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 2U);

  iter->Next();
  ASSERT_TRUE(iter->Valid());
  BatchResult published = iter->GetBatch();
  ASSERT_EQ(published.sequence, 2U);
  ASSERT_EQ(published.writeBatchPtr->Count(), 1U);
  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
}
#endif
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
