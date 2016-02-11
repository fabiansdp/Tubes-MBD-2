// Author: Kun Ren (kun.ren@yale.edu)

#include "txn/mvcc_storage.h"

// Init the storage
void MVCCStorage::InitStorage() {
  for (int i = 0; i < 10000;i++) {
    Write(i, 0, 0);
    Mutex* key_mutex = new Mutex();
    mutexs_[i] = key_mutex;
  }
}

// Free memory.
MVCCStorage::~MVCCStorage() {
  for (unordered_map<Key, deque<Version*>*>::iterator it = mvcc_data_.begin();
       it != mvcc_data_.end(); ++it) {
    delete it->second;          
  }
  
  mvcc_data_.clear();
  
  for (unordered_map<Key, Mutex*>::iterator it = mutexs_.begin();
       it != mutexs_.end(); ++it) {
    delete it->second;          
  }
  
  mutexs_.clear();
}

// Lock the key to protect its version_list. Remember to lock the key when you read/update the version_list 
void MVCCStorage::Lock(Key key) {
  mutexs_[key]->Lock();
}

// Unlock the key.
void MVCCStorage::Unlock(Key key) {
  mutexs_[key]->Unlock();
}

// MVCC Read
bool MVCCStorage::Read(Key key, Value* result, int txn_unique_id) {
  // CPSC 438/538:
  //
  // Implement this method!
  
  // Hint: Iterate the version_lists and return the verion whose write timestamp
  // (version_id) is the lagrgest write timestamp less than or equal to txn_unique_id.
  
  return true;
}


// Check whether apply or abort the write
bool MVCCStorage::CheckWrite(Key key, int txn_unique_id) {
  // CPSC 438/538:
  //
  // Implement this method!
  
  // Hint: Before apply write, you should check whether we should apply or abort the write
  // based on MVCC timestamp ordering protocol. Return true if we can apply the
  // write, return false if we should abort the write.
  
  return true;
}

// MVCC Write, call this method only if CheckWrite return true.
void MVCCStorage::Write(Key key, Value value, int txn_unique_id) {
  // CPSC 438/538:
  //
  // Implement this method!
  
  // Hint: Insert a new version(malloc a Version and specify its value/version_id/max_read_id) into the version_lists. Note that InitStorage()
  // also calls this method to init storage.
}


// Iterate the database to erase the old versions
void MVCCStorage::GarbageCollection(int oldest_txn) {
  // CPSC 438/538:
  //
  // Implement this method!
  
  // Hint: Iterate the storage(mvcc_data_) to erase the old versions. The idea
  // is that if the oldest_txn is 9, and the version_lists contains the following verion_ids:
  // 12, 10, 8, 7, 6, 5, 4, 3, 2, 1, and the oldest_txn will never have to read
  // verions from 1 to 7, so we can erase them(and free the memory).
  
}


