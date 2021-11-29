// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include <deque>

#include "txn/lock_manager.h"

using std::deque;

LockManager::~LockManager() {
  // Cleanup lock_table_
  for (auto it = lock_table_.begin(); it != lock_table_.end(); it++) {
    delete it->second;
  }
}

deque<LockManager::LockRequest>* LockManager::_getLockQueue(const Key& key) {
  deque<LockRequest> *dq = lock_table_[key];
  if (!dq) {
    dq = new deque<LockRequest>();
    lock_table_[key] = dq;
  }
  return dq;
}

LockManagerA::LockManagerA(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerA::WriteLock(Txn* txn, const Key& key) {
  bool empty = true;
  LockRequest lr(EXCLUSIVE, txn);
  deque<LockRequest> *dq = _getLockQueue(key);

  empty = dq->empty();
  dq->push_back(lr);

  if (!empty) { // Add to waiting list
    // Apabila key txn pada map kosong, tambahkan
    // Else, tambah counter
    if (txn_waits_.find(txn) == txn_waits_.end()) {
      txn_waits_[txn] = 1;
    } else {
      txn_waits_[txn]++;
    }
  }

  return empty;
}

bool LockManagerA::ReadLock(Txn* txn, const Key& key) {
  // Karena hanya memakai exclusive lock,
  // read lock sama seperti write lock
  return WriteLock(txn, key);
}

void LockManagerA::Release(Txn* txn, const Key& key) {
  deque<LockRequest> *queue = _getLockQueue(key);
  
  // Cek apakah elemen pertama merupakan lock owner dari key
  // Apabila tidak, traversal queue dan hapus elemen txn
  // Apabila iya, pop elemen pertama dan push front dari queue
  // ke queue ready_txns_ dan hapus dari waiting map
  if (queue->front().txn_ == txn) {
    queue->pop_front();

    if (queue->size() > 0) {
      LockRequest next = queue->front();
      if (--txn_waits_[next.txn_] == 0) {
        ready_txns_->push_back(next.txn_);
        txn_waits_.erase(next.txn_);
      } 
    }
  } else {
    for (auto it = queue->begin(); it < queue->end(); it++) {
      if (it->txn_ == txn) {
        queue->erase(it);
        break;
      }
    }
  }

}

LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners) {
  deque<LockRequest> *dq = _getLockQueue(key);

  // Hapus semua elemen vector owners
  owners->clear();

  if (dq->size() > 0) {
    owners->push_back(dq->front().txn_);
  }
  
  if (dq->empty()) {
    return UNLOCKED;
  } else {
    return EXCLUSIVE;
  }
}

LockManagerB::LockManagerB(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerB::_addLock(LockMode mode, Txn* txn, const Key& key) {
  LockRequest rq(mode, txn);
  LockMode status = Status(key, nullptr);

  deque<LockRequest> *dq = _getLockQueue(key);
  dq->push_back(rq);

  bool granted = status == UNLOCKED;
  if (mode == SHARED) {
    granted |= _noExclusiveWaiting(key);
  } else {
    _numExclusiveWaiting[key]++;
  }

  if (!granted)
    txn_waits_[txn]++;

  return granted;
}


bool LockManagerB::WriteLock(Txn* txn, const Key& key) {
  return _addLock(EXCLUSIVE, txn, key);
}

bool LockManagerB::ReadLock(Txn* txn, const Key& key) {
  return _addLock(SHARED, txn, key);
}

void LockManagerB::Release(Txn* txn, const Key& key) {
  deque<LockRequest> *queue = _getLockQueue(key);

  for (auto it = queue->begin(); it < queue->end(); it++) {
    if (it->txn_ == txn) {
      queue->erase(it);
      if (it->mode_ == EXCLUSIVE) {
        _numExclusiveWaiting[key]--;
      }

      break;
    }
  }

  // Advance the lock, by making new owners ready.
  // Some in newOwners already own the lock.  These are not in
  // txn_waits_.
  vector<Txn*> newOwners;
  Status(key, &newOwners);

  for (auto&& owner : newOwners) {
    auto waitCount = txn_waits_.find(owner);
    if (waitCount != txn_waits_.end() && --(waitCount->second) == 0) {
      ready_txns_->push_back(owner);
      txn_waits_.erase(waitCount);
    }
  }
}

LockMode LockManagerB::Status(const Key& key, vector<Txn*>* owners) {
  deque<LockRequest> *dq = _getLockQueue(key);
  if (dq->empty()) {
    return UNLOCKED;
  }

  LockMode mode = EXCLUSIVE;
  vector<Txn*> txn_owners;
  for (auto&& lockRequest : *dq) {
    if (lockRequest.mode_ == EXCLUSIVE && mode == SHARED)
        break;

    txn_owners.push_back(lockRequest.txn_);
    mode = lockRequest.mode_;

    if (mode == EXCLUSIVE)
      break;
  }

  if (owners)
    *owners = txn_owners;

  return mode;
}

inline bool LockManagerB::_noExclusiveWaiting(const Key& key) {
  return _numExclusiveWaiting[key] == 0;
}
