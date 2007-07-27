/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
#ifndef HYPERTABLE_ACCESSGROUP_H
#define HYPERTABLE_ACCESSGROUP_H

#include <queue>
#include <string>
#include <vector>

#include "Hypertable/Schema.h"

#include "CellCache.h"
#include "CellStore.h"
#include "RangeInfo.h"

using namespace hypertable;

namespace hypertable {

  class AccessGroup : CellList {

  public:

    typedef struct {
      KeyT     *key;
      uint64_t timestamp;
    } SplitKeyInfoT;

    struct ltSplitKeyInfo {
      bool operator()(const SplitKeyInfoT &ski1, const SplitKeyInfoT &ski2) const {
	return ski1.timestamp < ski2.timestamp;
      }
    };

    typedef std::priority_queue<SplitKeyInfoT, vector<SplitKeyInfoT>, ltSplitKeyInfo> SplitKeyQueueT;
    
    AccessGroup(Schema::AccessGroup *lg, RangeInfoPtr &tabletInfoPtr);
    virtual ~AccessGroup();
    virtual int Add(const KeyT *key, const ByteString32T *value);
    virtual CellListScanner *CreateScanner(bool suppressDeleted);
    virtual void Lock() { mRwMutex.lock(); }
    virtual void Unlock() { mRwMutex.unlock(); }
    virtual void LockShareable() { mRwMutex.lock_shareable(); }
    virtual void UnlockShareable() { mRwMutex.unlock_shareable(); }
    virtual void GetSplitKeys(SplitKeyQueueT &keyHeap);

    bool FamiliesIntersect(std::set<uint8_t> &families);
    uint64_t DiskUsage();
    void AddCellStore(CellStore *table, uint32_t id);
    bool NeedsCompaction();
    void RunCompaction(uint64_t timestamp, bool major);
    uint64_t GetLogCutoffTime() { return mLogCutoffTime; }
    void MarkBusy();
    void UnmarkBusy();

  private:
    boost::read_write_mutex  mRwMutex;
    std::set<uint8_t>    mColumnFamilies;
    std::string          mName;
    std::string          mTableName;
    std::string          mStartRow;
    std::string          mEndRow;
    std::vector<CellStore *> mStores;
    CellCache            *mCellCache;
    uint32_t             mNextTableId;
    uint64_t             mLogCutoffTime;
    bool                 mBusy;
    uint64_t             mDiskUsage;
    std::vector<KeyPtr>  mSplitKeys;
  };

}

#endif // HYPERTABLE_ACCESSGROUP_H