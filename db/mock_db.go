package db

import (
	"bytes"
	"sync"

	"github.com/zeniqsmart/db-zeniq-smart-chain/types"
)

type MockDB struct {
	mtx    sync.RWMutex
	blkMap map[int64]types.Block
	height int64
}

var _ types.DB = &MockDB{}

func (d *MockDB) Close() {
}

func (d *MockDB) SetMaxEntryCount(c int) {
}

func (d *MockDB) GetLatestHeight() int64 {
	return d.height
}

func (d *MockDB) GetUtxoInfos() (infos [][57]byte) {
	return nil
}

func (d *MockDB) GetAllUtxoIds() [][36]byte {
	return nil
}

func (d *MockDB) GetRedeemableUtxoIds() [][36]byte {
	return nil
}

func (d *MockDB) SetOpListsForCcUtxo(opListsForCcUtxo types.OpListsForCcUtxo) {}

func (d *MockDB) GetLostAndFoundUtxoIds() [][36]byte {
	return nil
}

func (d *MockDB) GetRedeemingUtxoIds() [][36]byte {
	return nil
}

func (d *MockDB) GetUtxoIdsByCovenantAddr(covenantAddr [20]byte) [][36]byte {
	return nil
}

func (d *MockDB) GetRedeemableUtxoIdsByCovenantAddr(covenantAddr [20]byte) [][36]byte {
	return nil
}

func (d *MockDB) AddBlock(blk *types.Block, pruneTillHeight int64, txid2sigMap map[[32]byte][65]byte) {
	if blk == nil {
		return
	}
	d.mtx.Lock()
	defer d.mtx.Unlock()
	if d.blkMap == nil {
		d.blkMap = make(map[int64]types.Block)
	}
	d.blkMap[blk.Height] = blk.Clone()
	d.height = blk.Height
}

func (d *MockDB) GetBlockHashByHeight(height int64) (res [32]byte) {
	d.mtx.RLock()
	defer d.mtx.RUnlock()
	for _, blk := range d.blkMap {
		if blk.Height == height {
			return blk.BlockHash
		}
	}
	return
}

func (d *MockDB) GetBlockByHeight(height int64) []byte {
	d.mtx.RLock()
	defer d.mtx.RUnlock()
	for _, blk := range d.blkMap {
		if blk.Height == height {
			return blk.BlockInfo
		}
	}
	return nil
}

func (d *MockDB) GetTxByHeightAndIndex(height int64, index int) []byte {
	var sig [65]byte
	d.mtx.RLock()
	defer d.mtx.RUnlock()
	for _, blk := range d.blkMap {
		if blk.Height == height {
			if index >= len(blk.TxList) {
				return nil
			}
			return append(sig[:], blk.TxList[index].Content...)
		}
	}
	return nil
}

func (d *MockDB) GetTxListByHeightWithRange(height int64, start, end int) [][]byte {
	return d.GetTxListByHeight(height)[start:end]
}

func (d *MockDB) GetTxListByHeight(height int64) (res [][]byte) {
	var sig [65]byte
	d.mtx.RLock()
	defer d.mtx.RUnlock()
	for _, blk := range d.blkMap {
		if blk.Height == height {
			res = make([][]byte, len(blk.TxList))
			for i, tx := range blk.TxList {
				res[i] = append(sig[:], tx.Content...)
			}
			break
		}
	}
	return
}

func (d *MockDB) GetBlockByHash(hash [32]byte, collectResult func([]byte) bool) {
	d.mtx.RLock()
	defer d.mtx.RUnlock()
	for _, blk := range d.blkMap {
		if bytes.Equal(blk.BlockHash[:], hash[:]) {
			ok := collectResult(blk.BlockInfo)
			if !ok {
				panic("should be true!")
			}
		}
	}
}

func (d *MockDB) GetTxByHash(hash [32]byte, collectResult func([]byte) bool) {
	var sig [65]byte
	d.mtx.RLock()
	defer d.mtx.RUnlock()
	for _, blk := range d.blkMap {
		for _, tx := range blk.TxList {
			if bytes.Equal(tx.HashId[:], hash[:]) {
				ok := collectResult(append(sig[:], tx.Content...))
				if !ok {
					panic("should be true!")
				}
			}
		}
	}
}

func hasTopic(log types.Log, t [32]byte) bool {
	for _, topic := range log.Topics {
		if bytes.Equal(topic[:], t[:]) {
			return true
		}
	}
	return false
}

func hasAllTopic(log types.Log, topics [][32]byte) bool {
	if len(topics) == 0 {
		return true
	}
	for _, t := range topics {
		if !hasTopic(log, t) {
			return false
		}
	}
	return true
}

func (d *MockDB) BasicQueryLogs(addr *[20]byte, topics [][32]byte, startHeight, endHeight uint32, fn func([]byte) bool) error {
	d.mtx.RLock()
	defer d.mtx.RUnlock()
	for i := int64(startHeight); i < int64(endHeight); i++ {
		blk, ok := d.blkMap[i]
		if !ok {
			continue
		}
		for _, tx := range blk.TxList {
			for _, log := range tx.LogList {
				if addr != nil && !bytes.Equal((*addr)[:], log.Address[:]) {
					continue
				}
				if !hasAllTopic(log, topics) {
					continue
				}
				needMore := fn(tx.Content)
				if !needMore {
					return nil
				}
			}
		}
	}
	return nil
}

func (d *MockDB) QueryTxByDst(addr [20]byte, startHeight, endHeight uint32, fn func([]byte) bool) error {
	d.queryTx(false, true, addr, startHeight, endHeight, fn)
	return nil
}

func (d *MockDB) QueryTxBySrc(addr [20]byte, startHeight, endHeight uint32, fn func([]byte) bool) error {
	d.queryTx(true, false, addr, startHeight, endHeight, fn)
	return nil
}

func (d *MockDB) QueryTxBySrcOrDst(addr [20]byte, startHeight, endHeight uint32, fn func([]byte) bool) error {
	d.queryTx(true, true, addr, startHeight, endHeight, fn)
	return nil
}

func (d *MockDB) queryTx(bySrc bool, byDst bool, addr [20]byte, startHeight, endHeight uint32, fn func([]byte) bool) {
	d.mtx.RLock()
	defer d.mtx.RUnlock()
	for i := int64(startHeight); i < int64(endHeight); i++ {
		blk, ok := d.blkMap[i]
		if !ok {
			continue
		}
		for _, tx := range blk.TxList {
			if (bySrc && bytes.Equal(addr[:], tx.SrcAddr[:])) ||
				(byDst && bytes.Equal(addr[:], tx.DstAddr[:])) {
				needMore := fn(tx.Content)
				if !needMore {
					return
				}
			}
		}
	}
	return
}

func (d *MockDB) QueryLogs(addrOrList [][20]byte, topicsOrList [][][32]byte, startHeight, endHeight uint32, fn func([]byte) bool) error {
	panic("Implement Me")
}

func (d *MockDB) QueryNotificationCounter(key []byte) (counter int64) {
	d.mtx.RLock()
	defer d.mtx.RUnlock()
	if len(key) != 21 || key[0] != types.TO_ADDR_KEY {
		panic("Implement Me")
	}
	for i := int64(0); i <= d.height; i++ {
		blk, ok := d.blkMap[i]
		if !ok {
			continue
		}
		for _, tx := range blk.TxList {
			if bytes.Equal(key[1:], tx.DstAddr[:]) {
				counter++
			}
		}
	}
	return
}

func (d *MockDB) SetExtractNotificationFn(fn types.ExtractNotificationFromTxFn) {
	panic("Implement Me")
}

func (d *MockDB) SetDisableComplexIndex(b bool) {
	panic("Implement Me")
}
