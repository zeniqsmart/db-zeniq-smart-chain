package db

import (
	"encoding/binary"

	"github.com/zeniqsmart/ads-zeniq-smart-chain/indextree"

	"github.com/zeniqsmart/db-zeniq-smart-chain/types"
)

type LiteDB struct {
	metadb            *RocksDB
	latestBlockhashes [512]*BlockHeightAndHash
}

var _ types.DB = (*LiteDB)(nil)

func NewLiteDB(path string) (d *LiteDB) {
	metadb, err := indextree.NewRocksDB("rocksdb", path)
	if err != nil {
		panic(err)
	}
	d = &LiteDB{metadb: metadb}
	iter := metadb.Iterator([]byte("B"), []byte("C"))
	defer iter.Close()
	for iter.Valid() {
		blkHH := (&BlockHeightAndHash{}).setBytes(iter.Value())
		d.latestBlockhashes[int(blkHH.Height)%len(d.latestBlockhashes)] = blkHH
	}
	return
}

func (d *LiteDB) AddBlock(blk *types.Block, pruneTillHeight int64, txid2sigMap map[[32]byte][65]byte) {
	blkHH := &BlockHeightAndHash{
		Height:    uint32(blk.Height),
		BlockHash: blk.BlockHash,
	}
	d.latestBlockhashes[int(blk.Height)%len(d.latestBlockhashes)] = blkHH
	blkKey := []byte("B1234")
	binary.LittleEndian.PutUint32(blkKey[1:], uint32(blk.Height))
	d.metadb.SetSync(blkKey, blkHH.toBytes())
}

func (d *LiteDB) SetOpListsForCcUtxo(opListsForCcUtxo types.OpListsForCcUtxo) {}

func (d *LiteDB) GetBlockHashByHeight(height int64) (res [32]byte) {
	heightAndHash := d.latestBlockhashes[int(height)%len(d.latestBlockhashes)]
	if heightAndHash == nil {
		return
	}
	if heightAndHash.Height == uint32(height) {
		res = heightAndHash.BlockHash
	}
	return
}

func (d *LiteDB) Close() {
	d.metadb.Close()
}

func (d *LiteDB) SetExtractNotificationFn(fn types.ExtractNotificationFromTxFn) {
}
func (d *LiteDB) SetDisableComplexIndex(b bool) {
}
func (d *LiteDB) GetLatestHeight() (latestHeight int64) {
	for _, blkHH := range d.latestBlockhashes {
		if blkHH != nil && latestHeight < int64(blkHH.Height) {
			latestHeight = int64(blkHH.Height)
		}
	}
	return
}
func (d *LiteDB) GetBlockByHeight(height int64) []byte {
	return nil
}
func (d *LiteDB) GetTxByHeightAndIndex(height int64, index int) []byte {
	return nil
}
func (d *LiteDB) GetTxListByHeightWithRange(height int64, start, end int) [][]byte {
	return nil
}
func (d *LiteDB) GetTxListByHeight(height int64) [][]byte {
	return nil
}
func (d *LiteDB) GetBlockByHash(hash [32]byte, collectResult func([]byte) bool) {
}
func (d *LiteDB) GetTxByHash(hash [32]byte, collectResult func([]byte) bool) {
}
func (d *LiteDB) GetTxSigByHash(hash [32]byte) (res [65]byte) {
	return
}
func (d *LiteDB) BasicQueryLogs(addr *[20]byte, topics [][32]byte, startHeight, endHeight uint32, fn func([]byte) bool) error {
	return nil
}
func (d *LiteDB) QueryLogs(addrOrList [][20]byte, topicsOrList [][][32]byte, startHeight, endHeight uint32, fn func([]byte) bool) error {
	return nil
}
func (d *LiteDB) QueryTxBySrc(addr [20]byte, startHeight, endHeight uint32, fn func([]byte) bool) error {
	return nil
}
func (d *LiteDB) QueryTxByDst(addr [20]byte, startHeight, endHeight uint32, fn func([]byte) bool) error {
	return nil
}
func (d *LiteDB) QueryTxBySrcOrDst(addr [20]byte, startHeight, endHeight uint32, fn func([]byte) bool) error {
	return nil
}
func (d *LiteDB) QueryNotificationCounter(key []byte) int64 {
	return 0
}
func (d *LiteDB) SetMaxEntryCount(c int) {
}

func (d *LiteDB) GetUtxoInfos() (infos [][36 + 1 + 20]byte)                           { return nil }
func (d *LiteDB) GetAllUtxoIds() [][36]byte                                           { return nil }
func (d *LiteDB) GetRedeemableUtxoIds() [][36]byte                                    { return nil }
func (d *LiteDB) GetLostAndFoundUtxoIds() [][36]byte                                  { return nil }
func (d *LiteDB) GetRedeemingUtxoIds() [][36]byte                                     { return nil }
func (d *LiteDB) GetUtxoIdsByCovenantAddr(covenantAddr [20]byte) [][36]byte           { return nil }
func (d *LiteDB) GetRedeemableUtxoIdsByCovenantAddr(covenantAddr [20]byte) [][36]byte { return nil }
