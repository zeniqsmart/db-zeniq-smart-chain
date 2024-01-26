package db

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash"
	"github.com/tendermint/tendermint/libs/log"
	"github.com/zeniqsmart/ads-zeniq-smart-chain/datatree"
	"github.com/zeniqsmart/ads-zeniq-smart-chain/indextree"

	"github.com/zeniqsmart/db-zeniq-smart-chain/indexer"
	"github.com/zeniqsmart/db-zeniq-smart-chain/types"
)

/*  Following keys are saved in rocksdb:
"HPF_SIZE" the size of hpfile
"SEED" seed for xxhash, used to generate short hash
"NEW" new block's information for indexing, deleted after consumption
"BXXXX" ('B' followed by 4 bytes) the indexing information for a block
"N------" ('N' followed by variable-length bytes) the notification counters
"SIG+txid32" caches transactions' signatures

cc-UTXO:
 'c'||Redeemable||UtxoId => nil
 'c'||LostAndFound||UtxoId => nil
 'c'||Redeeming||UtxoId => nil
 'c'||Addr2Utxo||CovenantAddr||UtxoId => nil
 'c'||UTXO||UtxoId => SourceType||CovenantAddr
*/

const (
	MaxExpandedSize      = 64
	MaxMatchedTx         = 50000
	DB_PARA_READ_THREADS = 8
)

type RocksDB = indextree.RocksDB
type HPFile = datatree.HPFile

var (
	ErrQueryConditionExpandedTooLarge = errors.New("query condition expanded too large")
	ErrTooManyPotentialResults        = errors.New("too many potential results")
)

// At this mutex, if a writer is trying to get a write-lock, no new reader can get read-lock
type rwMutex struct {
	mtx sync.RWMutex
	wg  sync.WaitGroup
}

func (mtx *rwMutex) lock() {
	mtx.wg.Add(1)
	mtx.mtx.Lock()
	mtx.wg.Done()
}
func (mtx *rwMutex) unlock() {
	mtx.mtx.Unlock()
}
func (mtx *rwMutex) rLock() {
	mtx.wg.Wait()
	mtx.mtx.RLock()
}
func (mtx *rwMutex) rUnlock() {
	mtx.mtx.RUnlock()
}

type DB struct {
	wg       sync.WaitGroup
	mtx      rwMutex
	path     string
	metadb   *RocksDB
	hpfile   *HPFile
	blkBuf   []byte
	idxBuf   []byte
	seed     [8]byte
	indexer  indexer.Indexer
	maxCount int
	height   int64
	//For the notify counters
	extractNotificationFromTx types.ExtractNotificationFromTxFn
	//Disable complex transaction index: from-addr to-addr logs
	disableComplexIndex bool
	//Cache for latest blockhashes
	latestBlockhashes [512]atomic.Value

	opListsForCcUtxo types.OpListsForCcUtxo

	logger log.Logger
}

type BlockHeightAndHash struct {
	Height    uint32
	BlockHash [32]byte
}

func (blkHH BlockHeightAndHash) toBytes() []byte {
	var res [4 + 32]byte
	binary.LittleEndian.PutUint32(res[:4], blkHH.Height)
	copy(res[4:], blkHH.BlockHash[:])
	return res[:]
}

func (blkHH *BlockHeightAndHash) setBytes(in []byte) *BlockHeightAndHash {
	if len(in) != 4+32 {
		panic("Incorrect length for BlockHeightAndHash")
	}
	blkHH.Height = binary.LittleEndian.Uint32(in[:4])
	copy(blkHH.BlockHash[:], in[4:])
	return blkHH
}

var _ types.DB = (*DB)(nil)

func CreateEmptyDB(path string, seed [8]byte, logger log.Logger) *DB {
	metadb, err := indextree.NewRocksDB("rocksdb", path)
	if err != nil {
		panic(err)
	}
	hpfile, err := datatree.NewHPFile(8*1024*1024, 2048*1024*1024, path+"/data")
	if err != nil {
		panic(err)
	}
	d := &DB{
		path:    path,
		metadb:  metadb,
		hpfile:  hpfile,
		blkBuf:  make([]byte, 0, 1024),
		idxBuf:  make([]byte, 0, 1024),
		seed:    seed,
		indexer: indexer.New(),
		logger:  logger,
	}
	d.SetExtractNotificationFn(DefaultExtractNotificationFromTxFn)
	var zero [8]byte
	d.metadb.OpenNewBatch()
	d.metadb.CurrBatch().Set([]byte("HPF_SIZE"), zero[:])
	d.metadb.CurrBatch().Set([]byte("SEED"), d.seed[:])
	d.metadb.CloseOldBatch()
	return d
}

func NewDB(path string, logger log.Logger) *DB {
	metadb, err := indextree.NewRocksDB("rocksdb", path)
	if err != nil {
		panic(err)
	}
	// 8MB Read Buffer, 2GB file block
	hpfile, err := datatree.NewHPFile(8*1024*1024, 2048*1024*1024, path+"/data")
	if err != nil {
		panic(err)
	}
	d := &DB{
		path:     path,
		metadb:   metadb,
		hpfile:   hpfile,
		blkBuf:   make([]byte, 0, 1024),
		idxBuf:   make([]byte, 0, 1024),
		indexer:  indexer.New(),
		maxCount: -1,
		logger:   logger,
	}
	d.SetExtractNotificationFn(DefaultExtractNotificationFromTxFn)
	// for a half-committed block, hpfile may have some garbage after the position
	// marked by HPF_SIZE
	bz := d.metadb.Get([]byte("HPF_SIZE"))
	size := binary.LittleEndian.Uint64(bz)
	err = d.hpfile.Truncate(int64(size))
	if err != nil {
		panic(err)
	}

	// reload the persistent data from metadb into in-memory indexer
	d.reloadToIndexer()

	// hash seed is also saved in metadb. It cannot be changed in DB's lifetime
	copy(d.seed[:], d.metadb.Get([]byte("SEED")))

	// If "NEW" key is not deleted, a pending block has not been indexed, so we
	// index it.
	blkBz := d.metadb.Get([]byte("NEW"))
	if blkBz == nil {
		return d
	}
	blk := &types.Block{}
	_, err = blk.UnmarshalMsg(blkBz)
	if err != nil {
		panic(err)
	}
	d.latestBlockhashes[int(blk.Height)%len(d.latestBlockhashes)].Store(&BlockHeightAndHash{
		Height:    uint32(blk.Height),
		BlockHash: blk.BlockHash,
	})
	d.wg.Add(1)
	go d.postAddBlock(blk, -1) //pruneTillHeight==-1 means no prune
	d.wg.Wait()                // wait for goroutine to finish
	return d
}

func (d *DB) Close() {
	d.wg.Wait() // wait for previous postAddBlock goroutine to finish
	d.hpfile.Close()
	d.metadb.Close()
	d.indexer.Close()
}

func (d *DB) SetMaxEntryCount(c int) {
	d.maxCount = (c * 12) / 10 // with 20% margin
	d.indexer.SetMaxOffsetCount(d.maxCount)
}

func (d *DB) GetLatestHeight() int64 {
	return atomic.LoadInt64(&d.height)
}

// Add a new block for indexing, and prune the index information for blocks before pruneTillHeight
// The ownership of 'blk' will be transferred to DB and cannot be changed by out world!
func (d *DB) AddBlock(blk *types.Block, pruneTillHeight int64, txid2sigMap map[[32]byte][65]byte) {
	d.wg.Wait() // wait for previous postAddBlock goroutine to finish
	if blk == nil {
		return
	}

	// firstly serialize and write the block into metadb under the key "NEW".
	// if the indexing process is aborted due to crash or something, we
	// can resume the block from metadb
	var err error
	d.blkBuf, err = blk.MarshalMsg(d.blkBuf[:0])
	if err != nil {
		panic(err)
	}
	d.metadb.OpenNewBatch()
	d.metadb.CurrBatch().Set([]byte("NEW"), d.blkBuf)
	d.handleOpListsForCcUtxo()
	for txid, sig := range txid2sigMap {
		d.metadb.CurrBatch().Set(append([]byte("SIG"), txid[:]...), sig[:])
	}
	d.metadb.CloseOldBatch()

	d.latestBlockhashes[int(blk.Height)%len(d.latestBlockhashes)].Store(&BlockHeightAndHash{
		Height:    uint32(blk.Height),
		BlockHash: blk.BlockHash,
	})
	d.logger.Debug(fmt.Sprintf("addBlock, height:%d, hash:%s", blk.Height, EncodeToHex(blk.BlockHash[:])))
	// start the postAddBlock goroutine which should finish before the next indexing job
	d.wg.Add(1)
	go d.postAddBlock(blk, pruneTillHeight)
	// when this function returns, we are sure that metadb has saved 'blk'
}

// append data at the end of hpfile, padding to 32 bytes
func (d *DB) appendToFile(data []byte) int64 {
	var zeros [32]byte
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(len(data)))
	pad := Padding32(4 + len(data))
	off, err := d.hpfile.Append([][]byte{buf[:], data, zeros[:pad]})
	if err != nil {
		panic(err)
	}
	return off / 32
}

// post-processing after AddBlock
func (d *DB) postAddBlock(blk *types.Block, pruneTillHeight int64) {
	blkIdx := &types.BlockIndex{
		Height:       uint32(blk.Height),
		BlockHash:    blk.BlockHash,
		TxHash48List: make([]uint64, len(blk.TxList)),
		TxPosList:    make([]int64, len(blk.TxList)),
	}
	if !d.disableComplexIndex {
		d.fillLogIndex(blk, blkIdx)
	}
	// Get a write lock before we start updating
	d.mtx.lock()
	defer func() {
		d.mtx.unlock()
		d.wg.Done()
	}()

	offset40 := d.appendToFile(blk.BlockInfo)
	blkIdx.BeginOffset = offset40
	blkIdx.BlockHash48 = Sum48(d.seed, blk.BlockHash[:])
	d.indexer.AddBlock(blkIdx.Height, blkIdx.BlockHash48, offset40)

	if !d.disableComplexIndex {
		for i, tx := range blk.TxList {
			sig := d.getTxSigByHashFromMeta(tx.HashId)
			offset40 = d.appendToFile(append(sig[:], tx.Content...))
			blkIdx.TxPosList[i] = offset40
			blkIdx.TxHash48List[i] = Sum48(d.seed, tx.HashId[:])
			id56 := GetId56(blkIdx.Height, i)
			d.indexer.AddTx(id56, blkIdx.TxHash48List[i], offset40)
		}
		for i, srcHash48 := range blkIdx.SrcHashes {
			d.indexer.AddSrc2Tx(srcHash48, blkIdx.Height, blkIdx.SrcPosLists[i])
		}
		for i, dstHash48 := range blkIdx.DstHashes {
			d.indexer.AddDst2Tx(dstHash48, blkIdx.Height, blkIdx.DstPosLists[i])
		}
		for i, addrHash48 := range blkIdx.AddrHashes {
			d.indexer.AddAddr2Tx(addrHash48, blkIdx.Height, blkIdx.AddrPosLists[i])
		}
		for i, topicHash48 := range blkIdx.TopicHashes {
			d.indexer.AddTopic2Tx(topicHash48, blkIdx.Height, blkIdx.TopicPosLists[i])
		}
	}

	d.metadb.OpenNewBatch()
	for _, tx := range blk.TxList {
		d.metadb.CurrBatch().Delete(append([]byte("SIG"), tx.HashId[:]...))
	}
	blkKey := []byte("B1234")
	binary.BigEndian.PutUint32(blkKey[1:], blkIdx.Height)
	if !d.metadb.Has(blkKey) { // if we have not processed this block before
		d.updateNotificationCounters(blk)
	}
	// save the index information to metadb, such that we can later recover and prune in-memory index
	var err error
	d.idxBuf, err = blkIdx.MarshalMsg(d.idxBuf[:0])
	if err != nil {
		panic(err)
	}
	d.metadb.CurrBatch().Set(blkKey, d.idxBuf)
	// write the size of hpfile to metadb
	var b8 [8]byte
	binary.LittleEndian.PutUint64(b8[:], uint64(d.hpfile.Size()))
	d.metadb.CurrBatch().Set([]byte("HPF_SIZE"), b8[:])
	// with blkIdx and hpfile updated, we finish processing the pending block.
	d.metadb.CurrBatch().Delete([]byte("NEW"))
	d.metadb.CloseOldBatch()
	d.hpfile.Flush()
	if d.hpfile.Size() > 192 {
		err = d.hpfile.ReadAt(b8[:], 192, false)
		if err != nil {
			panic(err)
		}
	}
	d.pruneTillBlock(pruneTillHeight)

	atomic.StoreInt64(&d.height, blk.Height)
}

func (d *DB) SetExtractNotificationFn(fn types.ExtractNotificationFromTxFn) {
	d.extractNotificationFromTx = fn
}

func (d *DB) SetDisableComplexIndex(b bool) {
	d.disableComplexIndex = b
}

func (d *DB) updateNotificationCounters(blk *types.Block) {
	if d.extractNotificationFromTx == nil || d.disableComplexIndex {
		return
	}
	notiMap := make(map[string]int64, len(blk.TxList)*2)
	for _, tx := range blk.TxList {
		d.extractNotificationFromTx(tx, notiMap)
	}
	notiStrList := make([]string, 0, len(notiMap))
	notiCountList := make([]int64, 0, len(notiMap))
	for notiStr, notiCount := range notiMap {
		notiStrList = append(notiStrList, notiStr)
		notiCountList = append(notiCountList, notiCount)
	}
	sharedIdx := int64(-1)
	parallelRun(DB_PARA_READ_THREADS, func(_ int) {
		for {
			myIdx := atomic.AddInt64(&sharedIdx, 1)
			if myIdx >= int64(len(notiStrList)) {
				return
			}
			k := append([]byte{'N'}, notiStrList[myIdx]...)
			bz := d.metadb.Get(k)
			value := int64(0)
			if len(bz) != 0 {
				value = int64(binary.LittleEndian.Uint64(bz))
			}
			notiCountList[myIdx] += value
		}
	})
	for i, notiStr := range notiStrList {
		var bz [8]byte
		binary.LittleEndian.PutUint64(bz[:], uint64(notiCountList[i]))
		d.metadb.CurrBatch().Set(append([]byte{'N'}, notiStr...), bz[:])
	}
}

// prune in-memory index and hpfile till the block at 'pruneTillHeight' (not included)
func (d *DB) pruneTillBlock(pruneTillHeight int64) {
	if pruneTillHeight < 0 {
		return
	}
	// get an iterator in the range [0, pruneTillHeight)
	start := []byte("B1234")
	binary.BigEndian.PutUint32(start[1:], 0)
	end := []byte("B1234")
	binary.BigEndian.PutUint32(end[1:], uint32(pruneTillHeight))
	iter := d.metadb.Iterator(start, end)
	defer iter.Close()
	keys := make([][]byte, 0, 100)
	for iter.Valid() {
		keys = append(keys, iter.Key())
		// get the recorded index information for a block
		bi := &types.BlockIndex{}
		_, err := bi.UnmarshalMsg(iter.Value())
		if err != nil {
			panic(err)
		}
		// now prune in-memory index and hpfile
		d.pruneBlock(bi)
		iter.Next()
	}
	// remove the recorded index information from metadb
	d.metadb.OpenNewBatch()
	for _, key := range keys {
		d.metadb.CurrBatch().Delete(key)
	}
	d.metadb.CloseOldBatch()
}

func (d *DB) pruneBlock(bi *types.BlockIndex) {
	// Prune the head part of hpfile
	err := d.hpfile.PruneHead(bi.BeginOffset)
	if err != nil {
		panic(err)
	}
	// Erase the information recorded in 'bi'
	d.indexer.EraseBlock(bi.Height, bi.BlockHash48)
	for i, hash48 := range bi.TxHash48List {
		id56 := GetId56(bi.Height, i)
		d.indexer.EraseTx(id56, hash48, bi.TxPosList[i])
	}
	for _, hash48 := range bi.SrcHashes {
		d.indexer.EraseSrc2Tx(hash48, bi.Height)
	}
	for _, hash48 := range bi.DstHashes {
		d.indexer.EraseDst2Tx(hash48, bi.Height)
	}
	for _, hash48 := range bi.AddrHashes {
		d.indexer.EraseAddr2Tx(hash48, bi.Height)
	}
	for _, hash48 := range bi.TopicHashes {
		d.indexer.EraseTopic2Tx(hash48, bi.Height)
	}
}

// fill blkIdx.Topic* and blkIdx.Addr* according to 'blk'
func (d *DB) fillLogIndex(blk *types.Block, blkIdx *types.BlockIndex) {
	var zeroAddr [20]byte
	srcIndex := make(map[uint64][]uint32)
	dstIndex := make(map[uint64][]uint32)
	addrIndex := make(map[uint64][]uint32)
	topicIndex := make(map[uint64][]uint32)
	for i, tx := range blk.TxList {
		if !bytes.Equal(tx.SrcAddr[:], zeroAddr[:]) {
			srcHash48 := Sum48(d.seed, tx.SrcAddr[:])
			AppendAtKey(srcIndex, srcHash48, uint32(i))
		}
		if !bytes.Equal(tx.DstAddr[:], zeroAddr[:]) {
			dstHash48 := Sum48(d.seed, tx.DstAddr[:])
			AppendAtKey(dstIndex, dstHash48, uint32(i))
		}
		for _, log := range tx.LogList {
			for _, topic := range log.Topics {
				topicHash48 := Sum48(d.seed, topic[:])
				AppendAtKey(topicIndex, topicHash48, uint32(i))
			}
			addrHash48 := Sum48(d.seed, log.Address[:])
			AppendAtKey(addrIndex, addrHash48, uint32(i))
		}
	}
	// the map 'srcIndex' is recorded into two slices
	blkIdx.SrcHashes = make([]uint64, 0, len(srcIndex))
	blkIdx.SrcPosLists = make([][]uint32, 0, len(srcIndex))
	for src, posList := range srcIndex {
		blkIdx.SrcHashes = append(blkIdx.SrcHashes, src)
		blkIdx.SrcPosLists = append(blkIdx.SrcPosLists, posList)
	}
	// the map 'dstIndex' is recorded into two slices
	blkIdx.DstHashes = make([]uint64, 0, len(dstIndex))
	blkIdx.DstPosLists = make([][]uint32, 0, len(dstIndex))
	for dst, posList := range dstIndex {
		blkIdx.DstHashes = append(blkIdx.DstHashes, dst)
		blkIdx.DstPosLists = append(blkIdx.DstPosLists, posList)
	}
	// the map 'addrIndex' is recorded into two slices
	blkIdx.AddrHashes = make([]uint64, 0, len(addrIndex))
	blkIdx.AddrPosLists = make([][]uint32, 0, len(addrIndex))
	for addr, posList := range addrIndex {
		blkIdx.AddrHashes = append(blkIdx.AddrHashes, addr)
		blkIdx.AddrPosLists = append(blkIdx.AddrPosLists, posList)
	}
	// the map 'topicIndex' is recorded into two slices
	blkIdx.TopicHashes = make([]uint64, 0, len(topicIndex))
	blkIdx.TopicPosLists = make([][]uint32, 0, len(topicIndex))
	for topic, posList := range topicIndex {
		blkIdx.TopicHashes = append(blkIdx.TopicHashes, topic)
		blkIdx.TopicPosLists = append(blkIdx.TopicPosLists, posList)
	}
	//return
}

// reload index information from metadb into in-memory indexer
func (d *DB) reloadToIndexer() {
	// Get an iterator over all recorded blocks' indexes
	start := []byte{byte('B'), 0, 0, 0, 0}
	end := []byte{byte('B'), 255, 255, 255, 255}
	iter := d.metadb.Iterator(start, end)
	defer iter.Close()
	for iter.Valid() {
		bi := &types.BlockIndex{}
		_, err := bi.UnmarshalMsg(iter.Value())
		if err != nil {
			panic(err)
		}
		d.reloadBlockToIndexer(bi)
		iter.Next()
	}
}

// reload one block's index information into in-memory indexer
func (d *DB) reloadBlockToIndexer(blkIdx *types.BlockIndex) {
	d.indexer.AddBlock(blkIdx.Height, blkIdx.BlockHash48, blkIdx.BeginOffset)
	d.latestBlockhashes[int(blkIdx.Height)%len(d.latestBlockhashes)].Store(&BlockHeightAndHash{
		Height:    blkIdx.Height,
		BlockHash: blkIdx.BlockHash,
	})
	atomic.StoreInt64(&d.height, int64(blkIdx.Height))
	for i, txHash48 := range blkIdx.TxHash48List {
		id56 := GetId56(blkIdx.Height, i)
		d.indexer.AddTx(id56, txHash48, blkIdx.TxPosList[i])
	}
	for i, srcHash48 := range blkIdx.SrcHashes {
		d.indexer.AddSrc2Tx(srcHash48, blkIdx.Height, blkIdx.SrcPosLists[i])
	}
	for i, dstHash48 := range blkIdx.DstHashes {
		d.indexer.AddDst2Tx(dstHash48, blkIdx.Height, blkIdx.DstPosLists[i])
	}
	for i, addrHash48 := range blkIdx.AddrHashes {
		d.indexer.AddAddr2Tx(addrHash48, blkIdx.Height, blkIdx.AddrPosLists[i])
	}
	for i, topicHash48 := range blkIdx.TopicHashes {
		d.indexer.AddTopic2Tx(topicHash48, blkIdx.Height, blkIdx.TopicPosLists[i])
	}
}

// read at offset40*32 to fetch data out
func (d *DB) readInFile(offset40 int64) []byte {
	// read the length out
	var buf [4]byte
	offset := GetRealOffset(offset40*32, d.hpfile.Size())
	err := d.hpfile.ReadAt(buf[:], offset, false)
	if err != nil {
		panic(err)
	}
	size := binary.LittleEndian.Uint32(buf[:])
	// read the payload out
	bz := make([]byte, int(size)+4)
	err = d.hpfile.ReadAt(bz, offset, false)
	if err != nil {
		panic(err)
	}
	return bz[4:]
}

// given a recent block's height, return its blockhash
func (d *DB) GetBlockHashByHeight(height int64) (res [32]byte) {
	heightAndHashV := d.latestBlockhashes[int(height)%len(d.latestBlockhashes)].Load()
	if heightAndHashV == nil {
		d.logger.Debug(fmt.Sprintf("getBlockHashByHeight, heightAndHash is nil in height:%d", height))
		return
	}
	heightAndHash := heightAndHashV.(*BlockHeightAndHash)
	if heightAndHash == nil {
		d.logger.Debug(fmt.Sprintf("getBlockHashByHeight, heightAndHash is nil in height:%d", height))
		return
	}
	d.logger.Debug(fmt.Sprintf("getBlockHashByHeight: heightAndHash.height:%d, heightAndHash.hash:%s in height:%d", heightAndHash.Height, hex.EncodeToString(heightAndHash.BlockHash[:]), height))
	if heightAndHash.Height == uint32(height) {
		res = heightAndHash.BlockHash
	} else {
		d.logger.Debug(fmt.Sprintf("GetBlockHashByHeight: height:%d != heightAndHash.Height:%d", height, heightAndHash.Height))
	}
	return
}

// given a block's height, return serialized information.
func (d *DB) GetBlockByHeight(height int64) []byte {
	d.mtx.rLock()
	defer d.mtx.rUnlock()
	offset40 := d.indexer.GetOffsetByBlockHeight(uint32(height))
	if offset40 < 0 {
		return nil
	}
	return d.readInFile(offset40)
}

// given a transaction's height+index, return serialized information.
func (d *DB) GetTxByHeightAndIndex(height int64, index int) []byte {
	d.mtx.rLock()
	defer d.mtx.rUnlock()
	id56 := GetId56(uint32(height), index)
	offset40 := d.indexer.GetOffsetByTxID(id56)
	if offset40 < 0 {
		return nil
	}
	return d.readInFile(offset40)
}

// given a blocks's height, return serialized information of its transactions.
func (d *DB) GetTxListByHeightWithRange(height int64, start, end int) [][]byte {
	d.mtx.rLock()
	defer d.mtx.rUnlock()
	if end < 0 || end > (1<<24)-1 {
		end = (1 << 24) - 1
	}
	if start > (1<<24)-2 {
		start = (1 << 24) - 2
	}
	if end < start+1 {
		end = start + 1
	}
	id56Start := GetId56(uint32(height), start)
	id56End := GetId56(uint32(height), end)
	offList, _ := d.indexer.GetOffsetsByTxIDRange(id56Start, id56End)
	res := make([][]byte, len(offList))
	for i, offset40 := range offList {
		res[i] = d.readInFile(offset40)
	}
	return res
}

func (d *DB) GetTxListByHeight(height int64) [][]byte {
	return d.GetTxListByHeightWithRange(height, 0, -1)
}

// given a block's hash, feed possibly-correct serialized information to collectResult; if
// collectResult confirms the information is correct by returning true, this function stops loop.
func (d *DB) GetBlockByHash(hash [32]byte, collectResult func([]byte) bool) {
	d.mtx.rLock()
	defer d.mtx.rUnlock()
	hash48 := Sum48(d.seed, hash[:])
	offsets, _ := d.indexer.GetOffsetsByBlockHash(hash48)
	for _, offset40 := range offsets {
		bz := d.readInFile(offset40)
		if collectResult(bz) {
			return
		}
	}
}

// given a block's hash, feed possibly-correct serialized information to collectResult; if
// collectResult confirms the information is correct by returning true, this function stops loop.
func (d *DB) GetTxByHash(hash [32]byte, collectResult func([]byte) bool) {
	d.mtx.rLock()
	defer d.mtx.rUnlock()
	hash48 := Sum48(d.seed, hash[:])
	offsets, _ := d.indexer.GetOffsetsByTxHash(hash48)
	for _, offset40 := range offsets {
		bz := d.readInFile(offset40)
		if collectResult(bz) {
			return
		}
	}
}

func (d *DB) getTxSigByHashFromMeta(hash [32]byte) (res [65]byte) {
	bz := d.metadb.Get(append([]byte("SIG"), hash[:]...))
	if len(bz) != 0 {
		copy(res[:], bz)
	}
	return
}

type addrAndTopics struct {
	addr   *[20]byte
	topics [][32]byte
}

func (aat *addrAndTopics) appendTopic(topic [32]byte) addrAndTopics {
	return addrAndTopics{
		addr:   aat.addr,
		topics: append(append([][32]byte{}, aat.topics...), topic),
	}
}

//func (aat *addrAndTopics) isEmpty() bool {
//	return aat.addr == nil && len(aat.topics) == 0
//}

func (aat *addrAndTopics) toShortStr() string {
	s := "-"
	if aat.addr != nil {
		s = string((*aat.addr)[0])
	}
	for _, t := range aat.topics {
		s += string(t[0])
	}
	return s
}

/*
Given a 'AND of OR' list, expand it into 'OR of ADD' list.
For example, '(a|b|c) & (d|e) & (f|g)' expands to:

	(a&d&f) | (a&d&g) | (a&e&f) | (a&e&g)
	(b&d&f) | (b&d&g) | (b&e&f) | (b&e&g)
	(c&d&f) | (c&d&g) | (c&e&f) | (c&e&g)
*/
func expandQueryCondition(addrOrList [][20]byte, topicsOrList [][][32]byte) []addrAndTopics {
	res := make([]addrAndTopics, 0, MaxExpandedSize)
	if len(addrOrList) == 0 {
		res = append(res, addrAndTopics{addr: nil})
	} else {
		res = make([]addrAndTopics, 0, len(addrOrList))
		for i := range addrOrList {
			res = append(res, addrAndTopics{addr: &addrOrList[i]})
		}
	}
	if len(topicsOrList) >= 1 && len(res) <= MaxExpandedSize && len(topicsOrList[0]) != 0 {
		res = expandTopics(topicsOrList[0], res)
	}
	if len(topicsOrList) >= 2 && len(res) <= MaxExpandedSize && len(topicsOrList[1]) != 0 {
		res = expandTopics(topicsOrList[1], res)
	}
	if len(topicsOrList) >= 3 && len(res) <= MaxExpandedSize && len(topicsOrList[2]) != 0 {
		res = expandTopics(topicsOrList[2], res)
	}
	if len(topicsOrList) >= 4 && len(res) <= MaxExpandedSize && len(topicsOrList[3]) != 0 {
		res = expandTopics(topicsOrList[3], res)
	}
	return res
}

// For each element in 'inList', expand it into len(topicsOrList) by appending different topics, and put
// the results into 'outList'
func expandTopics(topicOrList [][32]byte, inList []addrAndTopics) (outList []addrAndTopics) {
	outList = make([]addrAndTopics, 0, MaxExpandedSize)
	for _, aat := range inList {
		for _, topic := range topicOrList {
			outList = append(outList, aat.appendTopic(topic))
			if len(outList) > MaxExpandedSize {
				return
			}
		}
	}
	return
}

func reverseOffList(s []int64) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

// Given 0~1 addr and 0~4 topics, feed the possibly-matching transactions to 'fn'; the return value of 'fn' indicates
// whether it wants more data.
func (d *DB) BasicQueryLogs(addr *[20]byte, topics [][32]byte,
	startHeight, endHeight uint32, fn func([]byte) bool) error {
	d.mtx.rLock()
	defer d.mtx.rUnlock()
	reverse := false
	if startHeight > endHeight {
		reverse = true
		startHeight, endHeight = endHeight, startHeight
	}
	offList, ok := d.getTxOffList(addr, topics, startHeight, endHeight)
	if !ok {
		return ErrTooManyPotentialResults
	}
	if reverse {
		reverseOffList(offList)
	}
	d.runFnAtTxs(offList, fn)
	return nil
}

// Read TXs out according to offset lists, and apply 'fn' to them
func (d *DB) runFnAtTxs(offList []int64, fn func([]byte) bool) {
	if d.maxCount > 0 && len(offList) >= d.maxCount {
		fn(nil) // to report error
		return
	}
	for _, offset40 := range offList {
		bz := d.readInFile(offset40)
		if needMore := fn(bz); !needMore {
			break
		}
	}
}

// Get a list of TXs' offsets out from the indexer.
func (d *DB) getTxOffList(addr *[20]byte, topics [][32]byte, startHeight, endHeight uint32) ([]int64, bool) {
	addrHash48 := uint64(1) << 63 // an invalid value
	if addr != nil {
		addrHash48 = Sum48(d.seed, (*addr)[:])
	}
	topicHash48List := make([]uint64, len(topics))
	for i, hash := range topics {
		topicHash48List[i] = Sum48(d.seed, hash[:])
	}
	return d.indexer.QueryTxOffsets(addrHash48, topicHash48List, startHeight, endHeight)
}

func (d *DB) QueryLogs(addrOrList [][20]byte, topicsOrList [][][32]byte,
	startHeight, endHeight uint32, fn func([]byte) bool) error {
	aatList := expandQueryCondition(addrOrList, topicsOrList)
	if len(aatList) > MaxExpandedSize {
		return ErrQueryConditionExpandedTooLarge
	}
	offLists := make([][]int64, len(aatList))
	for i, aat := range aatList {
		var ok bool
		offLists[i], ok = d.getTxOffList(aat.addr, aat.topics, startHeight, endHeight)
		if !ok {
			return ErrTooManyPotentialResults
		}
	}
	offList := mergeOffLists(offLists)
	if len(offList) > d.maxCount {
		return ErrTooManyPotentialResults
	}
	d.runFnAtTxs(offList, fn)
	return nil
}

func (d *DB) QueryTxBySrc(addr [20]byte, startHeight, endHeight uint32, fn func([]byte) bool) error {
	d.mtx.rLock()
	defer d.mtx.rUnlock()
	addrHash48 := Sum48(d.seed, addr[:])
	reverse := false
	if startHeight > endHeight {
		reverse = true
		startHeight, endHeight = endHeight, startHeight
	}
	offList, ok := d.indexer.QueryTxOffsetsBySrc(addrHash48, startHeight, endHeight)
	if !ok {
		return ErrTooManyPotentialResults
	}
	if reverse {
		reverseOffList(offList)
	}
	d.runFnAtTxs(offList, fn)
	return nil
}

func (d *DB) QueryTxByDst(addr [20]byte, startHeight, endHeight uint32, fn func([]byte) bool) error {
	d.mtx.rLock()
	defer d.mtx.rUnlock()
	addrHash48 := Sum48(d.seed, addr[:])
	reverse := false
	if startHeight > endHeight {
		reverse = true
		startHeight, endHeight = endHeight, startHeight
	}
	offList, ok := d.indexer.QueryTxOffsetsByDst(addrHash48, startHeight, endHeight)
	if !ok {
		return ErrTooManyPotentialResults
	}
	if reverse {
		reverseOffList(offList)
	}
	d.runFnAtTxs(offList, fn)
	return nil
}

func (d *DB) QueryTxBySrcOrDst(addr [20]byte, startHeight, endHeight uint32, fn func([]byte) bool) error {
	d.mtx.rLock()
	defer d.mtx.rUnlock()
	addrHash48 := Sum48(d.seed, addr[:])
	reverse := false
	if startHeight > endHeight {
		reverse = true
		startHeight, endHeight = endHeight, startHeight
	}
	offListSrc, ok := d.indexer.QueryTxOffsetsBySrc(addrHash48, startHeight, endHeight)
	if !ok {
		return ErrTooManyPotentialResults
	}
	offListDst, ok := d.indexer.QueryTxOffsetsByDst(addrHash48, startHeight, endHeight)
	if !ok {
		return ErrTooManyPotentialResults
	}
	offList := mergeOffLists([][]int64{offListSrc, offListDst})
	if reverse {
		reverseOffList(offList)
	}
	d.runFnAtTxs(offList, fn)
	return nil
}

func (d *DB) QueryNotificationCounter(key []byte) int64 {
	bz := d.metadb.Get(append([]byte{'N'}, key...))
	if len(bz) == 0 {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(bz))
}

// ===================================

// Merge multiple sorted offset lists into one
func mergeOffLists(offLists [][]int64) []int64 {
	if len(offLists) == 1 {
		return offLists[0]
	}
	res := make([]int64, 0, 1000)
	for {
		idx, min := findMinimumFirstElement(offLists)
		if idx == -1 { // every one in offLists has been consumed
			break
		}
		if len(res) == 0 || res[len(res)-1] != min {
			res = append(res, min)
		}
		offLists[idx] = offLists[idx][1:] //consume one element of this offset list
	}
	return res
}

// Among several offset list, the idx-th list's first element is the minimum and 'min' is its value
func findMinimumFirstElement(offLists [][]int64) (idx int, min int64) {
	idx, min = -1, 0
	for i := range offLists {
		if len(offLists[i]) == 0 {
			continue
		}
		if idx == -1 || min > offLists[i][0] {
			idx = i
			min = offLists[i][0]
		}
	}
	return
}

// returns the short hash of the key
func Sum48(seed [8]byte, key []byte) uint64 {
	digest := xxhash.New()
	_, _ = digest.Write(seed[:])
	_, _ = digest.Write(key)
	return (digest.Sum64() << 16) >> 16
}

// append value at a slice at 'key'. If the slice does not exist, create it.
func AppendAtKey(m map[uint64][]uint32, key uint64, value uint32) {
	_, ok := m[key]
	if !ok {
		m[key] = make([]uint32, 0, 10)
	}
	if len(m[key]) == 0 || m[key][len(m[key])-1] != value { // avoid duplication
		m[key] = append(m[key], value)
	}
}

// make sure (length+n)%32 == 0
func Padding32(length int) (n int) {
	mod := length % 32
	if mod != 0 {
		n = 32 - mod
	}
	return
}

// offset40 can represent 32TB range, but a hpfile's virual size can be larger than it.
// calculate a real offset from offset40 which pointing to a valid position in hpfile.
func GetRealOffset(offset, size int64) int64 {
	unit := int64(32) << 40 // 32 tera bytes
	n := size / unit
	if size%unit == 0 {
		n--
	}
	offset += n * unit
	if offset > size {
		offset -= unit
	}
	return offset
}

func GetId56(height uint32, i int) uint64 {
	return (uint64(height) << 24) | uint64(i)
}

func parallelRun(workerCount int, fn func(workerID int)) {
	var wg sync.WaitGroup
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func(i int) {
			fn(i)
			wg.Done()
		}(i)
	}
	wg.Wait()
}

//================================

// To-address of TX
// From-address of SEP20-Transfer
// To-address of SEP20-Transfer
func DefaultExtractNotificationFromTxFn(tx types.Tx, notiMap map[string]int64) {
	var addToMap = func(k string) {
		notiMap[k] += 1
	}
	k := append([]byte{types.FROM_ADDR_KEY}, tx.SrcAddr[:]...)
	addToMap(string(k))
	k = append([]byte{types.TO_ADDR_KEY}, tx.DstAddr[:]...)
	addToMap(string(k))
	for _, log := range tx.LogList {
		if len(log.Topics) != 3 || !bytes.Equal(log.Topics[0][:], types.TransferEvent[:]) {
			continue
		}
		k := append(append([]byte{types.TRANS_FROM_ADDR_KEY}, log.Address[:]...), log.Topics[1][:]...)
		addToMap(string(k))
		k = append(append([]byte{types.TRANS_TO_ADDR_KEY}, log.Address[:]...), log.Topics[2][:]...)
		addToMap(string(k))
	}
}

func EncodeToHex(b []byte) string {
	enc := make([]byte, len(b)*2+2)
	copy(enc, "0x")
	hex.Encode(enc[2:], b)
	return string(enc)
}

//================================

const (
	Redeemable   = byte(0)
	LostAndFound = byte(1)
	Redeeming    = byte(2)
	Burn         = byte(9)
	Addr2Utxo    = byte(10)
	UTXO         = byte(255)
)

func (d *DB) SetOpListsForCcUtxo(opListsForCcUtxo types.OpListsForCcUtxo) {
	d.opListsForCcUtxo = opListsForCcUtxo
}

func (d *DB) handleOpListsForCcUtxo() {
	for _, op := range d.opListsForCcUtxo.NewRedeemableOps {
		key := append(append([]byte("c"), Redeemable), op.UtxoId[:]...)
		d.metadb.CurrBatch().Set(key, []byte{})
		key = append(append(append([]byte("c"), Addr2Utxo), op.CovenantAddr[:]...), op.UtxoId[:]...)
		d.metadb.CurrBatch().Set(key, []byte{})
		key = append(append([]byte("c"), UTXO), op.UtxoId[:]...)
		d.metadb.CurrBatch().Set(key, append([]byte{Redeemable}, op.CovenantAddr[:]...))
	}
	for _, op := range d.opListsForCcUtxo.NewLostAndFoundOps {
		key := append(append([]byte("c"), LostAndFound), op.UtxoId[:]...)
		d.metadb.CurrBatch().Set(key, []byte{})
		d.logger.Debug("NewLostAndFoundOps write kv", "txid", hex.EncodeToString(op.UtxoId[:32]))
		key = append(append([]byte("c"), UTXO), op.UtxoId[:]...)
		d.metadb.CurrBatch().Set(key, append([]byte{LostAndFound}, op.CovenantAddr[:]...))
	}
	for _, op := range d.opListsForCcUtxo.RedeemOps {
		key := append(append([]byte("c"), Redeeming), op.UtxoId[:]...)
		d.metadb.CurrBatch().Set(key, []byte{})
		d.logger.Debug("RedeemOps write kv", "txid", hex.EncodeToString(op.UtxoId[:32]))
		key = append(append([]byte("c"), UTXO), op.UtxoId[:]...)
		d.metadb.CurrBatch().Set(key, append([]byte{Redeeming}, op.CovenantAddr[:]...))
		if op.SourceType != Burn {
			key = append(append([]byte("c"), op.SourceType), op.UtxoId[:]...)
			d.metadb.CurrBatch().Delete(key)
			d.logger.Debug("RedeemOps delete kv", "txid", hex.EncodeToString(op.UtxoId[:32]), "sourceType", op.SourceType)
		}
		key = append(append(append([]byte("c"), Addr2Utxo), op.CovenantAddr[:]...), op.UtxoId[:]...)
		d.metadb.CurrBatch().Delete(key)
	}
	for _, op := range d.opListsForCcUtxo.ConvertedOps {
		key := append(append(append([]byte("c"), Addr2Utxo), op.OldCovenantAddr[:]...), op.PrevUtxoId[:]...)
		d.metadb.CurrBatch().Delete(key)
		key = append(append(append([]byte("c"), Addr2Utxo), op.NewCovenantAddr[:]...), op.UtxoId[:]...)
		d.metadb.CurrBatch().Set(key, []byte{})

		key = append(append([]byte("c"), UTXO), op.PrevUtxoId[:]...)
		d.metadb.CurrBatch().Delete(key)
		key = append(append([]byte("c"), UTXO), op.UtxoId[:]...)
		d.metadb.CurrBatch().Set(key, append([]byte{Redeemable}, op.NewCovenantAddr[:]...))

		key = append(append([]byte("c"), Redeemable), op.PrevUtxoId[:]...)
		d.metadb.CurrBatch().Delete(key)
		key = append(append([]byte("c"), Redeemable), op.UtxoId[:]...)
		d.metadb.CurrBatch().Set(key, []byte{})
	}
	for _, op := range d.opListsForCcUtxo.DeletedOps {
		d.logger.Debug("DeletedOps delete kv", "txid", hex.EncodeToString(op.UtxoId[:32]), "sourceType", op.SourceType)
		if op.SourceType == LostAndFound {
			//change to redeeming
			op.SourceType = Redeeming
		}
		key := append(append([]byte("c"), op.SourceType), op.UtxoId[:]...)
		d.metadb.CurrBatch().Delete(key)
		key = append(append(append([]byte("c"), Addr2Utxo), op.CovenantAddr[:]...), op.UtxoId[:]...)
		d.metadb.CurrBatch().Delete(key)
		key = append(append([]byte("c"), UTXO), op.UtxoId[:]...)
		d.metadb.CurrBatch().Delete(key)
	}
}

func (d *DB) GetAllUtxoIds() [][36]byte {
	keyPrefix := append([]byte("c"), UTXO)
	return d.getUtxoIds(keyPrefix)
}
func (d *DB) GetRedeemableUtxoIds() [][36]byte {
	keyPrefix := append([]byte("c"), Redeemable)
	return d.getUtxoIds(keyPrefix)
}
func (d *DB) GetLostAndFoundUtxoIds() [][36]byte {
	keyPrefix := append([]byte("c"), LostAndFound)
	return d.getUtxoIds(keyPrefix)
}
func (d *DB) GetRedeemingUtxoIds() [][36]byte {
	keyPrefix := append([]byte("c"), Redeeming)
	return d.getUtxoIds(keyPrefix)
}
func (d *DB) GetUtxoIdsByCovenantAddr(covenantAddr [20]byte) [][36]byte {
	keyPrefix := append(append([]byte("c"), Addr2Utxo), covenantAddr[:]...)
	return d.getUtxoIds(keyPrefix)
}

func (d *DB) GetRedeemableUtxoIdsByCovenantAddr(covenantAddr [20]byte) [][36]byte {
	keyPrefixA := append([]byte("c"), Redeemable)
	keyPrefixB := append(append([]byte("c"), Addr2Utxo), covenantAddr[:]...)
	return d.getCommonUtxoIds(keyPrefixA, keyPrefixB)
}

/*
cc-UTXO:

	'c'||Redeemable||UtxoId => nil
	'c'||LostAndFound||UtxoId => nil
	'c'||Redeeming||UtxoId => nil
	'c'||Addr2Utxo||CovenantAddr||UtxoId => nil
	'c'||UTXO||UtxoId => SourceType||CovenantAddr
*/
func (d *DB) getUtxoIds(keyPrefix []byte) (ids [][36]byte) {
	keyPrefixLen := len(keyPrefix)
	start := append(keyPrefix, bytes.Repeat([]byte{0}, 36)...)
	end := append(keyPrefix, bytes.Repeat([]byte{255}, 36)...)
	iter := d.metadb.Iterator(start, end)
	defer iter.Close()
	for iter.Valid() {
		utxoId := [36]byte{}
		copy(utxoId[:], iter.Key()[keyPrefixLen:])
		ids = append(ids, utxoId)
		iter.Next()
	}
	return
}

func (d *DB) getCommonUtxoIds(keyPrefixA, keyPrefixB []byte) (ids [][36]byte) {
	startA := append(keyPrefixA, bytes.Repeat([]byte{0}, 36)...)
	endA := append(keyPrefixA, bytes.Repeat([]byte{255}, 36)...)
	iterA := d.metadb.Iterator(startA, endA)
	defer iterA.Close()
	startB := append(keyPrefixB, bytes.Repeat([]byte{0}, 36)...)
	endB := append(keyPrefixB, bytes.Repeat([]byte{255}, 36)...)
	iterB := d.metadb.Iterator(startB, endB)
	defer iterB.Close()
	for iterA.Valid() && iterB.Valid() {
		utxoIdA := [36]byte{}
		copy(utxoIdA[:], iterA.Key()[len(keyPrefixA):])
		utxoIdB := [36]byte{}
		copy(utxoIdB[:], iterB.Key()[len(keyPrefixB):])
		cmp := bytes.Compare(utxoIdA[:], utxoIdB[:])
		if cmp == 0 {
			ids = append(ids, utxoIdA)
			iterA.Next()
			iterB.Next()
		} else if cmp > 0 {
			iterB.Next()
		} else { // cmp < 0
			iterA.Next()
		}
	}
	return
}

func (d *DB) GetUtxoInfos() (infos [][36 + 1 + 20]byte) {
	keyPrefix := append([]byte("c"), UTXO)
	start := append(keyPrefix, bytes.Repeat([]byte{0}, 36)...)
	end := append(keyPrefix, bytes.Repeat([]byte{255}, 36)...)
	iter := d.metadb.Iterator(start, end)
	defer iter.Close()
	for iter.Valid() {
		info := [36 + 1 + 20]byte{}
		copy(info[:], iter.Key()[2:])
		copy(info[36:], iter.Value())
		infos = append(infos, info)
		iter.Next()
	}
	return
}
