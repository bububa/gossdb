package gossdb

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	MAX_RETRIES = 3
	TIMEOUT     = time.Duration(time.Second * 15)
	KEEPALIVE   = true
)

var (
	ErrBadResponse     = fmt.Errorf("bad response")
	ErrNotEnoughParams = fmt.Errorf("not enougn params")
)

type Client struct {
	sock     *net.TCPConn
	recv_buf bytes.Buffer
	addr     *net.TCPAddr
	mutex    *sync.Mutex
}

type KVPair struct {
	k string
	v interface{}
}

func NewKVPair(k string, v interface{}) *KVPair {
	return &KVPair{k: k, v: v}
}

func Connect(ip string, port int) (*Client, error) {
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", ip, port))
	if err != nil {
		return nil, err
	}
	sock, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return nil, err
	}
	//sock.SetKeepAlive(KEEPALIVE)
	var c Client
	c.sock = sock
	c.addr = addr
	c.mutex = new(sync.Mutex)
	return &c, nil
}

func (c *Client) lock() {
	c.mutex.Lock()
}

func (c *Client) unlock() {
	c.mutex.Unlock()
}

func (c *Client) Reconnect() error {
	c.Close()
	sock, err := net.DialTCP("tcp", nil, c.addr)
	if err != nil {
		return err
	}
	c.sock = sock
	if c.recv_buf.Len() > 0 {
		c.recv_buf.Reset()
	}
	return nil
}

func (c *Client) Do(retries int, args ...interface{}) ([]string, error) {
	c.lock()
	defer c.unlock()
	err := c.send(args)
	if err != nil {
		if !strings.Contains(fmt.Sprintf("%s", err), "bad request") && retries < MAX_RETRIES {
			retries++
			c.Reconnect()
			return c.Do(retries, args...)
		}
		return nil, err
	}
	resp, err := c.recv()
	if err != nil && retries < MAX_RETRIES {
		retries++
		c.Reconnect()
		return c.Do(retries, args...)
	}
	return resp, err
}

func (c *Client) Set(key string, val string) (bool, error) {
	resp, err := c.Do(0, "set", key, val)
	if err != nil {
		return false, err
	}
	if len(resp) > 0 && resp[0] == "ok" {
		return true, nil
	}
	return false, ErrBadResponse
}

func (c *Client) Setx(key string, val string, ttl int32) (bool, error) {
	resp, err := c.Do(0, "setx", key, val, ttl)
	if err != nil {
		return false, err
	}
	if len(resp) > 0 && resp[0] == "ok" {
		return true, nil
	}
	return false, ErrBadResponse
}

func (c *Client) Setnx(key string, val string) (bool, error) {
	resp, err := c.Do(0, "setnx", key, val)
	if err != nil {
		return false, err
	}
	if len(resp) > 0 && resp[0] == "ok" {
		return true, nil
	}
	return false, ErrBadResponse
}

// TODO: Will somebody write addition semantic methods?
func (c *Client) Get(key string) (interface{}, error) {
	resp, err := c.Do(0, "get", key)
	if err != nil {
		return nil, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return resp[1], nil
	}
	if len(resp) > 0 && resp[0] == "not_found" {
		return nil, nil
	}
	return nil, ErrBadResponse
}

func (c *Client) Getset(key string) (interface{}, error) {
	resp, err := c.Do(0, "getset", key)
	if err != nil {
		return nil, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return resp[1], nil
	}
	if len(resp) > 0 && resp[0] == "not_found" {
		return nil, nil
	}
	return nil, ErrBadResponse
}

func (c *Client) Del(key string) (bool, error) {
	resp, err := c.Do(0, "del", key)
	if err != nil {
		return false, err
	}
	if len(resp) > 0 && resp[0] == "ok" {
		return true, nil
	}
	return false, ErrBadResponse
}

func (c *Client) MultiSet(pairs ...*KVPair) (bool, error) {
	var args []interface{}
	args = append(args, "multi_set")
	for _, pair := range pairs {
		args = append(args, pair.k)
		args = append(args, pair.v)
	}
	resp, err := c.Do(0, args...)
	if err != nil {
		return false, err
	}
	if len(resp) > 0 && resp[0] == "ok" {
		return true, nil
	}
	return false, ErrBadResponse
}

func (c *Client) MultiGet(ks ...string) ([]*KVPair, error) {
	var args []interface{}
	args = append(args, "multi_get")
	for _, k := range ks {
		args = append(args, k)
	}
	resp, err := c.Do(0, args...)
	if err != nil {
		return nil, err
	}
	if len(resp)&1 == 1 && resp[0] == "ok" {
		var pairs []*KVPair
		for i := 1; i < len(resp); i += 2 {
			pairs = append(pairs, NewKVPair(resp[i], resp[i+1]))
		}
		return pairs, nil
	}
	if len(resp) > 0 && resp[0] == "not_found" {
		return nil, nil
	}
	return nil, ErrBadResponse
}

func (c *Client) MultiDel(ks ...string) (bool, error) {
	var args []interface{}
	args = append(args, "multi_del")
	for _, k := range ks {
		args = append(args, k)
	}
	resp, err := c.Do(0, args...)
	if err != nil {
		return false, err
	}
	if len(resp) > 0 && resp[0] == "ok" {
		return true, nil
	}
	return false, ErrBadResponse
}

func (c *Client) Scan(startKey string, endKey string, limit int) (kvList [][2]string, err error) {
	resp, err := c.Do(0, "scan", startKey, endKey, limit)
	if err != nil {
		return nil, err
	}
	if len(resp)&1 == 1 && resp[0] == "ok" {
		kvList = [][2]string{}
		res := resp[1:]
		for i := 0; i < len(res); i += 2 {
			k := res[i]
			v := res[i+1]
			kv := [2]string{k, v}
			kvList = append(kvList, kv)
		}
		return kvList, nil
	}
	return nil, ErrBadResponse
}

func (c *Client) Exists(key string) (bool, error) {
	resp, err := c.Do(0, "exists", key)
	if err != nil {
		return false, err
	}
	if len(resp) > 0 && resp[0] == "ok" {
		return true, nil
	}
	return false, ErrBadResponse
}

func (c *Client) Incr(key string, num int) (int64, error) {
	resp, err := c.Do(0, "incr", key, num)
	if err != nil {
		return 0, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return strconv.ParseInt(resp[1], 10, 64)
	}
	return 0, ErrBadResponse
}

func (c *Client) Decr(key string, num int) (res int64, err error) {
	resp, err := c.Do(0, "decr", key, num)
	if err != nil {
		return 0, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return strconv.ParseInt(resp[1], 10, 64)
	}
	return 0, ErrBadResponse
}

//Key-Map
func (c *Client) HSet(key, field, val string) (success bool, err error) {
	resp, err := c.Do(0, "hset", key, field, val)
	if err != nil {
		return false, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return true, nil
	}
	return false, ErrBadResponse
}

func (c *Client) HGet(key, field string) (val interface{}, err error) {
	resp, err := c.Do(0, "hget", key, field)
	if err != nil {
		return nil, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		val = resp[1]
		return val, nil
	}
	if len(resp) > 0 && resp[0] == "not_found" {
		return nil, nil
	}
	return nil, ErrBadResponse
}

func (c *Client) HDel(key, field string) (success bool, err error) {
	resp, err := c.Do(0, "hdel", key, field)
	if err != nil {
		return false, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return true, nil
	}
	return false, ErrBadResponse
}

func (c *Client) HIncr(key, field string, num int) (res int64, err error) {
	resp, err := c.Do(0, "hincr", key, field, num)
	if err != nil {
		return 0, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return strconv.ParseInt(resp[1], 10, 64)
	}
	return 0, ErrBadResponse
}

func (c *Client) HDecr(key, field string, num int) (res int64, err error) {
	resp, err := c.Do(0, "hdecr", key, field, num)
	if err != nil {
		return 0, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return strconv.ParseInt(resp[1], 10, 64)
	}
	return 0, ErrBadResponse
}

func (c *Client) HExists(key, field string) (exists bool, err error) {
	resp, err := c.Do(0, "hexists", key, field)
	if err != nil {
		return false, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		num, err2 := strconv.Atoi(resp[1])
		exists = num == 1
		return exists, err2
	}
	return false, ErrBadResponse
}

func (c *Client) HSize(key string) (size int64, err error) {
	resp, err := c.Do(0, "hsize", key)
	if err != nil {
		return 0, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return strconv.ParseInt(resp[1], 10, 64)

	}
	return 0, ErrBadResponse
}

func (c *Client) HList(startKey, endKey string, limit int) (keyList []string, err error) {
	resp, err := c.Do(0, "hlist", startKey, endKey, limit)
	if err != nil {
		return nil, err
	}
	if len(resp) > 0 && resp[0] == "ok" {
		keyList = resp[1:]
		return keyList, nil
	}
	return nil, ErrBadResponse
}

func (c *Client) HKeys(key, startField, endField string, limit int) (fieldList []string, err error) {
	resp, err := c.Do(0, "hkeys", key, startField, endField, limit)
	if err != nil {
		return nil, err
	}
	if len(resp) > 0 && resp[0] == "ok" {
		fieldList = resp[1:]
		return fieldList, nil
	}
	return nil, ErrBadResponse
}

func (c *Client) HScan(key, startField, endField string, limit int) (fvList [][2]string, err error) {
	resp, err := c.Do(0, "hscan", key, startField, endField, limit)
	if err != nil {
		return nil, err
	}
	if len(resp)&1 == 1 && resp[0] == "ok" {
		fvList = [][2]string{}
		res := resp[1:]
		for i := 0; i < len(res); i += 2 {
			f := res[i]
			v := res[i+1]
			fv := [2]string{f, v}
			fvList = append(fvList, fv)
		}
		return fvList, nil
	}
	return nil, ErrBadResponse
}

func (c *Client) HRScan(key, startField, endField string, limit int) (fvList [][2]string, err error) {
	resp, err := c.Do(0, "hrscan", key, startField, endField, limit)
	if err != nil {
		return nil, err
	}
	if len(resp)&1 == 1 && resp[0] == "ok" {
		fvList = [][2]string{}
		res := resp[1:]
		for i := 0; i < len(res); i += 2 {
			f := res[i]
			v := res[i+1]
			fv := [2]string{f, v}
			fvList = append(fvList, fv)
		}
		return fvList, nil
	}
	return nil, ErrBadResponse
}

func (c *Client) HClear(key string) (success bool, err error) {
	resp, err := c.Do(0, "hclear", key)
	if err != nil {
		return false, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return true, nil
	}
	return false, ErrBadResponse
}

func (c *Client) MultiHSet(key string, fvMap map[string]string) (success bool, err error) {
	if len(fvMap) == 0 {
		return false, ErrNotEnoughParams
	}
	args := []interface{}{"multi_hset", key}
	for f, v := range fvMap {
		args = append(args, f, v)
	}
	resp, err := c.Do(0, args...)
	if err != nil {
		return false, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return true, nil
	}
	return false, ErrBadResponse
}

func (c *Client) MultiHGet(key string, fieldList []string) (fvMap map[string]string, err error) {
	if len(fieldList) == 0 {
		return nil, ErrNotEnoughParams
	}
	args := []interface{}{"multi_hget", key}
	for _, f := range fieldList {
		args = append(args, f)
	}
	resp, err := c.Do(0, args...)
	if len(resp)&1 == 1 && resp[0] == "ok" {
		fvMap = map[string]string{}
		res := resp[1:]
		for i := 0; i < len(res); i += 2 {
			k := res[i]
			v := res[i+1]
			fvMap[k] = v
		}
		return fvMap, nil
	}
	return nil, ErrBadResponse
}

func (c *Client) MultiHDel(key string, fieldList []string) (success bool, err error) {
	if len(fieldList) == 0 {
		return false, ErrNotEnoughParams
	}
	args := []interface{}{"multi_del", key}
	for _, f := range fieldList {
		args = append(args, f)
	}
	resp, err := c.Do(0, args...)
	if err != nil {
		return false, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return true, nil
	}
	return false, ErrBadResponse
}

//Key-Zset
func (c *Client) ZSet(key, ele string, score int) (success bool, err error) {
	resp, err := c.Do(0, "zset", key, ele, score)
	if err != nil {
		return false, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return true, nil
	}
	return false, ErrBadResponse
}

func (c *Client) ZGet(key, ele string) (score interface{}, err error) {
	resp, err := c.Do(0, "zget", key, ele)
	if err != nil {
		return nil, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return strconv.ParseInt(resp[1], 10, 64)
	}
	if len(resp) > 0 && resp[0] == "not_found" {
		return nil, nil
	}
	return nil, ErrBadResponse
}

func (c *Client) ZDel(key, ele string) (success bool, err error) {
	resp, err := c.Do(0, "zdel", key, ele)
	if err != nil {
		return false, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return true, nil
	}
	return false, ErrBadResponse
}

func (c *Client) ZIncr(key, ele string, num int) (score int64, err error) {
	resp, err := c.Do(0, "zincr", key, ele, num)
	if err != nil {
		return 0, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return strconv.ParseInt(resp[1], 10, 64)
	}
	return 0, ErrBadResponse
}

func (c *Client) ZSize(key string) (size int64, err error) {
	resp, err := c.Do(0, "zsize", key)
	if err != nil {
		return 0, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return strconv.ParseInt(resp[1], 10, 64)
	}
	return 0, ErrBadResponse
}

func (c *Client) ZExists(key, ele string) (exists bool, err error) {
	resp, err := c.Do(0, "zexists", key, ele)
	if err != nil {
		return false, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		num, err2 := strconv.Atoi(resp[1])
		if err2 != nil {
			return false, err2
		}
		exists = num > 0
		return exists, nil
	}
	return false, ErrBadResponse
}

func (c *Client) ZList(startKey, endKey string, limit int) (keyList []string, err error) {
	resp, err := c.Do(0, "zlist", startKey, endKey, limit)
	if err != nil {
		return nil, err
	}
	if len(resp) > 0 && resp[0] == "ok" {
		keyList = resp[1:]
		return keyList, nil
	}
	return nil, ErrBadResponse
}

func (c *Client) ZKeys(key, startEle string, scoreStart, scoreEnd, limit int) (keyList []string, err error) {
	resp, err := c.Do(0, "zkeys", key, startEle, scoreStart, scoreEnd, limit)
	if err != nil {
		return nil, err
	}
	if len(resp) > 0 && resp[0] == "ok" {
		keyList = resp[1:]
		return keyList, nil
	}
	return nil, ErrBadResponse
}

func (c *Client) ZScan(key, startEle string, scoreStart, scoreEnd, limit int) (esMap map[string]int64, err error) {
	resp, err := c.Do(0, "zscan", key, startEle, scoreStart, scoreEnd, limit)
	if err != nil {
		return nil, err
	}
	if len(resp) > 0 && resp[0] == "ok" {
		res := resp[1:]
		esMap = map[string]int64{}
		for i := 0; i < len(res); i += 2 {
			k := res[i]
			v, err2 := strconv.ParseInt(res[i+1], 10, 64)
			if err2 != nil {
				return nil, err2
			}
			esMap[k] = v
		}
		return esMap, nil
	}
	return nil, ErrBadResponse
}

func (c *Client) ZRScan(key, startEle string, scoreStart, scoreEnd, limit int) (esMap map[string]int64, err error) {
	resp, err := c.Do(0, "zrscan", key, startEle, scoreStart, scoreEnd, limit)
	if err != nil {
		return nil, err
	}
	if len(resp) > 0 && resp[0] == "ok" {
		res := resp[1:]
		esMap = map[string]int64{}
		for i := 0; i < len(res); i += 2 {
			e := res[i]
			s, err2 := strconv.ParseInt(res[i+1], 10, 64)
			if err2 != nil {
				return nil, err2
			}
			esMap[e] = s
		}
		return esMap, nil
	}
	return nil, ErrBadResponse
}

func (c *Client) ZRank(key, ele string) (score int64, err error) {
	resp, err := c.Do(0, "zrank", key, ele)
	if err != nil {
		return 0, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return strconv.ParseInt(resp[1], 10, 64)
	}
	return 0, ErrBadResponse
}

func (c *Client) ZRRank(key, ele string) (score int64, err error) {
	resp, err := c.Do(0, "zrrank", key, ele)
	if err != nil {
		return 0, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return strconv.ParseInt(resp[1], 10, 64)
	}
	return 0, ErrBadResponse
}

func (c *Client) ZRange(key string, offset, limit int) (esList [][2]interface{}, err error) {
	resp, err := c.Do(0, "zrange", key, offset, limit)
	if err != nil {
		return nil, err
	}
	if len(resp)&1 == 1 && resp[0] == "ok" {
		res := resp[1:]
		esList = [][2]interface{}{}
		for i := 0; i < len(res); i += 2 {
			e := res[i]
			s, err2 := strconv.Atoi(res[i+1])
			if err2 != nil {
				return nil, err2
			}
			es := [2]interface{}{e, s}
			esList = append(esList, es)
		}
		return esList, nil
	}
	return nil, ErrBadResponse
}

func (c *Client) ZRRange(key string, offset, limit int) (esList [][2]interface{}, err error) {
	resp, err := c.Do(0, "zrrange", key, offset, limit)
	if err != nil {
		return nil, err
	}
	if len(resp)&1 == 1 && resp[0] == "ok" {
		res := resp[1:]
		esList = [][2]interface{}{}
		for i := 0; i < len(res); i += 2 {
			e := res[i]
			s, err2 := strconv.Atoi(res[i+1])
			if err2 != nil {
				return nil, err2
			}
			es := [2]interface{}{e, s}
			esList = append(esList, es)
		}
		return esList, nil
	}
	return nil, ErrBadResponse
}

func (c *Client) ZClear(key string) (success bool, err error) {
	resp, err := c.Do(0, "zclear", key)
	if err != nil {
		return false, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return true, nil
	}
	return false, ErrBadResponse
}

func (c *Client) MultiZSet(key string, esMap map[string]int) (success bool, err error) {
	if len(esMap) == 0 {
		return false, ErrNotEnoughParams
	}
	args := []interface{}{"multi_zset", key}
	for e, s := range esMap {
		args = append(args, e, s)
	}
	resp, err := c.Do(0, args...)
	if err != nil {
		return false, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return true, nil
	}
	return false, ErrBadResponse
}

func (c *Client) MultiZGet(key string, eleList []string) (esMap map[string]int64, err error) {
	if len(eleList) == 0 {
		return nil, ErrNotEnoughParams
	}
	args := []interface{}{"multi_zget", key}
	for _, e := range eleList {
		args = append(args, e)
	}
	resp, err := c.Do(0, args...)
	if len(resp)&1 == 1 && resp[0] == "ok" {
		res := resp[1:]
		esMap = map[string]int64{}
		for i := 0; i < len(res); i += 2 {
			k := res[i]
			v, err2 := strconv.ParseInt(res[i+1], 10, 64)
			if err2 != nil {
				return nil, err2
			}
			esMap[k] = v
		}
		return esMap, nil
	}
	return nil, ErrBadResponse
}

func (c *Client) MultiZDel(key string, eleList []string) (success bool, err error) {
	if len(eleList) == 0 {
		return false, ErrNotEnoughParams
	}
	args := []interface{}{"multi_zdel", key}
	for _, e := range eleList {
		args = append(args, e)
	}
	resp, err := c.Do(0, args...)
	if err != nil {
		return false, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return true, nil
	}
	return false, ErrBadResponse
}

//Key-List/Queue
func (c *Client) QSzie(key string) (size int64, err error) {
	resp, err := c.Do(0, "qsize", key)
	if err != nil {
		return 0, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return strconv.ParseInt(resp[1], 10, 64)
	}
	return 0, ErrBadResponse
}

func (c *Client) QClear(key string) (success bool, err error) {
	resp, err := c.Do(0, "qclear", key)
	if err != nil {
		return false, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return true, nil
	}
	return false, ErrBadResponse
}

func (c *Client) QFront(key string) (item string, err error) {
	resp, err := c.Do(0, "qfront", key)
	if err != nil {
		return "", err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		item = resp[1]
		return item, nil
	}
	return "", ErrBadResponse
}

func (c *Client) QBack(key string) (item string, err error) {
	resp, err := c.Do(0, "qback", key)
	if err != nil {
		return "", err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		item = resp[1]
		return item, nil
	}
	return "", ErrBadResponse
}

func (c *Client) QGet(key string, index int) (item interface{}, err error) {
	resp, err := c.Do(0, "qget", key, index)
	if err != nil {
		return nil, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		item = resp[1]
		return item, nil
	}
	if len(resp) > 0 && resp[0] == "not_found" {
		return nil, nil
	}
	return nil, ErrBadResponse
}

func (c *Client) QSlice(key string, begin, end int) (itemList []string, err error) {
	resp, err := c.Do(0, "qslice", key, begin, end)
	if err != nil {
		return nil, err
	}
	if len(resp) > 0 && resp[0] == "ok" {
		itemList = resp[1:]
		return itemList, nil
	}
	return nil, ErrBadResponse
}

func (c *Client) QPush(key, item string) (success bool, err error) {
	return c.QPushBack(key, item)
}

func (c *Client) QPushFront(key, item string) (success bool, err error) {
	resp, err := c.Do(0, "qpush_front", key, item)
	if err != nil {
		return false, err
	}
	if len(resp) == 1 && resp[0] == "ok" {
		return true, nil
	}
	return false, ErrBadResponse

}

func (c *Client) QPushBack(key, item string) (success bool, err error) {
	resp, err := c.Do(0, "qpush_back", key, item)
	if err != nil {
		return false, err
	}
	if len(resp) == 1 && resp[0] == "ok" {
		return true, nil
	}
	return false, ErrBadResponse
}

func (c *Client) QPop(key string) (ele interface{}, err error) {
	return c.QPopFront(key)
}

func (c *Client) QPopFront(key string) (ele interface{}, err error) {
	resp, err := c.Do(0, "qpop_front", key)
	if err != nil {
		return nil, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		ele = resp[1]
		return ele, nil
	}
	if len(resp) > 0 && resp[0] == "not_found" {
		return nil, nil
	}
	return nil, ErrBadResponse
}

func (c *Client) QPopBack(key string) (ele interface{}, err error) {
	resp, err := c.Do(0, "qpop_back", key)
	if err != nil {
		return nil, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		ele = resp[1]
		return ele, nil
	}
	if len(resp) > 0 && resp[0] == "not_found" {
		return nil, nil
	}
	return nil, ErrBadResponse
}

func (c *Client) send(args []interface{}) error {
	var buf bytes.Buffer
	for _, arg := range args {
		var s string
		switch arg := arg.(type) {
		case string:
			s = arg
		case []byte:
			s = string(arg)
		case []string:
			for _, s := range arg {
				p := strconv.Itoa(len(s))
				buf.WriteString(p)
				buf.WriteByte('\n')
				buf.WriteString(s)
				buf.WriteByte('\n')
			}
			continue
		case int, int32, int64, uint, uint32, uint64:
			s = fmt.Sprintf("%d", arg)
		case float32, float64:
			s = fmt.Sprintf("%f", arg)
		case bool:
			if arg {
				s = "1"
			} else {
				s = "0"
			}
		case nil:
			s = ""
		default:
			return fmt.Errorf("bad request:%v", arg)
		}
		p := strconv.Itoa(len(s))
		buf.WriteString(p)
		buf.WriteByte('\n')
		buf.WriteString(s)
		buf.WriteByte('\n')
	}
	buf.WriteByte('\n')
	c.sock.SetWriteDeadline(time.Now().Add(TIMEOUT))
	_, err := c.sock.Write(buf.Bytes())
	return err
}

func (c *Client) recv() ([]string, error) {
	var tmp [1024 * 128]byte
	c.sock.SetReadDeadline(time.Now().Add(TIMEOUT))
	for {
		n, err := c.sock.Read(tmp[0:])
		if err != nil {
			return nil, err
		}
		c.recv_buf.Write(tmp[0:n])
		resp := c.parse()
		if resp == nil || len(resp) > 0 {
			return resp, nil
		}
	}
	return nil, nil
}

func (c *Client) parse() []string {
	resp := []string{}
	/*sl := strings.Split(c.recv_buf.String(), "\n")
	for i, v := range sl {
		if v == "" {
			continue
		}

		if i%2 == 1 {
			resp = append(resp, v)
		}
	}
	return resp*/
	buf := c.recv_buf.Bytes()
	var idx, offset int
	idx = 0
	offset = 0

	for {
		idx = bytes.IndexByte(buf[offset:], '\n')
		if idx == -1 {
			break
		}
		p := buf[offset : offset+idx]
		offset += idx + 1
		//fmt.Printf("> [%s]\n", p);
		if len(p) == 0 || (len(p) == 1 && p[0] == '\r') {
			if len(resp) == 0 {
				continue
			} else {
				c.recv_buf.Next(offset)
				return resp
			}
		}

		size, err := strconv.Atoi(string(p))
		if err != nil || size < 0 {
			return nil
		}
		if offset+size >= c.recv_buf.Len() {
			break
		}

		v := buf[offset : offset+size]
		resp = append(resp, string(v))
		offset += size + 1
	}

	return []string{}
}

// Close The Client Connection
func (c *Client) Close() error {
	return c.sock.Close()
}
