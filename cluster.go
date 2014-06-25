package gossdb

import (
	"crypto/sha1"
	"strconv"
	"strings"
)

type Cluster struct {
	shards []*Client
}

func NewCluster(shardsAddr []string) (*Cluster, error) {
	c := &Cluster{}
	for _, addr := range shardsAddr {
		h := strings.Split(addr, ":")
		port, err := strconv.ParseInt(h[1], 10, 64)
		if err != nil {
			return nil, err
		}
		s, err := Connect(h[0], int(port))
		if err != nil {
			c.Close()
			return nil, err
		}
		c.shards = append(c.shards, s)
	}
	return c, nil
}

// Locate the ID of shard containing a key
func (c *Cluster) locate(k []byte) uint16 {
	h := sha1.New()
	for len(k) > 0 {
		n, err := h.Write(k)
		if err != nil {
			panic(err)
		}
		k = k[n:]
	}
	s := h.Sum(nil)
	pos := ((uint16(s[0]) << 0) | (uint16(s[1]) << 8)) % uint16(len(c.shards))
	return pos
}

// Locate the IDs of shard containting the keys
func (c *Cluster) locateKeys(ks ...string) map[int][]string {
	res := make(map[int][]string)
	for _, k := range ks {
		loc := int(c.locate([]byte(k)))
		res[loc] = append(res[loc], k)
	}
	return res
}

func (c *Cluster) locatePairs(ps ...*KVPair) map[int][]*KVPair {
	res := make(map[int][]*KVPair)
	for _, p := range ps {
		loc := int(c.locate([]byte(p.k)))
		res[loc] = append(res[loc], p)
	}
	return res
}

func (c *Cluster) Set(key string, val string) (bool, error) {
	return c.shards[c.locate([]byte(key))].Set(key, val)
}

func (c *Cluster) Setx(key string, val string, ttl int32) (bool, error) {
	return c.shards[c.locate([]byte(key))].Setx(key, val, ttl)
}

func (c *Cluster) Setnx(key string, val string) (bool, error) {
	return c.shards[c.locate([]byte(key))].Setnx(key, val)
}

func (c *Cluster) Get(key string) (interface{}, error) {
	return c.shards[c.locate([]byte(key))].Get(key)
}

func (c *Cluster) Del(key string) (bool, error) {
	return c.shards[c.locate([]byte(key))].Del(key)
}

func (c *Cluster) MultiGet(ks ...string) []*KVPair {
	if len(ks) == 0 {
		return nil
	}
	parts := c.locateKeys(ks...)
	ch := make(chan []*KVPair, len(parts))
	for i, part := range parts {
		go func(idx int, keys []string, shard *Client) {
			ps, err := shard.MultiGet(keys...)
			if err == nil && ps != nil {
				ch <- ps
			} else {
				ch <- nil
			}
		}(i, part, c.shards[i])
	}

	var ps []*KVPair
	for i := 0; i < len(parts); i++ {
		pairs := <-ch
		for _, v := range pairs {
			if v == nil {
				continue
			}
			ps = append(ps, v)
		}
	}
	return ps
}

func (c *Cluster) MultiSet(ps ...*KVPair) (ks []string, err error) {
	if len(ps) == 0 {
		return nil, nil
	}
	parts := c.locatePairs(ps...)
	ch := make(chan int, len(parts))
	for i, part := range parts {
		go func(idx int, pairs []*KVPair, shard *Client) {
			success, err2 := shard.MultiSet(pairs...)
			if err2 == nil && success {
				ch <- idx
			} else {
				if err2 != nil {
					err = err2
				}
				ch <- -1
			}
		}(i, part, c.shards[i])
	}

	for i := 0; i < len(parts); i++ {
		if id := <-ch; id >= 0 {
			for _, p := range parts[id] {
				ks = append(ks, p.k)
			}
		}
	}
	return ks, err
}

func (c *Cluster) MultiDel(ks ...string) (res []string, err error) {
	if len(ks) == 0 {
		return nil, nil
	}
	parts := c.locateKeys(ks...)
	ch := make(chan int, len(parts))
	for i, part := range parts {
		go func(idx int, keys []string, shard *Client) {
			success, err2 := shard.MultiDel(keys...)
			if err2 == nil && success {
				ch <- idx
			} else {
				if err2 != nil {
					err = err2
				}
				ch <- -1
			}
		}(i, part, c.shards[i])
	}

	for i := 0; i < len(parts); i++ {
		if id := <-ch; id >= 0 {
			res = append(res, parts[id]...)
		}
	}
	return res, err
}

func (c *Cluster) Exists(key string) (bool, error) {
	return c.shards[c.locate([]byte(key))].Exists(key)
}

func (c *Cluster) Incr(key string, num int) (int64, error) {
	return c.shards[c.locate([]byte(key))].Incr(key, num)
}

func (c *Cluster) HSet(name string, key string, val string) (bool, error) {
	return c.shards[c.locate([]byte(name))].HSet(name, key, val)
}

func (c *Cluster) HGet(name string, key string) (interface{}, error) {
	return c.shards[c.locate([]byte(name))].HGet(name, key)
}

func (c *Cluster) HDel(name string, key string) (bool, error) {
	return c.shards[c.locate([]byte(name))].HDel(name, key)
}

func (c *Cluster) HIncr(name string, key string, num int) (int64, error) {
	return c.shards[c.locate([]byte(name))].HIncr(name, key, num)
}

func (c *Cluster) HExists(name string, key string) (bool, error) {
	return c.shards[c.locate([]byte(name))].HExists(name, key)
}

func (c *Cluster) Close() error {
	for _, conn := range c.shards {
		err := conn.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
