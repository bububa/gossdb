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
func (c *Cluster) locateKeys(ks ...[]byte) [][][]byte {
	res := make([][][]byte, len(c.shards))
	for _, k := range ks {
		loc := c.locate(k)
		res[loc] = append(res[loc], k)
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

func (c *Cluster) Exists(key string) (bool, error) {
	return c.shards[c.locate([]byte(key))].Exists(key)
}

func (c *Cluster) Incr(key string, num int) (int64, error) {
	return c.shards[c.locate([]byte(key))].Incr(key, num)
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
