package gossdb

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

const (
	MAX_RETRIES = 3
	TIMEOUT     = time.Duration(time.Second * 15)
	KEEPALIVE   = true
)

type Client struct {
	sock     *net.TCPConn
	recv_buf bytes.Buffer
	addr     *net.TCPAddr
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
	sock.SetKeepAlive(KEEPALIVE)
	var c Client
	c.sock = sock
	c.addr = addr
	return &c, nil
}

func (c *Client) Reconnect() error {
	c.Close()
	sock, err := net.DialTCP("tcp", nil, c.addr)
	if err != nil {
		return err
	}
	c.sock = sock
	return nil
}

func (c *Client) Do(retries int, args ...interface{}) ([]string, error) {
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
	if len(resp) == 2 && resp[0] == "ok" {
		return true, nil
	}
	return false, fmt.Errorf("bad response:%v", resp)
}

func (c *Client) Setx(key string, val string, ttl int32) (bool, error) {
	resp, err := c.Do(0, "setx", key, val, ttl)
	if err != nil {
		return false, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return true, nil
	}
	return false, fmt.Errorf("bad response:%v", resp)
}

func (c *Client) Setnx(key string, val string) (bool, error) {
	resp, err := c.Do(0, "setnx", key, val)
	if err != nil {
		return false, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return true, nil
	}
	return false, fmt.Errorf("bad response:%v", resp)
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
	if len(resp) == 0 || resp[0] == "not_found" {
		return nil, nil
	}
	return nil, fmt.Errorf("bad response:%v", resp)
}

func (c *Client) Getset(key string) (interface{}, error) {
	resp, err := c.Do(0, "getset", key)
	if err != nil {
		return nil, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return resp[1], nil
	}
	if len(resp) == 0 || resp[0] == "not_found" {
		return nil, nil
	}
	return nil, fmt.Errorf("bad response:%v", resp)
}

func (c *Client) Del(key string) (bool, error) {
	resp, err := c.Do(0, "del", key)
	if err != nil {
		return false, err
	}
	if len(resp) >= 1 && resp[0] == "ok" {
		return true, nil
	}
	return false, fmt.Errorf("bad response:%v", resp)
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
	if len(resp) == 2 && resp[0] == "ok" {
		return true, nil
	}
	return false, fmt.Errorf("bad response:%v", resp)
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
	var pairs []*KVPair
	for i := 1; i < len(resp); i += 2 {
		fmt.Printf("K:%s, V:%s\n", resp[i], resp[i+1])
		pairs = append(pairs, NewKVPair(resp[i], resp[i+1]))
	}
	if len(resp) >= 3 && resp[0] == "ok" {
		return pairs, nil
	}
	if len(resp) == 0 || resp[0] == "not_found" {
		return nil, nil
	}
	return nil, fmt.Errorf("bad response:%v", resp)
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
	if len(resp) >= 1 && resp[0] == "ok" {
		return true, nil
	}
	return false, fmt.Errorf("bad response:%v", resp)
}

func (c *Client) Exists(key string) (bool, error) {
	resp, err := c.Do(0, "exists", key)
	if err != nil {
		return false, err
	}
	if len(resp) >= 1 && resp[0] == "ok" {
		return true, nil
	}
	return false, fmt.Errorf("bad response")
}

func (c *Client) Incr(key string, num int) (int64, error) {
	resp, err := c.Do(0, "incr", key, num)
	if err != nil {
		return 0, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return strconv.ParseInt(resp[1], 10, 64)
	}
	return 0, fmt.Errorf("bad response:%v", resp)
}

func (c *Client) HSet(name string, key string, val string) (bool, error) {
	resp, err := c.Do(0, "hset", name, key, val)
	if err != nil {
		return false, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return true, nil
	}
	return false, fmt.Errorf("bad response:%v", resp)
}

func (c *Client) HGet(name string, key string) (interface{}, error) {
	resp, err := c.Do(0, "hget", name, key)
	if err != nil {
		return nil, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return resp[1], nil
	}
	if resp[0] == "not_found" {
		return nil, nil
	}
	return nil, fmt.Errorf("bad response:%v", resp)
}

func (c *Client) HDel(name string, key string) (bool, error) {
	resp, err := c.Do(0, "hdel", name, key)
	if err != nil {
		return false, err
	}
	if len(resp) >= 1 && resp[0] == "ok" {
		return true, nil
	}
	return false, fmt.Errorf("bad response:%v", resp)
}

func (c *Client) HIncr(name string, key string, num int) (int64, error) {
	resp, err := c.Do(0, "hincr", name, key, num)
	if err != nil {
		return 0, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return strconv.ParseInt(resp[1], 10, 64)
	}
	return 0, fmt.Errorf("bad response:%v", resp)
}

func (c *Client) HExists(name string, key string) (bool, error) {
	resp, err := c.Do(0, "hexists", key)
	if err != nil {
		return false, err
	}
	if len(resp) >= 1 && resp[0] == "ok" {
		return true, nil
	}
	return false, fmt.Errorf("bad response:%v", resp)
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
		if len(resp) == 0 {
			fmt.Printf("SSDB BUF:%s", c.recv_buf.String())
		}
		if resp == nil || len(resp) > 0 {
			return resp, nil
		}
	}
	return nil, nil
}

func (c *Client) parse() []string {
	resp := []string{}
	sl := strings.Split(c.recv_buf.String(), "\n")
	for i, v := range sl {
		if v == "" {
			break
		}

		if i%2 == 1 {
			resp = append(resp, v)
		}
	}
	return resp
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
