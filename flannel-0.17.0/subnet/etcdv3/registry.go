// Copyright 2015 flannel authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcd3

import (
	"encoding/json"
	_ "fmt"
	"github.com/flannel-io/flannel/pkg/ip"
	. "github.com/flannel-io/flannel/subnet"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/api/v3/mvccpb"
	etcd "go.etcd.io/etcd/client/v3"
	"golang.org/x/net/context"
	log "k8s.io/klog"
	"path"
	_ "path"
	"regexp"
	"sync"
	"time"
)

var (
	errTryAgain = errors.New("try again")
)

type Registry interface {
	getNetworkConfig(ctx context.Context) (string, error)
	getSubnets(ctx context.Context) ([]Lease, uint64, error)
	getSubnet(ctx context.Context, sn ip.IP4Net, sn6 ip.IP6Net) (*Lease, uint64, error)
	createSubnet(ctx context.Context, sn ip.IP4Net, sn6 ip.IP6Net, attrs *LeaseAttrs, ttl time.Duration) (time.Time, error)
	updateSubnet(ctx context.Context, sn ip.IP4Net, sn6 ip.IP6Net, attrs *LeaseAttrs, ttl time.Duration, asof uint64) (time.Time, error)
	deleteSubnet(ctx context.Context, sn ip.IP4Net, sn6 ip.IP6Net) error
	watchSubnets(ctx context.Context, since uint64) (Event, uint64, error)
	watchSubnet(ctx context.Context, since uint64, sn ip.IP4Net, sn6 ip.IP6Net) (Event, uint64, error)
}

type EtcdConfig struct {
	Endpoints []string
	Keyfile   string
	Certfile  string
	CAFile    string
	Prefix    string
	Username  string
	Password  string
}

type etcdNewFunc func(c *EtcdConfig) (*etcd.Client, error)

type etcdSubnetRegistry struct {
	cliNewFunc   etcdNewFunc
	mux          sync.Mutex
	cli          *etcd.Client
	etcdCfg      *EtcdConfig
	networkRegex *regexp.Regexp
}

func newEtcdClient(c *EtcdConfig) (*etcd.Client, error) {
	cli, err := etcd.New(etcd.Config{
		Endpoints: c.Endpoints,
		Username:  c.Username,
		Password:  c.Password,
	})
	if err != nil {
		return nil, err
	}

	return cli, nil
}

func newEtcdSubnetRegistry(config *EtcdConfig, cliNewFunc etcdNewFunc) (Registry, error) {
	r := &etcdSubnetRegistry{
		etcdCfg:      config,
		networkRegex: regexp.MustCompile(config.Prefix + `/([^/]*)(/|/config)?$`),
	}
	if cliNewFunc != nil {
		r.cliNewFunc = cliNewFunc
	} else {
		r.cliNewFunc = newEtcdClient
	}

	var err error
	r.cli, err = r.cliNewFunc(config)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (esr *etcdSubnetRegistry) getNetworkConfig(ctx context.Context) (string, error) {
	key := path.Join(esr.etcdCfg.Prefix, "config")
	resp, err := esr.client().Get(ctx, key)
	if err != nil {
		return "", err
	}
	return string(resp.Kvs[0].Value), nil
}

// getSubnets queries etcd to get a list of currently allocated leases for a given network.
// It returns the leases along with the "as-of" etcd-index that can be used as the starting
// point for etcd watch.
func (esr *etcdSubnetRegistry) getSubnets(ctx context.Context) ([]Lease, uint64, error) {
	key := path.Join(esr.etcdCfg.Prefix, "subnets")
	resp, err := esr.client().Get(ctx, key, etcd.WithPrefix())
	if err != nil {
		return nil, 0, err
	}

	leases := []Lease{}
	for _, node := range resp.Kvs {
		l, err := nodeToLease(node)
		if err != nil {
			log.Warningf("Ignoring bad subnet node: %v", err)
			continue
		}

		leases = append(leases, *l)
	}

	return leases, uint64(resp.Header.Revision), nil
}

func (esr *etcdSubnetRegistry) getSubnet(ctx context.Context, sn ip.IP4Net, sn6 ip.IP6Net) (*Lease, uint64, error) {
	key := path.Join(esr.etcdCfg.Prefix, "subnets", MakeSubnetKey(sn, sn6))
	resp, err := esr.client().Get(ctx, key)
	if err != nil {
		return nil, 0, err
	}
	if len(resp.Kvs) == 0 {
		return nil, 0, ErrKeyNotFound
	}
	l, err := nodeToLease(resp.Kvs[0])
	return l, uint64(resp.Header.Revision), err
}

func (esr *etcdSubnetRegistry) createSubnet(ctx context.Context, sn ip.IP4Net, sn6 ip.IP6Net, attrs *LeaseAttrs, ttl time.Duration) (time.Time, error) {
	key := path.Join(esr.etcdCfg.Prefix, "subnets", MakeSubnetKey(sn, sn6))
	value, err := json.Marshal(attrs)
	if err != nil {
		return time.Time{}, err
	}

	lease := etcd.NewLease(esr.cli)
	leasResp, err := lease.Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		return time.Time{}, err
	}

	txn := esr.cli.Txn(ctx)
	resp, err := txn.If(etcd.Compare(etcd.CreateRevision(key), "=", 0)).
		Then(etcd.OpPut(key, string(value), etcd.WithLease(leasResp.ID))).
		Commit()

	if err != nil {
		return time.Time{}, err
	}
	// TODO key已存在,验证与etcd v2 错误区别
	if !resp.Succeeded {
		return time.Time{}, ErrKeyAlreadyExists
	}

	return time.Now().Add(ttl), nil
}

func (esr *etcdSubnetRegistry) updateSubnet(ctx context.Context, sn ip.IP4Net, sn6 ip.IP6Net, attrs *LeaseAttrs, ttl time.Duration, asof uint64) (time.Time, error) {
	return time.Time{}, nil
	//key := path.Join(esr.etcdCfg.Prefix, "subnets", MakeSubnetKey(sn, sn6))
	//value, err := json.Marshal(attrs)
	//if err != nil {
	//	return time.Time{}, err
	//}
	//resp, err := esr.client().Set(ctx, key, string(value), &etcd.SetOptions{
	//	PrevIndex: asof,
	//	TTL:       ttl,
	//})
	//if err != nil {
	//	return time.Time{}, err
	//}
	//
	//exp := time.Time{}
	//if resp.Node.Expiration != nil {
	//	exp = *resp.Node.Expiration
	//}
	//
	//return exp, nil
}

func (esr *etcdSubnetRegistry) deleteSubnet(ctx context.Context, sn ip.IP4Net, sn6 ip.IP6Net) error {
	return nil
	//key := path.Join(esr.etcdCfg.Prefix, "subnets", MakeSubnetKey(sn, sn6))
	//_, err := esr.client().Delete(ctx, key, nil)
	//return err
}

func (esr *etcdSubnetRegistry) watchSubnets(ctx context.Context, since uint64) (Event, uint64, error) {
	return Event{}, 0, nil
	//key := path.Join(esr.etcdCfg.Prefix, "subnets")
	//opts := &etcd.WatcherOptions{
	//	AfterIndex: since,
	//	Recursive:  true,
	//}
	//e, err := esr.client().Watcher(key, opts).Next(ctx)
	//if err != nil {
	//	return Event{}, 0, err
	//}
	//
	//evt, err := parseSubnetWatchResponse(e)
	//return evt, e.Node.ModifiedIndex, err
}

func (esr *etcdSubnetRegistry) watchSubnet(ctx context.Context, since uint64, sn ip.IP4Net, sn6 ip.IP6Net) (Event, uint64, error) {
	return Event{}, 0, nil
	//key := path.Join(esr.etcdCfg.Prefix, "subnets", MakeSubnetKey(sn, sn6))
	//opts := &etcd.WatcherOptions{
	//	AfterIndex: since,
	//}
	//
	//e, err := esr.client().Watcher(key, opts).Next(ctx)
	//if err != nil {
	//	return Event{}, 0, err
	//}
	//
	//evt, err := parseSubnetWatchResponse(e)
	//return evt, e.Node.ModifiedIndex, err
}

func (esr *etcdSubnetRegistry) client() *etcd.Client {
	return nil
	//esr.mux.Lock()
	//defer esr.mux.Unlock()
	//return esr.cli
}

func parseSubnetWatchResponse(resp *etcd.GetResponse) (Event, error) {
	return Event{}, nil
	//sn, tsn6 := ParseSubnetKey(resp.Node.Key)
	//if sn == nil {
	//	return Event{}, fmt.Errorf("%v %q: not a subnet, skipping", resp.Action, resp.Node.Key)
	//}
	//
	//var sn6 ip.IP6Net
	//if tsn6 != nil {
	//	sn6 = *tsn6
	//}
	//
	//switch resp.Action {
	//case "delete", "expire":
	//	return Event{
	//		Type: EventRemoved,
	//		Lease: Lease{
	//			EnableIPv4: true,
	//			Subnet:     *sn,
	//			EnableIPv6: !sn6.Empty(),
	//			IPv6Subnet: sn6,
	//		},
	//	}, nil
	//
	//default:
	//	attrs := &LeaseAttrs{}
	//	err := json.Unmarshal([]byte(resp.Node.Value), attrs)
	//	if err != nil {
	//		return Event{}, err
	//	}
	//
	//	exp := time.Time{}
	//	if resp.Node.Expiration != nil {
	//		exp = *resp.Node.Expiration
	//	}
	//
	//	evt := Event{
	//		Type: EventAdded,
	//		Lease: Lease{
	//			EnableIPv4: true,
	//			Subnet:     *sn,
	//			EnableIPv6: !sn6.Empty(),
	//			IPv6Subnet: sn6,
	//			Attrs:      *attrs,
	//			Expiration: exp,
	//		},
	//	}
	//	return evt, nil
	//}
}

func nodeToLease(node *mvccpb.KeyValue) (*Lease, error) {
	return nil, nil
	//sn, tsn6 := ParseSubnetKey(node.Key)
	//if sn == nil {
	//	return nil, fmt.Errorf("failed to parse subnet key %s", node.Key)
	//}
	//
	//var sn6 ip.IP6Net
	//if tsn6 != nil {
	//	sn6 = *tsn6
	//}
	//
	//attrs := &LeaseAttrs{}
	//if err := json.Unmarshal([]byte(node.Value), attrs); err != nil {
	//	return nil, err
	//}
	//
	//exp := time.Time{}
	//if node.Expiration != nil {
	//	exp = *node.Expiration
	//}
	//
	//lease := Lease{
	//	EnableIPv4: true,
	//	EnableIPv6: !sn6.Empty(),
	//	Subnet:     *sn,
	//	IPv6Subnet: sn6,
	//	Attrs:      *attrs,
	//	Expiration: exp,
	//	Asof:       node.ModifiedIndex,
	//}
	//
	//return &lease, nil
}
