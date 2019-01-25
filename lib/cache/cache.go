/*
Copyright 2018-2019 Gravitational, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cache

import (
	"context"
	"sync"
	"time"

	"github.com/gravitational/teleport"
	"github.com/gravitational/teleport/lib/backend"
	"github.com/gravitational/teleport/lib/defaults"
	"github.com/gravitational/teleport/lib/services"
	"github.com/gravitational/teleport/lib/services/local"

	"github.com/gravitational/trace"
	log "github.com/sirupsen/logrus"
)

// ForAuth sets up watch configuration for the auth server
func ForAuth(cfg Config) Config {
	cfg.Watches = []services.WatchKind{
		{Kind: services.KindCertAuthority, LoadSecrets: true},
		{Kind: services.KindStaticTokens},
		{Kind: services.KindToken},
		{Kind: services.KindClusterName},
		{Kind: services.KindClusterConfig},
	}
	return cfg
}

// ForProxy sets up watch configuration for proxy
func ForProxy(cfg Config) Config {
	cfg.Watches = []services.WatchKind{
		{Kind: services.KindCertAuthority, LoadSecrets: false},
		{Kind: services.KindClusterName},
		{Kind: services.KindClusterConfig},
		{Kind: services.KindUser},
		{Kind: services.KindRole},
		{Kind: services.KindNamespace},
		{Kind: services.KindNode},
		{Kind: services.KindProxy},
		{Kind: services.KindReverseTunnel},
		{Kind: services.KindTunnelConnection},
	}
	return cfg
}

// ForNode sets up watch configuration for node
func ForNode(cfg Config) Config {
	cfg.Watches = []services.WatchKind{
		{Kind: services.KindCertAuthority, LoadSecrets: false},
		{Kind: services.KindClusterName},
		{Kind: services.KindClusterConfig},
		{Kind: services.KindUser},
		{Kind: services.KindRole},
		{Kind: services.KindNamespace},
	}
	return cfg
}

// Cache implements auth.AccessPoint interface and remembers
// the previously returned upstream value for each API call.
//
// This which can be used if the upstream AccessPoint goes offline
type Cache struct {
	sync.RWMutex
	Config
	*log.Entry
	ctx                context.Context
	cancel             context.CancelFunc
	trustCache         services.Trust
	clusterConfigCache services.ClusterConfiguration
	provisionerCache   services.Provisioner
	usersCache         services.UsersService
	accessCache        services.Access
	presenceCache      services.Presence
	collections        map[string]collection
}

// Config is Cache config
type Config struct {
	// Context is context for parent operations
	Context context.Context
	// Watches provides a list of resources
	// for the cache to watch
	Watches []services.WatchKind
	// Events provides events watchers
	Events services.Events
	// Trust is a service providing information about certificate
	// authorities
	Trust services.Trust
	// ClusterConfig is a cluster configuration service
	ClusterConfig services.ClusterConfiguration
	// Provisioner is a provisioning service
	Provisioner services.Provisioner
	// Users is a users service
	Users services.UsersService
	// Access is an access service
	Access services.Access
	// Presence is a presence service
	Presence services.Presence
	// Backend is a backend for local cache
	Backend backend.Backend
	// RetryPeriod is a period between cache retries on failures
	RetryPeriod time.Duration
	// ReloadPeriod is a period when cache performs full reload
	ReloadPeriod time.Duration
	// EventsC is a channel for event notifications,
	// used in tests
	EventsC chan CacheEvent
}

// CheckAndSetDefaults checks parameters and sets default values
func (c *Config) CheckAndSetDefaults() error {
	if c.Context == nil {
		c.Context = context.Background()
	}
	if c.Events == nil {
		return trace.BadParameter("missing Events parameter")
	}
	if c.Backend == nil {
		return trace.BadParameter("missing Backend parameter")
	}
	if c.RetryPeriod == 0 {
		c.RetryPeriod = defaults.HighResPollingPeriod
	}
	if c.ReloadPeriod == 0 {
		c.ReloadPeriod = defaults.LowResPollingPeriod
	}
	return nil
}

// CacheEvent is event used in tests
type CacheEvent struct {
	// Type is event type
	Type string
	// Event is event processed
	// by the event cycle
	Event services.Event
}

const (
	// EventProcessed is emitted whenever event is processed
	EventProcessed = "event_processed"
	// WatcherStarted is emitted when a new event watcher is started
	WatcherStarted = "watcher_started"
)

// New creates a new instance of Cache
func New(config Config) (*Cache, error) {
	if err := config.CheckAndSetDefaults(); err != nil {
		return nil, trace.Wrap(err)
	}
	ctx, cancel := context.WithCancel(config.Context)
	cs := &Cache{
		ctx:                ctx,
		cancel:             cancel,
		Config:             config,
		trustCache:         local.NewCAService(config.Backend),
		clusterConfigCache: local.NewClusterConfigurationService(config.Backend),
		provisionerCache:   local.NewProvisioningService(config.Backend),
		usersCache:         local.NewIdentityService(config.Backend),
		accessCache:        local.NewAccessService(config.Backend),
		presenceCache:      local.NewPresenceService(config.Backend),
		Entry: log.WithFields(log.Fields{
			trace.Component: teleport.ComponentCachingClient,
		}),
	}
	collections, err := setupCollections(cs, config.Watches)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	cs.collections = collections
	err = cs.fetch()
	if err != nil {
		return nil, trace.Wrap(err)
	}
	go cs.update()
	return cs, nil
}

func (c *Cache) update() {
	t := time.NewTicker(c.RetryPeriod)
	defer t.Stop()

	r := time.NewTicker(c.ReloadPeriod)
	defer r.Stop()
	for {
		select {
		case <-t.C:
		case <-r.C:
		case <-c.ctx.Done():
			return
		}
		err := c.fetchAndWatch()
		if err != nil {
			c.Warningf("Going to re-init the cache because of the error: %v.", err)
		}
	}
}

func (c *Cache) notify(event CacheEvent) {
	if c.EventsC == nil {
		return
	}
	select {
	case c.EventsC <- event:
		return
	case <-c.ctx.Done():
		return
	}
}

// fetchAndWatch keeps cache up to date by replaying
// events and syncing local cache storage.
//
// Here are some thoughts on consistency in face of errors:
//
// 1. Every client is connected to the database fan-out
// system. This system creates a buffered channel for every
// client and tracks the channel overflow. Thanks to channels every client gets its
// own unique iterator over the event stream. If client looses connection
// or fails to keep up with the stream, the server will terminate
// the channel and client will have to re-initialize.
//
// 2. Replays of stale events. Etcd provides a strong
// mechanism to track the versions of the storage - revisions
// of every operation that are uniquely numbered and monothonically
// and consistently ordered thanks to Raft. Unfortunately, DynamoDB
// does not provide such a mechanism for its event system, so
// some tradeofs have to be made:
//   a. We assume that events are ordered in regards to the
//   individual key operations which is the guarantees both Etcd and DynamodDB
//   provide.
//   b. Thanks to the init event sent by the server on a sucessfull connect,
//   and guarantees 1 and 2a, client assumes that once it connects and receives an event,
//   it will not miss any events, however it can receive stale events.
//   Event could be stale, if it relates to a change that happened before
//   the version read by client from the database, for example,
//   given the event stream: 1. Update a=1 2. Delete a 3. Put a = 2
//   Client could have subscribed before event 1 happened,
//   read the value a=2 and then received events 1 and 2 and 3.
//   The cache will replay all events 1, 2 and 3 and end up in the correct
//   state 3. If we had a consistent revision number, we could
//   have skipped 1 and 2, but in the absense of such mechanism in Dynamo
//   we assume that this cache will eventually end up in a correct state
//   potentially lagging behind the state of the database.
//
func (c *Cache) fetchAndWatch() error {
	watcher, err := c.Events.NewWatcher(c.ctx, services.Watch{Kinds: c.watchKinds()})
	if err != nil {
		return trace.Wrap(err)
	}
	defer watcher.Close()
	// before fetch, make sure watcher is synced by receiving init event,
	// to avoid the scenario:
	// 1. Cache process:   w = NewWatcher()
	// 2. Cache process:   c.fetch()
	// 3. Backend process: addItem()
	// 4. Cache process:   <- w.Events()
	//
	// If there is a way that NewWatcher() on line 1 could
	// return without subscription established first,
	// Code line 3 could execute and line 4 could miss event,
	// wrapping up with out of sync replica.
	// To avoid this, before doing fetch,
	// cache process makes sure the connection is established
	// by receiving init event first.
	select {
	case <-watcher.Done():
		if err != nil {
			return trace.Wrap(watcher.Error())
		}
		return trace.ConnectionProblem(nil, "unexpected watcher close")
	case <-c.ctx.Done():
		return trace.ConnectionProblem(c.ctx.Err(), "context is closing")
	case event := <-watcher.Events():
		if event.Type != backend.OpInit {
			return trace.BadParameter("expected init event, got %v instead", event.Type)
		}
	}
	err = c.fetch()
	if err != nil {
		return trace.Wrap(err)
	}
	c.notify(CacheEvent{Type: WatcherStarted})
	for {
		select {
		case <-watcher.Done():
			if err != nil {
				return trace.Wrap(watcher.Error())
			}
			return trace.ConnectionProblem(nil, "unexpected watcher close")
		case <-c.ctx.Done():
			return trace.ConnectionProblem(c.ctx.Err(), "context is closing")
		case event := <-watcher.Events():
			err = c.processEvent(event)
			if err != nil {
				return trace.Wrap(err)
			}
			c.notify(CacheEvent{Event: event, Type: EventProcessed})
		}
	}
}

func (c *Cache) watchKinds() []services.WatchKind {
	out := make([]services.WatchKind, 0, len(c.collections))
	for _, collection := range c.collections {
		out = append(out, collection.watchKind())
	}
	return out
}

// Close closes all outstanding and active cache operations
func (c *Cache) Close() error {
	c.cancel()
	return nil
}

func (c *Cache) fetch() error {
	for _, collection := range c.collections {
		if err := collection.fetch(); err != nil {
			return trace.Wrap(err)
		}
	}
	return nil
}

func (c *Cache) processEvent(event services.Event) error {
	collection, ok := c.collections[event.Resource.GetKind()]
	if !ok {
		c.Warningf("Skipping unsupported event %v.", event.Resource.GetKind())
		return nil
	}
	return collection.processEvent(event)
}

// GetCertAuthority returns certificate authority by given id. Parameter loadSigningKeys
// controls if signing keys are loaded
func (c *Cache) GetCertAuthority(id services.CertAuthID, loadSigningKeys bool, opts ...services.MarshalOption) (services.CertAuthority, error) {
	return c.trustCache.GetCertAuthority(id, loadSigningKeys, opts...)
}

// GetCertAuthorities returns a list of authorities of a given type
// loadSigningKeys controls whether signing keys should be loaded or not
func (c *Cache) GetCertAuthorities(caType services.CertAuthType, loadSigningKeys bool, opts ...services.MarshalOption) ([]services.CertAuthority, error) {
	return c.trustCache.GetCertAuthorities(caType, loadSigningKeys, opts...)
}

// GetStaticTokens gets the list of static tokens used to provision nodes.
func (c *Cache) GetStaticTokens() (services.StaticTokens, error) {
	return c.clusterConfigCache.GetStaticTokens()
}

// GetTokens returns all active (non-expired) provisioning tokens
func (c *Cache) GetTokens(opts ...services.MarshalOption) ([]services.ProvisionToken, error) {
	return c.provisionerCache.GetTokens(opts...)
}

// GetToken finds and returns token by ID
func (c *Cache) GetToken(token string) (services.ProvisionToken, error) {
	return c.provisionerCache.GetToken(token)
}

// GetClusterConfig gets services.ClusterConfig from the backend.
func (c *Cache) GetClusterConfig(opts ...services.MarshalOption) (services.ClusterConfig, error) {
	return c.clusterConfigCache.GetClusterConfig(opts...)
}

// GetClusterName gets the name of the cluster from the backend.
func (c *Cache) GetClusterName(opts ...services.MarshalOption) (services.ClusterName, error) {
	return c.clusterConfigCache.GetClusterName(opts...)
}

// GetRoles is a part of auth.AccessPoint implementation
func (c *Cache) GetRoles() ([]services.Role, error) {
	return c.accessCache.GetRoles()
}

// GetRole is a part of auth.AccessPoint implementation
func (c *Cache) GetRole(name string) (services.Role, error) {
	return c.accessCache.GetRole(name)
}

// GetNamespace returns namespace
func (c *Cache) GetNamespace(name string) (*services.Namespace, error) {
	return c.presenceCache.GetNamespace(name)
}

// GetNamespaces is a part of auth.AccessPoint implementation
func (c *Cache) GetNamespaces() ([]services.Namespace, error) {
	return c.presenceCache.GetNamespaces()
}

// GetNodes is a part of auth.AccessPoint implementation
func (c *Cache) GetNodes(namespace string, opts ...services.MarshalOption) ([]services.Server, error) {
	return c.presenceCache.GetNodes(namespace, opts...)
}

// GetProxies is a part of auth.AccessPoint implementation
func (c *Cache) GetReverseTunnels() ([]services.ReverseTunnel, error) {
	return c.presenceCache.GetReverseTunnels()
}

// GetProxies is a part of auth.AccessPoint implementation
func (c *Cache) GetProxies() ([]services.Server, error) {
	return c.presenceCache.GetProxies()
}

// GetUser is a part of auth.AccessPoint implementation.
func (c *Cache) GetUser(name string) (user services.User, err error) {
	return c.usersCache.GetUser(name)
}

// GetUsers is a part of auth.AccessPoint implementation
func (c *Cache) GetUsers() (users []services.User, err error) {
	return c.usersCache.GetUsers()
}

// GetTunnelConnections is a part of auth.AccessPoint implementation
// GetTunnelConnections are not using recent cache as they are designed
// to be called periodically and always return fresh data
func (c *Cache) GetTunnelConnections(clusterName string, opts ...services.MarshalOption) ([]services.TunnelConnection, error) {
	return c.presenceCache.GetTunnelConnections(clusterName, opts...)
}

// GetAllTunnelConnections is a part of auth.AccessPoint implementation
// GetAllTunnelConnections are not using recent cache, as they are designed
// to be called periodically and always return fresh data
func (c *Cache) GetAllTunnelConnections(opts ...services.MarshalOption) (conns []services.TunnelConnection, err error) {
	return c.presenceCache.GetAllTunnelConnections(opts...)
}
