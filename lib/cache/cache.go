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
}

// Config is Cache config
type Config struct {
	// Context is context for parent operations
	Context context.Context
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
	if c.Trust == nil {
		return trace.BadParameter("missing Trust parameter")
	}
	if c.ClusterConfig == nil {
		return trace.BadParameter("missing ClusterConfig parameter")
	}
	if c.Provisioner == nil {
		return trace.BadParameter("missing Provisioner parameter")
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
		Entry: log.WithFields(log.Fields{
			trace.Component: teleport.ComponentCachingClient,
		}),
	}
	err := cs.fetch()
	if err != nil {
		return nil, trace.Wrap(err)
	}
	go cs.update()
	return cs, nil
}

// instance is a cache instance,
type instance struct {
	parent *Cache
	services.Trust
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
	watcher, err := c.Events.NewWatcher(c.ctx, services.Watch{
		Kinds: []services.WatchKind{
			{Kind: services.KindCertAuthority, LoadSecrets: true},
			{Kind: services.KindStaticTokens},
			{Kind: services.KindToken},
			{Kind: services.KindClusterName},
			{Kind: services.KindClusterConfig},
			//
			{Kind: services.KindUser},
			{Kind: services.KindRole},
			{Kind: services.KindNamespace},
			{Kind: services.KindNode},
			{Kind: services.KindProxy},
			{Kind: services.KindReverseTunnel},
			{Kind: services.KindTunnelConnection},
		},
	})
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

// Close closes all outstanding and active cache operations
func (c *Cache) Close() error {
	c.cancel()
	return nil
}

func (c *Cache) fetch() error {
	if err := c.updateCertAuthorities(services.HostCA); err != nil {
		return trace.Wrap(err)
	}
	if err := c.updateCertAuthorities(services.UserCA); err != nil {
		return trace.Wrap(err)
	}
	if err := c.updateStaticTokens(); err != nil {
		return trace.Wrap(err)
	}
	if err := c.updateTokens(); err != nil {
		return trace.Wrap(err)
	}
	if err := c.updateClusterConfig(); err != nil {
		return trace.Wrap(err)
	}
	if err := c.updateClusterName(); err != nil {
		return trace.Wrap(err)
	}
	if err := c.updateUsers(); err != nil {
		return trace.Wrap(err)
	}
	if err := c.updateRoles(); err != nil {
		return trace.Wrap(err)
	}
	if err := c.updateNamespaces(); err != nil {
		return trace.Wrap(err)
	}
	if err := c.updateNodes(); err != nil {
		return trace.Wrap(err)
	}
	if err := c.updateProxies(); err != nil {
		return trace.Wrap(err)
	}
	if err = c.updateReverseTunnels(); err != nil {
		return trace.Wrap(err)
	}
	if err := c.updateTunnelConnections(); err != nil {
		return trace.Wrap(err)
	}
	return nil
}

func (c *Cache) processEvent(event services.Event) error {
	switch event.Type {
	case backend.OpDelete:
		switch event.Resource.GetKind() {
		case services.KindToken:
			err := c.provisionerCache.DeleteToken(event.Resource.GetName())
			if err != nil {
				c.Warningf("Failed to delete provisioning token %v.", err)
				return trace.Wrap(err)
			}
		case services.KindClusterConfig:
			err := c.clusterConfigCache.DeleteClusterConfig()
			if err != nil {
				c.Warningf("Failed to delete cluster config %v.", err)
				return trace.Wrap(err)
			}
		case services.KindClusterName:
			err := c.clusterConfigCache.DeleteClusterName()
			if err != nil {
				c.Warningf("Failed to delete cluster name %v.", err)
				return trace.Wrap(err)
			}
		case services.KindStaticTokens:
			err := c.clusterConfigCache.DeleteStaticTokens()
			if err != nil {
				c.Warningf("Failed to delete static tokens %v.", err)
				return trace.Wrap(err)
			}
		case services.KindCertAuthority:
			err := c.trustCache.DeleteCertAuthority(services.CertAuthID{
				Type:       services.CertAuthType(event.Resource.GetSubKind()),
				DomainName: event.Resource.GetName(),
			})
			if err != nil {
				c.Warningf("Failed to delete cert authority %v.", err)
				return trace.Wrap(err)
			}
		case services.KindUser:
			err := c.usersCache.DeleteUser(event.Resource.GetName())
			if err != nil {
				c.Warningf("Failed to delete user %v.", err)
				return trace.Wrap(err)
			}
		case services.KindRole:
			err := c.accessCache.DeleteRole(event.Resource.GetName())
			if err != nil {
				c.Warningf("Failed to delete role %v.", err)
				return trace.Wrap(err)
			}
		case services.KindNamespace:
			err := c.presenceCache.DeleteNamespace(event.Resource.GetName())
			if err != nil {
				c.Warningf("Failed to delete namespace %v.", err)
				return trace.Wrap(err)
			}
		case services.KindNode:
			err := c.presenceCache.DeleteNode(event.Resource.GetNamespace(), event.Resource.GetName())
			if err != nil {
				c.Warningf("Failed to delete resource %v.", err)
				return trace.Wrap(err)
			}
		case services.KindProxy:
			err := c.presenceCache.DeleteProxy(event.Resource.GetName())
			if err != nil {
				c.Warningf("Failed to delete resource %v.", err)
				return trace.Wrap(err)
			}
		case services.KindReverseTunnel:
			err := c.presenceCache.DeleteReverseTunnel(event.Resource.GetName())
			if err != nil {
				c.Warningf("Failed to delete resource %v.", err)
				return trace.Wrap(err)
			}
		case services.KindTunnelConnection:
			err := c.presenceCache.DeleteTunnelConnection(event.Resource.GetSubKind(), event.Resource.GetName())
			if err != nil {
				c.Warningf("Failed to delete resource %v.", err)
				return trace.Wrap(err)
			}
		default:
			c.Debugf("Skipping unsupported resource %v", event.Resource.GetKind())
		}
	case backend.OpPut:
		switch resource := event.Resource.(type) {
		case services.StaticTokens:
			if err := c.clusterConfigCache.SetStaticTokens(resource); err != nil {
				return trace.Wrap(err)
			}
		case services.ProvisionToken:
			if err := c.provisionerCache.UpsertToken(resource); err != nil {
				return trace.Wrap(err)
			}
		case services.CertAuthority:
			if err := c.trustCache.UpsertCertAuthority(resource); err != nil {
				return trace.Wrap(err)
			}
		case services.ClusterConfig:
			if err := c.clusterConfigCache.SetClusterConfig(resource); err != nil {
				return trace.Wrap(err)
			}
		case services.ClusterName:
			if err := c.clusterConfigCache.UpsertClusterName(resource); err != nil {
				return trace.Wrap(err)
			}
		case services.User:
			if err := c.usersCache.UpsertUser(resource); err != nil {
				return trace.Wrap(err)
			}
		case services.Role:
			if err := c.accessCache.UpsertRole(resource); err != nil {
				return trace.Wrap(err)
			}
		case services.Namespace:
			if err := c.accessCache.UpsertNamespace(resource); err != nil {
				return trace.Wrap(err)
			}
		case services.Server:
			switch resource.GetKind() {
			case services.KindNode:
				if err := c.presenceCache.UpsertNode(resource); err != nil {
					return trace.Wrap(err)
				}
			case services.KindProxy:
				if err := c.presenceCache.UpsertProxy(resource); err != nil {
					return trace.Wrap(err)
				}
			default:
				c.Warningf("Skipping unsupported resource %v.", event.Resource.GetKind())
			}
		case services.ReverseTunnel:
			if err := c.presenceCache.UpsertReverseTunnel(resource); err != nil {
				return trace.Wrap(err)
			}
		case services.TunnelConnection:
			if err := c.presenceCache.UpsertTunnelConnection(resource); err != nil {
				return trace.Wrap(err)
			}
		default:
			c.Warningf("Skipping unsupported resource %v.", event.Resource.GetKind())
		}
	default:
		c.Warningf("Skipping unsupported event type %v.", event.Type)
	}
	return nil
}

func (c *Cache) updateCertAuthorities(caType services.CertAuthType) error {
	authorities, err := c.Trust.GetCertAuthorities(caType, true, services.SkipValidation())
	if err != nil {
		return trace.Wrap(err)
	}
	if err := c.trustCache.DeleteAllCertAuthorities(caType); err != nil {
		if !trace.IsNotFound(err) {
			return trace.Wrap(err)
		}
	}
	for _, ca := range authorities {
		if err := c.trustCache.UpsertCertAuthority(ca); err != nil {
			return trace.Wrap(err)
		}
	}
	return nil
}

func (c *Cache) updateStaticTokens() error {
	staticTokens, err := c.ClusterConfig.GetStaticTokens()
	if err != nil {
		if !trace.IsNotFound(err) {
			return trace.Wrap(err)
		}
		err := c.clusterConfigCache.DeleteStaticTokens()
		if err != nil {
			if !trace.IsNotFound(err) {
				return trace.Wrap(err)
			}
		}
		return nil
	}
	err = c.clusterConfigCache.SetStaticTokens(staticTokens)
	if err != nil {
		return trace.Wrap(err)
	}
	return nil
}

func (c *Cache) updateTokens() error {
	tokens, err := c.Provisioner.GetTokens()
	if err != nil {
		return trace.Wrap(err)
	}
	if err := c.provisionerCache.DeleteAllTokens(); err != nil {
		if !trace.IsNotFound(err) {
			return trace.Wrap(err)
		}
	}
	for _, token := range tokens {
		if err := c.provisionerCache.UpsertToken(token); err != nil {
			return trace.Wrap(err)
		}
	}
	return nil
}

func (c *Cache) updateClusterConfig() error {
	clusterConfig, err := c.ClusterConfig.GetClusterConfig()
	if err != nil {
		if !trace.IsNotFound(err) {
			return trace.Wrap(err)
		}
		err := c.clusterConfigCache.DeleteClusterConfig()
		if err != nil {
			if !trace.IsNotFound(err) {
				return trace.Wrap(err)
			}
		}
		return nil
	}
	if err := c.clusterConfigCache.SetClusterConfig(clusterConfig); err != nil {
		if !trace.IsNotFound(err) {
			return trace.Wrap(err)
		}
	}
	return nil
}

func (c *Cache) updateClusterName() error {
	clusterName, err := c.ClusterConfig.GetClusterName()
	if err != nil {
		if !trace.IsNotFound(err) {
			return trace.Wrap(err)
		}
		err := c.clusterConfigCache.DeleteClusterName()
		if err != nil {
			if !trace.IsNotFound(err) {
				return trace.Wrap(err)
			}
		}
		return nil
	}
	if err := c.clusterConfigCache.UpsertClusterName(clusterName); err != nil {
		if !trace.IsNotFound(err) {
			return trace.Wrap(err)
		}
	}
	return nil
}

func (c *Cache) updateUsers() error {
	values, err := c.Users.GetUsers()
	if err != nil {
		return trace.Wrap(err)
	}
	if err := c.provisionerCache.DeleteAllUsers(); err != nil {
		if !trace.IsNotFound(err) {
			return trace.Wrap(err)
		}
	}
	for _, token := range tokens {
		if err := c.provisionerCache.UpsertToken(token); err != nil {
			return trace.Wrap(err)
		}
	}
	return nil
}

func (c *Cache) updateRoles() error {
	return trace.Wrap(err)
}

func (c *Cache) updateNamespaces() error {
	return trace.Wrap(err)
}

func (c *Cache) updateNodes() error {
	return trace.Wrap(err)
}

func (c *Cache) updateProxies() error {
	return trace.Wrap(err)
}

func (c *Cache) updateReverseTunnels() error {
	return trace.Wrap(err)
}

func (c *Cache) updateTunnelConnections() error {
	return trace.Wrap(err)
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
