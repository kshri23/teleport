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
	"testing"
	"time"

	"github.com/gravitational/teleport"
	"github.com/gravitational/teleport/lib/backend"
	"github.com/gravitational/teleport/lib/backend/lite"
	"github.com/gravitational/teleport/lib/fixtures"
	"github.com/gravitational/teleport/lib/services"
	"github.com/gravitational/teleport/lib/services/local"
	"github.com/gravitational/teleport/lib/services/suite"
	"github.com/gravitational/teleport/lib/utils"

	"github.com/gravitational/trace"
	"github.com/jonboulle/clockwork"
	"gopkg.in/check.v1"
)

type CacheSuite struct {
	dataDir      string
	backend      backend.Backend
	clock        clockwork.Clock
	eventsC      chan CacheEvent
	cache        *Cache
	cacheBackend backend.Backend

	eventsS        *proxyEvents
	trustS         services.Trust
	provisionerS   services.Provisioner
	clusterConfigS services.ClusterConfiguration
}

var _ = check.Suite(&CacheSuite{})

// bootstrap check
func TestState(t *testing.T) { check.TestingT(t) }

func (s *CacheSuite) SetUpSuite(c *check.C) {
	utils.InitLoggerForTests(testing.Verbose())
	s.clock = clockwork.NewRealClock()
}

func (s *CacheSuite) SetUpTest(c *check.C) {
	// create a new auth server:
	s.dataDir = c.MkDir()
	var err error
	s.backend, err = lite.NewWithConfig(context.TODO(), lite.Config{Path: s.dataDir, PollStreamPeriod: 200 * time.Millisecond})
	c.Assert(err, check.IsNil)

	cacheDir := c.MkDir()
	s.cacheBackend, err = lite.NewWithConfig(context.TODO(), lite.Config{Path: cacheDir, EventsOff: true})
	c.Assert(err, check.IsNil)

	s.eventsC = make(chan CacheEvent, 100)
	ctx := context.TODO()

	s.trustS = local.NewCAService(s.backend)
	s.clusterConfigS = local.NewClusterConfigurationService(s.backend)
	s.provisionerS = local.NewProvisioningService(s.backend)
	s.eventsS = &proxyEvents{events: local.NewEventsService(s.backend)}
	s.cache, err = New(Config{
		Context:       ctx,
		Backend:       s.cacheBackend,
		Events:        s.eventsS,
		ClusterConfig: s.clusterConfigS,
		Provisioner:   s.provisionerS,
		Trust:         s.trustS,
		RetryPeriod:   200 * time.Millisecond,
		EventsC:       s.eventsC,
	})
	c.Assert(err, check.IsNil)
	c.Assert(s.cache, check.NotNil)

	select {
	case <-s.eventsC:
	case <-time.After(time.Second):
		c.Fatalf("wait for the watcher to start")
	}
}

func (s *CacheSuite) TearDownTest(c *check.C) {
	if s.backend != nil {
		s.backend.Close()
	}
	if s.cache != nil {
		s.cache.Close()
	}
}

// TestCA tests certificate authorities
func (s *CacheSuite) TestCA(c *check.C) {
	ca := suite.NewTestCA(services.UserCA, "example.com")
	c.Assert(s.trustS.UpsertCertAuthority(ca), check.IsNil)

	select {
	case <-s.eventsC:
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}

	out, err := s.cache.GetCertAuthority(ca.GetID(), true)
	c.Assert(err, check.IsNil)
	ca.SetResourceID(out.GetResourceID())
	fixtures.DeepCompare(c, ca, out)

	err = s.trustS.DeleteCertAuthority(ca.GetID())
	c.Assert(err, check.IsNil)

	select {
	case <-s.eventsC:
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}

	_, err = s.cache.GetCertAuthority(ca.GetID(), false)
	fixtures.ExpectNotFound(c, err)
}

// TestRecovery tests error recovery scenario
func (s *CacheSuite) TestRecovery(c *check.C) {
	ca := suite.NewTestCA(services.UserCA, "example.com")
	c.Assert(s.trustS.UpsertCertAuthority(ca), check.IsNil)

	select {
	case <-s.eventsC:
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}

	// event has arrived, now close the watchers
	watchers := s.eventsS.getWatchers()
	c.Assert(watchers, check.HasLen, 1)
	s.eventsS.closeWatchers()

	// wait for watcher to restart
	select {
	case event := <-s.eventsC:
		c.Assert(event.Type, check.Equals, WatcherStarted)
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}

	// add modification and expect the resource to recover
	ca2 := suite.NewTestCA(services.UserCA, "example2.com")
	c.Assert(s.trustS.UpsertCertAuthority(ca2), check.IsNil)

	// wait for watcher to receive an event
	select {
	case event := <-s.eventsC:
		c.Assert(event.Type, check.Equals, EventProcessed)
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}

	out, err := s.cache.GetCertAuthority(ca2.GetID(), false)
	c.Assert(err, check.IsNil)
	ca2.SetResourceID(out.GetResourceID())
	services.RemoveCASecrets(ca2)
	fixtures.DeepCompare(c, ca2, out)
}

// TestTokens tests static and dynamic tokens
func (s *CacheSuite) TestTokens(c *check.C) {
	staticTokens, err := services.NewStaticTokens(services.StaticTokensSpecV2{
		StaticTokens: []services.ProvisionTokenV1{
			{
				Token:   "static1",
				Roles:   teleport.Roles{teleport.RoleAuth, teleport.RoleNode},
				Expires: time.Now().UTC().Add(time.Hour),
			},
		},
	})
	c.Assert(err, check.IsNil)

	err = s.clusterConfigS.SetStaticTokens(staticTokens)
	c.Assert(err, check.IsNil)

	select {
	case event := <-s.eventsC:
		c.Assert(event.Type, check.Equals, EventProcessed)
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}

	out, err := s.cache.GetStaticTokens()
	c.Assert(err, check.IsNil)
	staticTokens.SetResourceID(out.GetResourceID())
	fixtures.DeepCompare(c, staticTokens, out)

	expires := time.Now().Add(10 * time.Hour).Truncate(time.Second).UTC()
	token, err := services.NewProvisionToken("token", teleport.Roles{teleport.RoleAuth, teleport.RoleNode}, expires)
	c.Assert(err, check.IsNil)

	err = s.provisionerS.UpsertToken(token)
	c.Assert(err, check.IsNil)

	select {
	case event := <-s.eventsC:
		c.Assert(event.Type, check.Equals, EventProcessed)
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}

	tout, err := s.cache.GetToken(token.GetName())
	c.Assert(err, check.IsNil)
	token.SetResourceID(tout.GetResourceID())
	fixtures.DeepCompare(c, token, tout)

	err = s.provisionerS.DeleteToken(token.GetName())
	c.Assert(err, check.IsNil)

	select {
	case event := <-s.eventsC:
		c.Assert(event.Type, check.Equals, EventProcessed)
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}

	_, err = s.cache.GetToken(token.GetName())
	fixtures.ExpectNotFound(c, err)
}

// TestClusterConfig tests cluster configuration
func (s *CacheSuite) TestClusterConfig(c *check.C) {
	// update cluster config to record at the proxy
	clusterConfig, err := services.NewClusterConfig(services.ClusterConfigSpecV3{
		SessionRecording: services.RecordAtProxy,
		Audit: services.AuditConfig{
			AuditEventsURI: []string{"dynamodb://audit_table_name", "file:///home/log"},
		},
	})
	c.Assert(err, check.IsNil)
	err = s.clusterConfigS.SetClusterConfig(clusterConfig)
	c.Assert(err, check.IsNil)

	clusterConfig, err = s.clusterConfigS.GetClusterConfig()
	c.Assert(err, check.IsNil)

	select {
	case event := <-s.eventsC:
		c.Assert(event.Type, check.Equals, EventProcessed)
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}

	out, err := s.cache.GetClusterConfig()
	c.Assert(err, check.IsNil)
	clusterConfig.SetResourceID(out.GetResourceID())
	fixtures.DeepCompare(c, clusterConfig, out)

	// update cluster name resource metadata
	clusterName, err := services.NewClusterName(services.ClusterNameSpecV2{
		ClusterName: "example.com",
	})
	c.Assert(err, check.IsNil)
	err = s.clusterConfigS.SetClusterName(clusterName)
	c.Assert(err, check.IsNil)

	clusterName, err = s.clusterConfigS.GetClusterName()
	c.Assert(err, check.IsNil)

	select {
	case event := <-s.eventsC:
		c.Assert(event.Type, check.Equals, EventProcessed)
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}

	c.Assert(err, check.IsNil)
	clusterName.SetResourceID(out.GetResourceID())
	fixtures.DeepCompare(c, clusterName, clusterName)
}

// TestUsersAndRoles tests users and roles
func (s *CacheSuite) TestUsersAndRoles(c *check.C) {
	// update cluster config to record at the proxy
	clusterConfig, err := services.NewClusterConfig(services.ClusterConfigSpecV3{
		SessionRecording: services.RecordAtProxy,
		Audit: services.AuditConfig{
			AuditEventsURI: []string{"dynamodb://audit_table_name", "file:///home/log"},
		},
	})
	c.Assert(err, check.IsNil)
	err = s.clusterConfigS.SetClusterConfig(clusterConfig)
	c.Assert(err, check.IsNil)

	clusterConfig, err = s.clusterConfigS.GetClusterConfig()
	c.Assert(err, check.IsNil)

	select {
	case event := <-s.eventsC:
		c.Assert(event.Type, check.Equals, EventProcessed)
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}

	out, err := s.cache.GetClusterConfig()
	c.Assert(err, check.IsNil)
	clusterConfig.SetResourceID(out.GetResourceID())
	fixtures.DeepCompare(c, clusterConfig, out)

	// update cluster name resource metadata
	clusterName, err := services.NewClusterName(services.ClusterNameSpecV2{
		ClusterName: "example.com",
	})
	c.Assert(err, check.IsNil)
	err = s.clusterConfigS.SetClusterName(clusterName)
	c.Assert(err, check.IsNil)

	clusterName, err = s.clusterConfigS.GetClusterName()
	c.Assert(err, check.IsNil)

	select {
	case event := <-s.eventsC:
		c.Assert(event.Type, check.Equals, EventProcessed)
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}

	c.Assert(err, check.IsNil)
	clusterName.SetResourceID(out.GetResourceID())
	fixtures.DeepCompare(c, clusterName, clusterName)
}

type proxyEvents struct {
	sync.Mutex
	watchers []services.Watcher
	events   services.Events
}

func (p *proxyEvents) getWatchers() []services.Watcher {
	p.Lock()
	defer p.Unlock()
	out := make([]services.Watcher, len(p.watchers))
	copy(out, p.watchers)
	return out
}

func (p *proxyEvents) closeWatchers() {
	p.Lock()
	defer p.Unlock()
	for i := range p.watchers {
		p.watchers[i].Close()
	}
	p.watchers = nil
	return
}

func (p *proxyEvents) NewWatcher(ctx context.Context, watch services.Watch) (services.Watcher, error) {
	w, err := p.events.NewWatcher(ctx, watch)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	p.Lock()
	defer p.Unlock()
	p.watchers = append(p.watchers, w)
	return w, nil
}
