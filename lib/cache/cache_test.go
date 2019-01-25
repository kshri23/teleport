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
	log "github.com/sirupsen/logrus"
	"gopkg.in/check.v1"
)

type CacheSuite struct {
	clock clockwork.Clock
}

var _ = check.Suite(&CacheSuite{})

// bootstrap check
func TestState(t *testing.T) { check.TestingT(t) }

func (s *CacheSuite) SetUpSuite(c *check.C) {
	utils.InitLoggerForTests(testing.Verbose())
	s.clock = clockwork.NewRealClock()
}

// testPack contains pack of
// services used for test run
type testPack struct {
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

func (t *testPack) Close() {
	var errors []error
	if t.backend != nil {
		errors = append(errors, t.backend.Close())
	}
	if t.cache != nil {
		errors = append(errors, t.cache.Close())
	}
	if err := trace.NewAggregate(errors...); err != nil {
		log.Warningf("Failed to close %v", err)
	}
}

// newPack returns a new test pack or fails the test on error
func (s *CacheSuite) newPackForAuth(c *check.C) *testPack {
	return s.newPack(c, ForAuth)
}

// newPack returns a new test pack or fails the test on error
func (s *CacheSuite) newPack(c *check.C, setupConfig func(c Config) Config) *testPack {
	p := &testPack{
		dataDir: c.MkDir(),
		clock:   s.clock,
	}
	var err error
	p.backend, err = lite.NewWithConfig(context.TODO(), lite.Config{
		Path:             p.dataDir,
		PollStreamPeriod: 200 * time.Millisecond,
	})
	c.Assert(err, check.IsNil)

	cacheDir := c.MkDir()
	p.cacheBackend, err = lite.NewWithConfig(context.TODO(), lite.Config{Path: cacheDir, EventsOff: true})
	c.Assert(err, check.IsNil)

	p.eventsC = make(chan CacheEvent, 100)
	ctx := context.TODO()

	p.trustS = local.NewCAService(p.backend)
	p.clusterConfigS = local.NewClusterConfigurationService(p.backend)
	p.provisionerS = local.NewProvisioningService(p.backend)
	p.eventsS = &proxyEvents{events: local.NewEventsService(p.backend)}
	p.cache, err = New(setupConfig(Config{
		Context:       ctx,
		Backend:       p.cacheBackend,
		Events:        p.eventsS,
		ClusterConfig: p.clusterConfigS,
		Provisioner:   p.provisionerS,
		Trust:         p.trustS,
		RetryPeriod:   200 * time.Millisecond,
		EventsC:       p.eventsC,
	}))
	c.Assert(err, check.IsNil)
	c.Assert(p.cache, check.NotNil)

	select {
	case <-p.eventsC:
	case <-time.After(time.Second):
		c.Fatalf("wait for the watcher to start")
	}
	return p
}

// TestCA tests certificate authorities
func (s *CacheSuite) TestCA(c *check.C) {
	p := s.newPackForAuth(c)
	defer p.Close()

	ca := suite.NewTestCA(services.UserCA, "example.com")
	c.Assert(p.trustS.UpsertCertAuthority(ca), check.IsNil)

	select {
	case <-p.eventsC:
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}

	out, err := p.cache.GetCertAuthority(ca.GetID(), true)
	c.Assert(err, check.IsNil)
	ca.SetResourceID(out.GetResourceID())
	fixtures.DeepCompare(c, ca, out)

	err = p.trustS.DeleteCertAuthority(ca.GetID())
	c.Assert(err, check.IsNil)

	select {
	case <-p.eventsC:
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}

	_, err = p.cache.GetCertAuthority(ca.GetID(), false)
	fixtures.ExpectNotFound(c, err)
}

// TestRecovery tests error recovery scenario
func (s *CacheSuite) TestRecovery(c *check.C) {
	p := s.newPackForAuth(c)
	defer p.Close()

	ca := suite.NewTestCA(services.UserCA, "example.com")
	c.Assert(p.trustS.UpsertCertAuthority(ca), check.IsNil)

	select {
	case <-p.eventsC:
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}

	// event has arrived, now close the watchers
	watchers := p.eventsS.getWatchers()
	c.Assert(watchers, check.HasLen, 1)
	p.eventsS.closeWatchers()

	// wait for watcher to restart
	select {
	case event := <-p.eventsC:
		c.Assert(event.Type, check.Equals, WatcherStarted)
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}

	// add modification and expect the resource to recover
	ca2 := suite.NewTestCA(services.UserCA, "example2.com")
	c.Assert(p.trustS.UpsertCertAuthority(ca2), check.IsNil)

	// wait for watcher to receive an event
	select {
	case event := <-p.eventsC:
		c.Assert(event.Type, check.Equals, EventProcessed)
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}

	out, err := p.cache.GetCertAuthority(ca2.GetID(), false)
	c.Assert(err, check.IsNil)
	ca2.SetResourceID(out.GetResourceID())
	services.RemoveCASecrets(ca2)
	fixtures.DeepCompare(c, ca2, out)
}

// TestTokens tests static and dynamic tokens
func (s *CacheSuite) TestTokens(c *check.C) {
	p := s.newPackForAuth(c)
	defer p.Close()

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

	err = p.clusterConfigS.SetStaticTokens(staticTokens)
	c.Assert(err, check.IsNil)

	select {
	case event := <-p.eventsC:
		c.Assert(event.Type, check.Equals, EventProcessed)
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}

	out, err := p.cache.GetStaticTokens()
	c.Assert(err, check.IsNil)
	staticTokens.SetResourceID(out.GetResourceID())
	fixtures.DeepCompare(c, staticTokens, out)

	expires := time.Now().Add(10 * time.Hour).Truncate(time.Second).UTC()
	token, err := services.NewProvisionToken("token", teleport.Roles{teleport.RoleAuth, teleport.RoleNode}, expires)
	c.Assert(err, check.IsNil)

	err = p.provisionerS.UpsertToken(token)
	c.Assert(err, check.IsNil)

	select {
	case event := <-p.eventsC:
		c.Assert(event.Type, check.Equals, EventProcessed)
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}

	tout, err := p.cache.GetToken(token.GetName())
	c.Assert(err, check.IsNil)
	token.SetResourceID(tout.GetResourceID())
	fixtures.DeepCompare(c, token, tout)

	err = p.provisionerS.DeleteToken(token.GetName())
	c.Assert(err, check.IsNil)

	select {
	case event := <-p.eventsC:
		c.Assert(event.Type, check.Equals, EventProcessed)
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}

	_, err = p.cache.GetToken(token.GetName())
	fixtures.ExpectNotFound(c, err)
}

// TestClusterConfig tests cluster configuration
func (s *CacheSuite) TestClusterConfig(c *check.C) {
	p := s.newPackForAuth(c)
	defer p.Close()

	// update cluster config to record at the proxy
	clusterConfig, err := services.NewClusterConfig(services.ClusterConfigSpecV3{
		SessionRecording: services.RecordAtProxy,
		Audit: services.AuditConfig{
			AuditEventsURI: []string{"dynamodb://audit_table_name", "file:///home/log"},
		},
	})
	c.Assert(err, check.IsNil)
	err = p.clusterConfigS.SetClusterConfig(clusterConfig)
	c.Assert(err, check.IsNil)

	clusterConfig, err = p.clusterConfigS.GetClusterConfig()
	c.Assert(err, check.IsNil)

	select {
	case event := <-p.eventsC:
		c.Assert(event.Type, check.Equals, EventProcessed)
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}

	out, err := p.cache.GetClusterConfig()
	c.Assert(err, check.IsNil)
	clusterConfig.SetResourceID(out.GetResourceID())
	fixtures.DeepCompare(c, clusterConfig, out)

	// update cluster name resource metadata
	clusterName, err := services.NewClusterName(services.ClusterNameSpecV2{
		ClusterName: "example.com",
	})
	c.Assert(err, check.IsNil)
	err = p.clusterConfigS.SetClusterName(clusterName)
	c.Assert(err, check.IsNil)

	clusterName, err = p.clusterConfigS.GetClusterName()
	c.Assert(err, check.IsNil)

	select {
	case event := <-p.eventsC:
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
	p := s.newPackForAuth(c)
	defer p.Close()

	// update cluster config to record at the proxy
	clusterConfig, err := services.NewClusterConfig(services.ClusterConfigSpecV3{
		SessionRecording: services.RecordAtProxy,
		Audit: services.AuditConfig{
			AuditEventsURI: []string{"dynamodb://audit_table_name", "file:///home/log"},
		},
	})
	c.Assert(err, check.IsNil)
	err = p.clusterConfigS.SetClusterConfig(clusterConfig)
	c.Assert(err, check.IsNil)

	clusterConfig, err = p.clusterConfigS.GetClusterConfig()
	c.Assert(err, check.IsNil)

	select {
	case event := <-p.eventsC:
		c.Assert(event.Type, check.Equals, EventProcessed)
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}

	out, err := p.cache.GetClusterConfig()
	c.Assert(err, check.IsNil)
	clusterConfig.SetResourceID(out.GetResourceID())
	fixtures.DeepCompare(c, clusterConfig, out)

	// update cluster name resource metadata
	clusterName, err := services.NewClusterName(services.ClusterNameSpecV2{
		ClusterName: "example.com",
	})
	c.Assert(err, check.IsNil)
	err = p.clusterConfigS.SetClusterName(clusterName)
	c.Assert(err, check.IsNil)

	clusterName, err = p.clusterConfigS.GetClusterName()
	c.Assert(err, check.IsNil)

	select {
	case event := <-p.eventsC:
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
