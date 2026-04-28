/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/kvrocks-controller/store"
	"github.com/apache/kvrocks-controller/store/engine"
)

func makeVoteServer(vote bool) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(VoteResponse{Vote: vote})
	}))
}

func makeSlowServer(delay time.Duration) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(delay)
		_ = json.NewEncoder(w).Encode(VoteResponse{Vote: true})
	}))
}

var testVoteReq = VoteRequest{
	Namespace:    "ns",
	ClusterName:  "cluster",
	ShardIndex:   0,
	FailedNodeID: "dead-node",
}

func TestRequestVotes_NoPeers_ReturnsTrue(t *testing.T) {
	m := engine.NewMock()
	m.SetID("solo")
	s := store.NewClusterStore(m)
	coord := NewVoteCoordinator(s, 500*time.Millisecond)

	approved, err := coord.RequestVotes(context.Background(), testVoteReq)
	require.NoError(t, err)
	assert.True(t, approved)
}

func TestCallVote_YesResponse(t *testing.T) {
	peer := makeVoteServer(true)
	defer peer.Close()

	coord := &VoteCoordinator{
		voteTimeout: 500 * time.Millisecond,
		httpClient:  &http.Client{Timeout: 500 * time.Millisecond},
	}
	addr := peer.URL[len("http://"):]
	resp, err := coord.callVote(context.Background(), addr, testVoteReq)
	require.NoError(t, err)
	assert.True(t, resp.Vote)
}

func TestCallVote_NoResponse(t *testing.T) {
	peer := makeVoteServer(false)
	defer peer.Close()

	coord := &VoteCoordinator{
		voteTimeout: 500 * time.Millisecond,
		httpClient:  &http.Client{Timeout: 500 * time.Millisecond},
	}
	addr := peer.URL[len("http://"):]
	resp, err := coord.callVote(context.Background(), addr, testVoteReq)
	require.NoError(t, err)
	assert.False(t, resp.Vote)
}

func TestRequestVotes_OneNo_ReturnsFalse(t *testing.T) {
	yesServer := makeVoteServer(true)
	defer yesServer.Close()
	noServer := makeVoteServer(false)
	defer noServer.Close()

	ctx := context.Background()
	m := engine.NewMock()
	m.SetID("leader")
	// Register two peers with the current timestamp so ListActivePeers treats them as live.
	now := time.Now().Unix()
	_ = m.Set(ctx, "/kvrocks/peers/peer-yes", []byte(fmt.Sprintf("%s|%d", yesServer.URL[len("http://"):], now)))
	_ = m.Set(ctx, "/kvrocks/peers/peer-no", []byte(fmt.Sprintf("%s|%d", noServer.URL[len("http://"):], now)))

	s := store.NewClusterStore(m)
	coord := NewVoteCoordinator(s, 500*time.Millisecond)

	approved, err := coord.RequestVotes(ctx, testVoteReq)
	require.NoError(t, err)
	assert.False(t, approved)
}

func TestRequestVotes_AllYes_ReturnsTrue(t *testing.T) {
	yes1 := makeVoteServer(true)
	defer yes1.Close()
	yes2 := makeVoteServer(true)
	defer yes2.Close()

	ctx := context.Background()
	m := engine.NewMock()
	m.SetID("leader")
	now := time.Now().Unix()
	_ = m.Set(ctx, "/kvrocks/peers/peer-1", []byte(fmt.Sprintf("%s|%d", yes1.URL[len("http://"):], now)))
	_ = m.Set(ctx, "/kvrocks/peers/peer-2", []byte(fmt.Sprintf("%s|%d", yes2.URL[len("http://"):], now)))

	s := store.NewClusterStore(m)
	coord := NewVoteCoordinator(s, 500*time.Millisecond)

	approved, err := coord.RequestVotes(ctx, testVoteReq)
	require.NoError(t, err)
	assert.True(t, approved)
}

func TestCallVote_Timeout(t *testing.T) {
	slow := makeSlowServer(1 * time.Second)
	defer slow.Close()

	coord := &VoteCoordinator{
		voteTimeout: 100 * time.Millisecond,
		httpClient:  &http.Client{Timeout: 200 * time.Millisecond},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := coord.callVote(ctx, slow.URL[len("http://"):], testVoteReq)
	assert.Error(t, err)
}
