// Copyright 2024 SurrealDB, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package surrealdb

import (
	"context"
	"fmt"
	"strings"

	"github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/pingcap/log"
)

const (
	surrealdbUri  = "surrealdb.uri"
	surrealdbUser = "surrealdb.user"
	surrealdbPass = "surrealdb.pass"
	surrealdbNs   = "surrealdb.ns"
	surrealdbDb   = "surrealdb.db"
)

type surrealdbCreator struct{}

func (c surrealdbCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	s := &surrealDB{
		dbPath: p.GetString(surrealdbUri, "ws://127.0.0.1:8000"),
		dbUser: p.GetString(surrealdbUser, "root"),
		dbPass: p.GetString(surrealdbPass, "root"),
		useNs:  p.GetString(surrealdbNs, "ycsb"),
		useDb:  p.GetString(surrealdbDb, "ycsb"),
		table:  p.GetString(prop.TableName, prop.TableNameDefault),
	}

	if p.GetBool(prop.DropData, prop.DropDataDefault) {
		db, err := s.connect()
		if db != nil {
			defer db.Close()
		}
		if err != nil {
			return nil, fmt.Errorf("unable to connect to SurrealDB: %w", err)
		}
		if _, err = surrealdb.Query[any](db, fmt.Sprintf(`REMOVE TABLE IF EXISTS %s`, s.table), nil); err != nil {
			return nil, fmt.Errorf("unable to remove table %s: %w", s.table, err)
		}
	}

	return s, nil
}

func (s *surrealDB) connect() (*surrealdb.DB, error) {
	// Create a new connection
	db, err := surrealdb.New(s.dbPath)
	if err != nil {
		return nil, err
	}
	// Authenticate to the inistance
	if s.dbUser != "" {
		if _, err = db.SignIn(&surrealdb.Auth{
			Username: s.dbUser,
			Password: s.dbPass,
		}); err != nil {
			return nil, err
		}
	}
	// Select a namespace and database
	if err = db.Use(s.useNs, s.useDb); err != nil {
		return nil, err
	}
	// Ensure the namespace, database, and table exist
	if _, err = surrealdb.Query[any](db, fmt.Sprintf(`DEFINE TABLE IF NOT EXISTS %s`, s.table), nil); err != nil {
		return nil, err
	}
	// Return the database connection
	return db, nil
}

type surrealDBConnection struct{}

type surrealDB struct {
	dbPath string
	dbUser string
	dbPass string
	useNs  string
	useDb  string
	table  string
}

func (s *surrealDB) Close() error {
	return nil
}

func (s *surrealDB) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	// Attempt to connect to SurrealDB
	db, err := s.connect()
	if err != nil {
		log.S().Panicf("Error connecting to SurrealDB endpoint: %w", err)
	}
	// Return the context
	return context.WithValue(ctx, surrealDBConnection{}, db)
}

func (s *surrealDB) CleanupThread(ctx context.Context) {
	ctx.Value(surrealDBConnection{}).(*surrealdb.DB).Close()
}

func (s *surrealDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	db := ctx.Value(surrealDBConnection{}).(*surrealdb.DB)
	if len(fields) == 0 {
		res, err := surrealdb.Select[map[string][]byte](db, models.RecordID{
			Table: table,
			ID:    key,
		})
		if err != nil {
			log.S().Errorf("Read error: %s", err.Error())
			return nil, fmt.Errorf("Read error: %s", err.Error())
		}
		return *res, nil
	} else {
		query := fmt.Sprintf(`SELECT %s FROM %s:%s`, strings.Join(fields, ","), table, key)
		res, err := surrealdb.Query[[]map[string][]byte](db, query, nil)
		if err != nil {
			log.S().Errorf("Read error: %s", err.Error())
			return nil, fmt.Errorf("Read error: %s", err.Error())
		}
		return (*res)[0].Result[0], nil
	}
}

func (s *surrealDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	db := ctx.Value(surrealDBConnection{}).(*surrealdb.DB)
	var query string
	if len(fields) == 0 {
		query = fmt.Sprintf(`SELECT * FROM %s:%s.. LIMIT %d`, table, startKey, count)
	} else {
		query = fmt.Sprintf(`SELECT %s FROM %s:%s.. LIMIT %d`, strings.Join(fields, ","), table, startKey, count)
	}
	res, err := surrealdb.Query[[]map[string][]byte](db, query, nil)
	if err != nil {
		log.S().Errorf("Read error: %s", err.Error())
		return nil, fmt.Errorf("Read error: %s", err.Error())
	}
	return (*res)[0].Result, nil
}

func (s *surrealDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	db := ctx.Value(surrealDBConnection{}).(*surrealdb.DB)
	_, err := surrealdb.Upsert[any](db, models.RecordID{
		Table: table,
		ID:    key,
	}, values)
	if err != nil {
		log.S().Errorf("Insert error: %s", err.Error())
		return fmt.Errorf("Insert error: %s", err.Error())
	}
	return nil
}

func (s *surrealDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	db := ctx.Value(surrealDBConnection{}).(*surrealdb.DB)
	_, err := surrealdb.Update[any](db, models.RecordID{
		Table: table,
		ID:    key,
	}, values)
	if err != nil {
		log.S().Errorf("Update error: %s", err.Error())
		return fmt.Errorf("Update error: %s", err.Error())
	}
	return nil
}

func (s *surrealDB) Delete(ctx context.Context, table string, key string) error {
	db := ctx.Value(surrealDBConnection{}).(*surrealdb.DB)
	_, err := surrealdb.Delete[any](db, models.RecordID{
		Table: table,
		ID:    key,
	})
	if err != nil {
		log.S().Errorf("Delete error: %s", err.Error())
		return fmt.Errorf("Delete error: %s", err.Error())
	}
	return nil
}

func init() {
	ycsb.RegisterDBCreator("surrealdb", surrealdbCreator{})
}
