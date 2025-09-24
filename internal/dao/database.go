package dao

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
)

type Config struct {
	URI string `cli:"db-uri"`
}

type DAO struct {
	db  *sqlx.DB
	log *slog.Logger
	uri string
}

func (d *DAO) Name() string {
	return "dao"
}

func (d *DAO) Close(ctx context.Context) error {

	var err error
	var closed = make(chan struct{})
	go func() {
		err = d.db.Close()
		close(closed)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-closed:
		return err
	case <-time.After(30 * time.Second):
		return fmt.Errorf("timed out waiting for database connection to close")
	}
}

func (d *DAO) txx(ctx context.Context, f func(tx *sqlx.Tx) error) error {
	tx, err := d.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err) // Return the original error err
	}

	err = f(tx)

	if err != nil {
		return errors.Join(err, tx.Rollback())
	}
	return tx.Commit()
}

func (d *DAO) Ping() error {
	return d.db.Ping()
}

func New(log *slog.Logger, config Config, dev bool) (*DAO, error) {
	uri := config.URI

	var db *sqlx.DB
	var err error

	// retry connection to give the database container time to start up
	for {
		log.Info("Trying to connect to db..")
		db, err = sqlx.Open("pgx", uri)
		if err != nil {
			log.Error("failed to open database connection,  retrying in 3 seconds", "err", err)
			time.Sleep(3 * time.Second)
			continue
		}
		err = db.Ping()
		if err != nil {
			log.Info("failed to ping database, retrying in 3 seconds...", "err", err)
			time.Sleep(3 * time.Second)
			_ = db.Close()
			continue

		}
		log.Info("Successfully connected to the PostgreSQL database!")
		break // Exit loop on successful connection

	}

	dao := &DAO{
		uri: uri,
		db:  db,
		log: log,
	}

	err = dao.ensure(dev)
	if err != nil {
		return nil, fmt.Errorf("failed to ensure database schema: %w", err)
	}

	return dao, nil
}
