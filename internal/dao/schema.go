package dao

import (
	"embed"
	_ "embed"
	"fmt"
	"io/fs"
)

//go:embed migrations/*.sql
var migrations embed.FS

func (d *DAO) ensure(dev bool) error {
	if dev {
		d.log.Info("Applying extensions...")
		_, err := d.db.Exec(`
-- 					 CREATE EXTENSION IF NOT EXISTS pg_tokenizer CASCADE; 
-- 					 CREATE EXTENSION IF NOT EXISTS vchord_bm25 CASCADE;
					 CREATE EXTENSION IF NOT EXISTS vector CASCADE;
					 CREATE EXTENSION IF NOT EXISTS hstore CASCADE;
					 CREATE EXTENSION IF NOT EXISTS pgcrypto CASCADE;
`)
		if err != nil {
			return fmt.Errorf("failed to apply extensions: %w", err)
		}
	}
	d.log.Info("Applying migrations...")
	// Walks dir in lexographical order
	return fs.WalkDir(migrations, "migrations", func(path string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if e.IsDir() {
			return nil
		}
		mig, err := migrations.ReadFile(path)
		if err != nil {
			return fmt.Errorf("failed to read file %s: %w", path, err)
		}
		d.log.Info("Applying migration", "path", path)

		_, err = d.db.Exec(string(mig))
		if err != nil {
			return fmt.Errorf("failed to apply migration %s: %w", path, err)
		}
		return nil

	})

}
