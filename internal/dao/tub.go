package dao

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/modfin/ragnar"
	"github.com/modfin/ragnar/internal/auth"
)

var bucketNameRegExp = regexp.MustCompile(`^[a-z0-9_\-]{3,}$`)

func tubToSchema(name string) (string, error) {
	name = strings.ToLower(name)
	if !bucketNameRegExp.MatchString(name) {
		return "", errors.New("tub name must only contain a-z0-9_-, and be at least 3 character long")
	}
	return fmt.Sprintf("_tub[%s]", name), nil
}

func (d *DAO) UpdateTub(ctx context.Context, tub ragnar.Tub) error {
	tubname := strings.ToLower(tub.TubName)
	if !bucketNameRegExp.MatchString(tubname) {
		return errors.New("tub name must only contain a-z0-9_-, and be at least 3 character long")
	}
	requiredHeaders := tub.GetRequiredDocumentHeaders()
	if len(requiredHeaders) > 0 {
		ok, err := d.AllDocumentsHasHeaders(ctx, tubname, requiredHeaders)
		if err != nil {
			return fmt.Errorf("error checking required headers in existing documents: %w", err)
		}
		if !ok {
			return fmt.Errorf("cannot update tub, some documents are missing required headers: %v", requiredHeaders)
		}
	}

	return d.txx(ctx, func(tx *sqlx.Tx) error {
		err := allowedTubOperation(tx, ctx, tubname, auth.ALLOW_UPDATE)
		if err != nil {
			return fmt.Errorf("error checking permission to update tub: %w", err)
		}

		q := `UPDATE "public"."tub" 
			 SET settings = CAST(''||$1||'' AS HSTORE),
			     updated_at = now()
			 WHERE tub_name = $2
		`

		res, err := tx.Exec(q, tub.Settings, tubname)
		if err != nil {
			return fmt.Errorf("error updating tub settings: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("error getting rows affected: %w", err)
		}
		if n == 0 {
			return errors.New("tub not found")
		}

		return nil
	})
}

func (d *DAO) CreateTub(ctx context.Context, tub ragnar.Tub) (ragnar.Tub, error) {

	accessKey, ok := auth.GetAccessKey(ctx)
	if !ok {
		return ragnar.Tub{}, errors.New("create tub, access key not found")
	}

	tub.TubName = strings.ToLower(tub.TubName)

	if !bucketNameRegExp.MatchString(tub.TubName) {
		return ragnar.Tub{}, errors.New("tub name must only contain a-z0-9_-, and be at least 3 character long")
	}

	token, err := d.GetAccessTokenFromKey(ctx, accessKey)
	if err != nil {
		return ragnar.Tub{}, fmt.Errorf("error getting access token: %w", err)
	}

	err = d.txx(ctx, func(tx *sqlx.Tx) error {

		err := tx.Get(&tub, `INSERT INTO "public"."tub" (tub_name) VALUES ($1) RETURNING *`, tub.TubName)
		if err != nil {
			return fmt.Errorf("error creating tub: %w", err)
		}

		_, err = tx.Exec(`INSERT INTO "public"."tub_acl" 
    					(access_key_id, tub_id, tub_name, 
    					 allow_create, allow_read, 
    					 allow_update, allow_delete) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			token.AccessKeyId, tub.TubId, tub.TubName,
			true, true,
			true, true,
		)
		if err != nil {
			return fmt.Errorf("error creating tub acl: %w", err)
		}

		// Creating tub specific schema
		schemaname, err := tubToSchema(tub.TubName)
		if err != nil {
			return fmt.Errorf("error getting schema: %w", err)
		}

		_, err = tx.Exec(fmt.Sprintf(`CREATE SCHEMA "%s"`, schemaname))
		if err != nil {
			return fmt.Errorf("error creating schema: %w", err)
		}

		q := `CREATE TABLE "%s"."document" (
			document_id TEXT        NOT NULL DEFAULT 'doc_' || gen_random_uuid(),
			tub_id      TEXT        NOT NULL REFERENCES "public"."tub" (tub_id),
			tub_name    TEXT        NOT NULL REFERENCES "public"."tub" (tub_name),
			
			headers     HSTORE      NOT NULL DEFAULT '',
			
			created_at  TIMESTAMP WITH TIME ZONE not null default now(),
			updated_at TIMESTAMP WITH TIME ZONE not null default now(),
			PRIMARY KEY (document_id)
			)`
		_, err = tx.Exec(fmt.Sprintf(q, schemaname))
		if err != nil {
			return fmt.Errorf("error create document table: %w", err)
		}
		q = `CREATE TABLE "%s"."chunk" (
			  chunk_id    INT,
			  document_id TEXT        NOT NULL REFERENCES "%s"."document" (document_id),
			  tub_id      TEXT        NOT NULL REFERENCES "public"."tub" (tub_id),
			  tub_name    TEXT        NOT NULL REFERENCES "public"."tub" (tub_name),
			  
			  context     TEXT        NOT NULL DEFAULT '',
			  content     TEXT        NOT NULL,
		
			  created_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
			  updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
		  
			  primary key (document_id, chunk_id)
			)`
		_, err = tx.Exec(fmt.Sprintf(q, schemaname, schemaname))
		if err != nil {
			return fmt.Errorf("error create chunk table: %w", err)
		}

		return nil
	})

	return tub, err
}

func (d *DAO) DeleteTub(ctx context.Context, tubname string) error {
	tubname = strings.ToLower(tubname)
	if !bucketNameRegExp.MatchString(tubname) {
		return errors.New("tub name must only contain a-z0-9_-, and be at least 3 character long")
	}

	return d.txx(ctx, func(tx *sqlx.Tx) error {

		err := allowedTubOperation(tx, ctx, tubname, auth.ALLOW_DELETE)
		if err != nil {
			return fmt.Errorf("error checking permission to delete tub: %w", err)
		}

		// TODO: maybe just mark as deleted?

		q := `DROP SCHEMA IF EXISTS "%s" CASCADE`

		schema, err := tubToSchema(tubname)
		if err != nil {
			return fmt.Errorf("error getting schema: %w", err)
		}
		q = fmt.Sprintf(q, schema)

		_, err = tx.ExecContext(ctx, q)
		if err != nil {
			return fmt.Errorf("error dropping schema: %w", err)
		}

		_, err = tx.ExecContext(ctx, `DELETE FROM "public"."tub_acl" WHERE tub_name = $1`, tubname)
		if err != nil {
			return fmt.Errorf("error delete tub acl: %w", err)
		}
		_, err = tx.ExecContext(ctx, `DELETE FROM "public"."tub" WHERE tub_name = $1`, tubname)
		if err != nil {
			return fmt.Errorf("error delete tub: %w", err)
		}

		return nil
	})
}

func (d *DAO) ListTubs(ctx context.Context) ([]ragnar.Tub, error) {

	accessToken, ok := auth.GetAccessKey(ctx)
	if !ok {
		return nil, errors.New("list, access key not found")
	}

	var tubs []ragnar.Tub

	q := `
	SELECT t.tub_id, t.tub_name, t.settings, t.created_at, t.updated_at, t.deleted_at
	FROM public.access_token token
	INNER JOIN public.tub_acl acl USING(access_key_id) 
	INNER JOIN public.tub t USING(tub_id)
	WHERE token.access_key = $1
	  AND acl.allow_read
`
	err := d.db.SelectContext(ctx, &tubs, q, accessToken)
	if err != nil {
		return nil, fmt.Errorf("error listing tubs: %w", err)
	}

	return tubs, nil
}

func (d *DAO) GetTub(ctx context.Context, tubname string) (ragnar.Tub, error) {

	accessToken, ok := auth.GetAccessKey(ctx)
	if !ok {
		return ragnar.Tub{}, errors.New("get tub, access key not found")
	}

	q := `
	SELECT t.tub_id, t.tub_name, t.settings, t.created_at, t.updated_at, t.deleted_at
	FROM public.access_token token
	INNER JOIN public.tub_acl acl USING(access_key_id) 
	INNER JOIN public.tub t USING(tub_name)
	WHERE token.access_key = $1
	  AND t.tub_name = $2
	  AND acl.allow_read
`
	var tub ragnar.Tub
	err := d.db.GetContext(ctx, &tub, q, accessToken, tubname)
	if err != nil {
		return tub, fmt.Errorf("error listing tubs: %w", err)
	}

	return tub, nil
}
