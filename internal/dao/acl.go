package dao

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/modfin/ragnar"
	"github.com/modfin/ragnar/internal/auth"
)

// DBGet interface is a subset of sqlx.DB and sqlx.Tx
type DBGet interface {
	Get(dest interface{}, query string, args ...interface{}) error
}

func (d *DAO) AllowedOperation(ctx context.Context, operation ...auth.ACLOperation) error {
	return allowedGeneralOperation(d.db, ctx, operation...)
}

func (d *DAO) AllowedTubOperation(ctx context.Context, tubname string, operation ...auth.ACLOperation) error {
	return allowedTubOperation(d.db, ctx, tubname, operation...)
}

func allowedTubOperation(getter DBGet, ctx context.Context, tubname string, operation ...auth.ACLOperation) error {
	accessKey, ok := ctx.Value(auth.ACCESS_KEY).(string)
	if !ok {
		return errors.New("access key not found in context")
	}

	tubname = strings.ToLower(tubname)
	if !bucketNameRegExp.MatchString(tubname) {
		return errors.New("tub name must only contain a-z0-9_-, and be at least 3 character long")
	}

	if len(operation) == 0 {
		return errors.New("operation is required")
	}

	q := `SELECT count(*) > 0 
			  FROM "public"."tub_acl" a
			  INNER JOIN "public"."access_token" t USING(access_key_id)
			  WHERE t.access_key = $1 
			    AND a.tub_name = $2
				AND now() <= coalesce(t.deleted_at, now())
				AND now() <= coalesce(a.deleted_at, now())
			    `

	additional := ""
	for _, op := range operation {
		switch op {
		case auth.ALLOW_CREATE:
			additional += "AND a.allow_create \n"
		case auth.ALLOW_READ:
			additional += "AND a.allow_read \n"
		case auth.ALLOW_UPDATE:
			additional += "AND a.allow_update \n"
		case auth.ALLOW_DELETE:
			additional += "AND a.allow_delete \n"
		}
	}
	q = q + additional

	var allowed bool
	err := getter.Get(&allowed, q, accessKey, tubname)
	if err != nil {
		return fmt.Errorf("error checking permission to update tub: %w", err)
	}
	if !allowed {
		return errors.New("access token does not have permission have requested permissions")
	}

	return nil

}
func allowedGeneralOperation(getter DBGet, ctx context.Context, operation ...auth.ACLOperation) error {
	accessKey, ok := ctx.Value(auth.ACCESS_KEY).(string)
	if !ok {
		return errors.New("access key not found in context")
	}

	if len(operation) == 0 {
		return errors.New("operation is required")
	}

	q := `SELECT count(*) > 0 
			  FROM "public"."access_token" t
			  WHERE t.access_key = $1 
				AND now() <= coalesce(t.deleted_at, now())
			    `

	additional := ""
	for _, op := range operation {
		switch op {
		case auth.ALLOW_CREATE:
			additional += "AND t.allow_create_tubs \n"
		case auth.ALLOW_READ:
			additional += "AND t.allow_read_tubs \n"
		default:
			return errors.New("only ALLOW_CREATE and ALLOW_READ are supported for general operations")
		}
	}
	q = q + additional

	var allowed bool
	err := getter.Get(&allowed, q, accessKey)
	if err != nil {
		return fmt.Errorf("error checking permission to update tub: %w", err)
	}
	if !allowed {
		return errors.New("access token does not have permission have requested permissions")
	}

	return nil

}

func (d *DAO) GetAccessTokenFromKeyID(ctx context.Context, keyId string) (*ragnar.AccessToken, error) {
	q := `SELECT * FROM "public"."access_token" WHERE access_key_id = $1 AND now() <= coalesce(deleted_at, now())`

	var tok ragnar.AccessToken
	err := d.db.GetContext(ctx, &tok, q, keyId)

	return &tok, err
}

func (d *DAO) GetAccessTokenFromKey(ctx context.Context, key string) (*ragnar.AccessToken, error) {
	q := `SELECT * FROM "public"."access_token" WHERE access_token.access_key = $1 AND now() <= coalesce(deleted_at, now())`

	var tok ragnar.AccessToken
	err := d.db.GetContext(ctx, &tok, q, key)

	return &tok, err
}
