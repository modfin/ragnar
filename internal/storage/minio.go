package storage

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io"
	"log/slog"
	"strings"
	"unicode"
)

const fileHashMetadataKey = "File-Hash"

type Config struct {
	Endpoint  string `cli:"s3-endpoint"`
	Bucket    string `cli:"s3-bucket"`
	AccessKey string `cli:"s3-access-key"`
	SecretKey string `cli:"s3-secret-key"`

	Production bool `cli:"production"`
}

type Storage struct {
	cfg Config
	log *slog.Logger

	client *minio.Client
}

func (s *Storage) Name() string {
	return "storage"
}

func New(log *slog.Logger, config Config) (*Storage, error) {

	minioClient, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKey, config.SecretKey, ""),
		Secure: config.Production, // Use SSL
	})

	if err != nil {
		return nil, fmt.Errorf("error initializing minio client: %w", err)
	}

	return &Storage{log: log, cfg: config, client: minioClient}, nil
}

func (s *Storage) Close(ctx context.Context) error {
	return nil
}

func (s *Storage) PutDocument(ctx context.Context, tub string, documentId string, file io.Reader, objectSize int64, headers pgtype.Hstore, fileHash string) (bool, error) {

	bucket := s.cfg.Bucket
	path := fmt.Sprintf("%s/%s", tub, documentId)

	s.log.Info("storing document", "bucket", bucket, "path", path, "size", objectSize)

	currentFileHash, err := s.getObjectHash(ctx, path)
	if err != nil {
		return false, fmt.Errorf("error checking existing document hash: %w", err)
	}
	if currentFileHash != nil && *currentFileHash == fileHash {
		s.log.Info("document already exists with same hash, skipping upload", "bucket", bucket, "path", path)
		return false, nil
	}

	putObject := minio.PutObjectOptions{}
	var meta = map[string]string{}
	for k, v := range headers {
		if v == nil {
			continue
		}
		if k == "content-type" {
			putObject.ContentType = *v
			continue
		}
		if k == "content-disposition" {
			putObject.ContentDisposition = *v
			continue
		}
		meta[k] = sanitizeHeaderValue(*v)
	}
	meta[fileHashMetadataKey] = fileHash
	putObject.UserMetadata = meta

	_, err = s.client.PutObject(ctx, bucket, path, file, objectSize, putObject)
	if err != nil {
		return false, fmt.Errorf("error storing document, %s/%s: %w", tub, documentId, err)
	}

	s.log.Info("successfully uploaded", "bucket", bucket, "path", path, "size", objectSize)
	return true, nil
}

func (s *Storage) PutDocumentMarkdown(ctx context.Context, tub string, documentId string, file io.Reader, objectSize int64, headers pgtype.Hstore, fileHash string) (bool, error) {

	bucket := s.cfg.Bucket
	path := fmt.Sprintf("%s/%s.md", tub, documentId)

	s.log.Info("storing document", "bucket", bucket, "path", path, "size", objectSize)
	currentFileHash, err := s.getObjectHash(ctx, path)
	if err != nil {
		return false, fmt.Errorf("error checking existing document markdown hash: %w", err)
	}
	if currentFileHash != nil && *currentFileHash == fileHash {
		s.log.Info("markdown document already exists with same hash, skipping upload", "bucket", bucket, "path", path)
		return false, nil
	}

	putObject := minio.PutObjectOptions{
		ContentType: "text/markdown",
	}
	var meta = map[string]string{}
	for k, v := range headers {
		if v == nil {
			continue
		}
		if k == "content-type" {
			continue
		}
		if k == "content-disposition" {
			continue
		}
		meta[k] = sanitizeHeaderValue(*v)
	}
	meta[fileHashMetadataKey] = fileHash
	putObject.UserMetadata = meta

	_, err = s.client.PutObject(ctx, bucket, path, file, objectSize, putObject)
	if err != nil {
		return false, fmt.Errorf("error storing document markdown, %s/%s: %w", tub, documentId, err)
	}

	s.log.Info("successfully uploaded", "bucket", bucket, "path", path, "size", objectSize)
	return true, nil
}

func (s *Storage) getObjectHash(ctx context.Context, path string) (*string, error) {
	s.log.Debug("Fetching object metadata from storage for hash retrieval")

	bucket := s.cfg.Bucket
	objInfo, err := s.client.StatObject(ctx, bucket, path, minio.StatObjectOptions{})
	if err != nil && minio.ToErrorResponse(err).Code == minio.NoSuchKey {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to fetch object metadata: %w", err)
	}

	if hash, ok := objInfo.UserMetadata[fileHashMetadataKey]; ok {
		return &hash, nil
	}
	return nil, nil
}

func (s *Storage) GetDocument(ctx context.Context, tub string, documentId string) (io.ReadCloser, error) {
	s.log.Debug("Fetching original document from storage")

	bucket := s.cfg.Bucket
	path := fmt.Sprintf("%s/%s", tub, documentId)
	obj, err := s.client.GetObject(ctx, bucket, path, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch document: %w", err)
	}

	return obj, nil
}

func (s *Storage) GetDocumentMarkdown(ctx context.Context, tub string, documentId string) (io.ReadCloser, error) {
	s.log.Debug("Fetching markdown version of document from storage")

	bucket := s.cfg.Bucket
	path := fmt.Sprintf("%s/%s.md", tub, documentId)
	obj, err := s.client.GetObject(ctx, bucket, path, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch document: %w", err)
	}

	return obj, nil
}

func (s *Storage) DeleteTub(ctx context.Context, tub string) error {
	s.log.Debug("starting deletion of all objects in tub", "tub", tub)

	objectsCh := s.client.ListObjects(ctx, s.cfg.Bucket, minio.ListObjectsOptions{
		Prefix: tub + "/", // to ensure only objects in the tub are deleted
		//Recursive: true,
	})

	// Check for any errors during deletion
	var deleteErrors []error
	for obj := range objectsCh {
		if obj.Err != nil {
			return fmt.Errorf("error listing objects for deletion in tub %s: %w", tub, obj.Err)
		}
		s.log.Debug("object to be deleted", "tub", tub, "object", obj.Key)
		err := s.client.RemoveObject(ctx, s.cfg.Bucket, obj.Key, minio.RemoveObjectOptions{
			GovernanceBypass: true,
		})
		if err != nil {
			deleteErrors = append(deleteErrors, fmt.Errorf("failed to delete object: %w, tub: %s, object: %s", err, tub, obj.Key))
		}
	}

	if len(deleteErrors) > 0 {
		errors := ""
		for _, err := range deleteErrors {
			errors += err.Error() + "; "
		}
		return fmt.Errorf("encountered %d error(s) while deleting objects from bucket %s: %s", len(deleteErrors), tub, errors)
	}

	s.log.Debug("successfully deleted all objects from bucket", "bucket", tub)
	return nil
}

func (s *Storage) DeleteDocument(ctx context.Context, tub string, documentId string) error {
	s.log.Debug("starting deletion of all objects in bucket", "tub", tub)

	path := fmt.Sprintf("%s/%s", tub, documentId)

	err := s.client.RemoveObject(ctx, s.cfg.Bucket, path, minio.RemoveObjectOptions{
		GovernanceBypass: true, // Important if you have object locking enabled
	})
	if err != nil {
		return fmt.Errorf("failed to delete object: %w", err)
	}
	return nil
}

func sanitizeHeaderValue(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsGraphic(r) {
			return r
		}
		if r == ' ' {
			return r
		}
		if r == '\u00A0' {
			return ' '
		}
		return -1
	}, s)
}
