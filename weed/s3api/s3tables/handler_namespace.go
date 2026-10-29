package s3tables

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// errOwnerlessBucket marks a table bucket that has the marker but no
// metadata: damaged, and answered with the operation's own denial rather than
// letting the caller name itself the bucket's owner before any check runs.
var errOwnerlessBucket = fmt.Errorf("table bucket has no owner metadata")

// mayRepairOwnerlessBucket reports whether the caller may repair a bucket in
// that state: the admin or a trusted internal handler. hasAdminAction also
// answers s3tables:*, an operation grant, not administrator status.
func (h *S3TablesHandler) mayRepairOwnerlessBucket(r *http.Request) bool {
	if h.trusted || h.getAccountID(r) == s3_constants.AccountAdminId {
		return true
	}
	for _, action := range getIdentityActions(r) {
		if action == s3_constants.ACTION_ADMIN {
			return true
		}
	}
	return false
}

// handleCreateNamespace creates a new namespace in a table bucket
func (h *S3TablesHandler) handleCreateNamespace(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	glog.V(3).Infof("S3Tables: handleCreateNamespace called")
	var req CreateNamespaceRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	if req.TableBucketARN == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "tableBucketARN is required")
		return fmt.Errorf("tableBucketARN is required")
	}

	if len(req.Namespace) == 0 {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "namespace is required")
		return fmt.Errorf("namespace is required")
	}

	bucketName, err := parseBucketNameFromARN(req.TableBucketARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	namespaceName, err := validateNamespace(req.Namespace)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	// Check if table bucket exists
	bucketPath := GetTableBucketPath(bucketName)
	var bucketMetadata tableBucketMetadata
	var bucketPolicy string
	var bucketTags map[string]string
	ownerAccountID := h.getAccountID(r)
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyMetadata)
		if err != nil {
			if errors.Is(err, ErrAttributeNotFound) {
				dir, name := splitPath(bucketPath)
				entryResp, lookupErr := filer_pb.LookupEntry(r.Context(), client, &filer_pb.LookupDirectoryEntryRequest{
					Directory: dir,
					Name:      name,
				})
				if lookupErr != nil {
					return lookupErr
				}
				if entryResp.Entry == nil || !IsTableBucketEntry(entryResp.Entry) {
					return filer_pb.ErrNotFound
				}
				if !h.mayRepairOwnerlessBucket(r) {
					return errOwnerlessBucket
				}
				// Repair the marker-only entry under the entry lock so a
				// creator finishing its own create in the meantime keeps its
				// owner: adopt whatever metadata is there, write only when it
				// is still absent.
				if err := h.mutateEntryExtended(r.Context(), client, bucketPath, func(extended map[string][]byte) error {
					if data, ok := extended[ExtendedKeyMetadata]; ok {
						return json.Unmarshal(data, &bucketMetadata)
					}
					bucketMetadata = tableBucketMetadata{
						Name:           bucketName,
						CreatedAt:      time.Now(),
						OwnerAccountID: ownerAccountID,
					}
					metadataBytes, err := json.Marshal(&bucketMetadata)
					if err != nil {
						return fmt.Errorf("failed to marshal bucket metadata: %w", err)
					}
					extended[ExtendedKeyMetadata] = metadataBytes
					return nil
				}); err != nil {
					return err
				}
			} else {
				return err
			}
		} else if err := json.Unmarshal(data, &bucketMetadata); err != nil {
			return fmt.Errorf("failed to unmarshal bucket metadata: %w", err)
		}

		// Fetch bucket policy if it exists
		policyData, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
		if err == nil {
			bucketPolicy = string(policyData)
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch bucket policy: %v", err)
		}
		bucketTags, err = h.readTags(r.Context(), client, bucketPath)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, errOwnerlessBucket) {
			h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to create namespace in this bucket")
			return ErrAccessDenied
		}
		if errors.Is(err, filer_pb.ErrNotFound) {
			if !h.authorizeAbsentTarget(r, filerClient, "CreateNamespace", bucketName, namespaceName, "", "") {
				h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to create namespace in this bucket")
				return ErrAccessDenied
			}
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchBucket, fmt.Sprintf("table bucket %s not found", bucketName))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to check table bucket: %v", err))
		}
		return err
	}

	bucketARN := h.generateTableBucketARN(bucketMetadata.OwnerAccountID, bucketName)
	principal := h.getAccountID(r)
	identityActions := getIdentityActions(r)
	glog.Infof("S3Tables: CreateNamespace permission check - principal=%s, owner=%s, actions=%v", principal, bucketMetadata.OwnerAccountID, identityActions)
	if !CheckPermissionWithContext("CreateNamespace", principal, bucketMetadata.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
		TrustedIssuers:   h.trustedIssuers(r),
		IsLocalAccountID: h.localAccountIDs,
		TableBucketName:  bucketName,
		Namespace:        namespaceName,
		TableBucketTags:  bucketTags,
		IdentityActions:  identityActions,
		DefaultAllow:     h.defaultAllowFor(r),
	}) {
		glog.Infof("S3Tables: Permission denied for CreateNamespace - principal=%s, owner=%s", principal, bucketMetadata.OwnerAccountID)
		h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to create namespace in this bucket")
		return ErrAccessDenied
	}

	namespacePath := GetNamespacePath(bucketName, namespaceName)

	// Check if namespace already exists
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		_, err := h.readCatalogRecord(r.Context(), client, namespacePath)
		return err
	})

	if err == nil {
		h.writeError(w, http.StatusConflict, ErrCodeNamespaceAlreadyExists, fmt.Sprintf("namespace %s already exists", namespaceName))
		return fmt.Errorf("namespace already exists")
	} else if !errors.Is(err, filer_pb.ErrNotFound) {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to check namespace: %v", err))
		return err
	}

	// Create the namespace with bucket owner to maintain consistency
	// (authorization above ensures the caller has permission to create in this bucket)
	now := time.Now()
	metadata := &namespaceMetadata{
		Namespace:      req.Namespace,
		CreatedAt:      now,
		OwnerAccountID: bucketMetadata.OwnerAccountID,
		Properties:     req.Properties,
	}

	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to marshal namespace metadata")
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	var taken error
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		attrs := map[string][]byte{ExtendedKeyMetadata: metadataBytes}
		err := h.createDirectoryExclusive(r.Context(), client, namespacePath, attrs)
		if errors.Is(err, filer_pb.ErrEntryAlreadyExists) {
			err = h.adoptNamespaceDirectory(r.Context(), client, namespacePath, attrs)
		}
		if errors.Is(err, ErrNameTaken) {
			taken = filer_pb.ErrEntryAlreadyExists // an answer, not a filer failure
			return nil
		}
		return err
	})
	if err == nil && taken != nil {
		err = taken
	}
	if errors.Is(err, filer_pb.ErrEntryAlreadyExists) {
		h.writeError(w, http.StatusConflict, ErrCodeNamespaceAlreadyExists, fmt.Sprintf("namespace %s already exists", namespaceName))
		return err
	}
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to create namespace")
		return err
	}

	resp := &CreateNamespaceResponse{
		Namespace:      req.Namespace,
		TableBucketARN: req.TableBucketARN,
		Properties:     req.Properties,
	}

	h.writeJSON(w, http.StatusOK, resp)
	return nil
}

// adoptNamespaceDirectory claims a directory already at a namespace's path.
// DeleteNamespace leaves the directory without a catalog record when tables
// dropped without purge still keep files in it; creating the namespace again
// takes the directory back, and the tables' files stay unregistered. The claim
// is compare-checked: a directory that still carries a catalog record, or a
// file at that path, is a name conflict reported as ErrNameTaken.
func (h *S3TablesHandler) adoptNamespaceDirectory(ctx context.Context, client filer_pb.SeaweedFilerClient, namespacePath string, attrs map[string][]byte) error {
	entry, err := h.lookupEntry(ctx, client, namespacePath)
	if err != nil {
		return err
	}
	if !entry.IsDirectory {
		return fmt.Errorf("%w: %s is not a directory", ErrNameTaken, namespacePath)
	}
	return h.claimCatalogEntry(ctx, client, namespacePath, attrs)
}

// handleGetNamespace gets details of a namespace
func (h *S3TablesHandler) handleGetNamespace(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	var req GetNamespaceRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	if req.TableBucketARN == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "tableBucketARN is required")
		return fmt.Errorf("tableBucketARN is required")
	}

	namespaceName, err := validateNamespace(req.Namespace)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	bucketName, err := parseBucketNameFromARN(req.TableBucketARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	namespacePath := GetNamespacePath(bucketName, namespaceName)
	bucketPath := GetTableBucketPath(bucketName)

	// Get namespace and bucket policy
	var metadata namespaceMetadata
	var bucketPolicy string
	var bucketTags map[string]string
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.readCatalogRecord(r.Context(), client, namespacePath)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &metadata); err != nil {
			return err
		}

		// Fetch bucket policy if it exists
		policyData, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
		if err == nil {
			bucketPolicy = string(policyData)
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch bucket policy: %v", err)
		}
		bucketTags, err = h.readTags(r.Context(), client, bucketPath)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchNamespace, fmt.Sprintf("namespace %s not found", flattenNamespace(req.Namespace)))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to get namespace: %v", err))
		}
		return err
	}

	bucketARN := h.generateTableBucketARN(metadata.OwnerAccountID, bucketName)
	principal := h.getAccountID(r)
	identityActions := getIdentityActions(r)
	if !CheckPermissionWithContext("GetNamespace", principal, metadata.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
		TrustedIssuers:   h.trustedIssuers(r),
		IsLocalAccountID: h.localAccountIDs,
		TableBucketName:  bucketName,
		Namespace:        namespaceName,
		TableBucketTags:  bucketTags,
		IdentityActions:  identityActions,
		DefaultAllow:     h.defaultAllowFor(r),
	}) {
		h.writeError(w, http.StatusNotFound, ErrCodeNoSuchNamespace, fmt.Sprintf("namespace %s not found", flattenNamespace(req.Namespace)))
		return ErrAccessDenied
	}

	resp := &GetNamespaceResponse{
		Namespace:      metadata.Namespace,
		CreatedAt:      metadata.CreatedAt,
		OwnerAccountID: metadata.OwnerAccountID,
		Properties:     metadata.Properties,
	}

	h.writeJSON(w, http.StatusOK, resp)
	return nil
}

// handleUpdateNamespace replaces the stored properties of an existing namespace.
// AWS S3 Tables namespaces have no properties; this backs the Iceberg REST
// catalog's namespace-property updates, which carry the merged map.
func (h *S3TablesHandler) handleUpdateNamespace(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	var req UpdateNamespaceRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	if req.TableBucketARN == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "tableBucketARN is required")
		return fmt.Errorf("tableBucketARN is required")
	}

	namespaceName, err := validateNamespace(req.Namespace)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	bucketName, err := parseBucketNameFromARN(req.TableBucketARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	namespacePath := GetNamespacePath(bucketName, namespaceName)
	bucketPath := GetTableBucketPath(bucketName)

	var metadata namespaceMetadata
	var bucketPolicy string
	var bucketTags map[string]string
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.readCatalogRecord(r.Context(), client, namespacePath)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &metadata); err != nil {
			return err
		}

		policyData, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
		if err == nil {
			bucketPolicy = string(policyData)
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch bucket policy: %v", err)
		}
		bucketTags, err = h.readTags(r.Context(), client, bucketPath)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchNamespace, fmt.Sprintf("namespace %s not found", flattenNamespace(req.Namespace)))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to get namespace: %v", err))
		}
		return err
	}

	bucketARN := h.generateTableBucketARN(metadata.OwnerAccountID, bucketName)
	principal := h.getAccountID(r)
	identityActions := getIdentityActions(r)
	if !CheckPermissionWithContext("UpdateNamespace", principal, metadata.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
		TrustedIssuers:   h.trustedIssuers(r),
		IsLocalAccountID: h.localAccountIDs,
		TableBucketName:  bucketName,
		Namespace:        namespaceName,
		TableBucketTags:  bucketTags,
		IdentityActions:  identityActions,
		DefaultAllow:     h.defaultAllowFor(r),
	}) {
		h.writeError(w, http.StatusNotFound, ErrCodeNoSuchNamespace, fmt.Sprintf("namespace %s not found", flattenNamespace(req.Namespace)))
		return ErrAccessDenied
	}

	metadata.Properties = req.Properties
	metadataBytes, err := json.Marshal(&metadata)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to marshal namespace metadata")
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return h.setExtendedAttribute(r.Context(), client, namespacePath, ExtendedKeyMetadata, metadataBytes)
	})
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to update namespace")
		return err
	}

	resp := &UpdateNamespaceResponse{
		Namespace:      metadata.Namespace,
		TableBucketARN: req.TableBucketARN,
		Properties:     metadata.Properties,
	}

	h.writeJSON(w, http.StatusOK, resp)
	return nil
}

// handleListNamespaces lists all namespaces in a table bucket
func (h *S3TablesHandler) handleListNamespaces(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	var req ListNamespacesRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	if req.TableBucketARN == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "tableBucketARN is required")
		return fmt.Errorf("tableBucketARN is required")
	}

	bucketName, err := parseBucketNameFromARN(req.TableBucketARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	maxNamespaces := req.MaxNamespaces
	if maxNamespaces <= 0 {
		maxNamespaces = 100
	}

	bucketPath := GetTableBucketPath(bucketName)

	// Check permission (check bucket ownership)
	var bucketMetadata tableBucketMetadata
	var bucketPolicy string
	var bucketTags map[string]string
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyMetadata)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &bucketMetadata); err != nil {
			return fmt.Errorf("failed to unmarshal bucket metadata: %w", err)
		}

		// Fetch bucket policy if it exists
		policyData, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
		if err == nil {
			bucketPolicy = string(policyData)
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch bucket policy: %v", err)
		}
		bucketTags, err = h.readTags(r.Context(), client, bucketPath)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) || errors.Is(err, ErrAttributeNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchBucket, fmt.Sprintf("table bucket %s not found", bucketName))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to list namespaces: %v", err))
		}
		return err
	}

	bucketARN := h.generateTableBucketARN(bucketMetadata.OwnerAccountID, bucketName)
	principal := h.getAccountID(r)
	identityActions := getIdentityActions(r)
	if !CheckPermissionWithContext("ListNamespaces", principal, bucketMetadata.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
		TrustedIssuers:   h.trustedIssuers(r),
		IsLocalAccountID: h.localAccountIDs,
		TableBucketName:  bucketName,
		TableBucketTags:  bucketTags,
		IdentityActions:  identityActions,
		DefaultAllow:     h.defaultAllowFor(r),
	}) {
		h.writeError(w, http.StatusNotFound, ErrCodeNoSuchBucket, fmt.Sprintf("table bucket %s not found", bucketName))
		return ErrAccessDenied
	}

	var namespaces []NamespaceSummary

	lastFileName := req.ContinuationToken
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		for len(namespaces) < maxNamespaces {
			resp, err := client.ListEntries(r.Context(), &filer_pb.ListEntriesRequest{
				Directory:          bucketPath,
				Limit:              uint32(maxNamespaces * 2),
				StartFromFileName:  lastFileName,
				InclusiveStartFrom: lastFileName == "" || lastFileName == req.ContinuationToken,
			})
			if err != nil {
				return err
			}

			hasMore := false
			for {
				entry, respErr := resp.Recv()
				if respErr != nil {
					if respErr == io.EOF {
						break
					}
					return respErr
				}
				if entry.Entry == nil {
					continue
				}

				// Skip the start item if it was included in the previous page
				if len(namespaces) == 0 && req.ContinuationToken != "" && entry.Entry.Name == req.ContinuationToken {
					continue
				}

				hasMore = true
				lastFileName = entry.Entry.Name

				if !entry.Entry.IsDirectory {
					continue
				}

				// Skip hidden entries
				if strings.HasPrefix(entry.Entry.Name, ".") {
					continue
				}

				// Apply prefix filter
				if req.Prefix != "" && !strings.HasPrefix(entry.Entry.Name, req.Prefix) {
					continue
				}

				// Read metadata from extended attribute
				data, ok := entry.Entry.Extended[ExtendedKeyMetadata]
				if !ok {
					continue
				}

				var metadata namespaceMetadata
				if err := json.Unmarshal(data, &metadata); err != nil {
					continue
				}

				if metadata.OwnerAccountID != bucketMetadata.OwnerAccountID {
					continue
				}

				namespaces = append(namespaces, NamespaceSummary{
					Namespace: metadata.Namespace,
					CreatedAt: metadata.CreatedAt,
				})

				if len(namespaces) >= maxNamespaces {
					return nil
				}
			}

			if !hasMore {
				break
			}
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			namespaces = []NamespaceSummary{}
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to list namespaces: %v", err))
			return err
		}
	}

	paginationToken := ""
	if len(namespaces) >= maxNamespaces {
		paginationToken = lastFileName
	}

	resp := &ListNamespacesResponse{
		Namespaces:        namespaces,
		ContinuationToken: paginationToken,
	}

	h.writeJSON(w, http.StatusOK, resp)
	return nil
}

// handleDeleteNamespace deletes a namespace from a table bucket
func (h *S3TablesHandler) handleDeleteNamespace(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	var req DeleteNamespaceRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	if req.TableBucketARN == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "tableBucketARN is required")
		return fmt.Errorf("tableBucketARN is required")
	}

	namespaceName, err := validateNamespace(req.Namespace)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	bucketName, err := parseBucketNameFromARN(req.TableBucketARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	namespacePath := GetNamespacePath(bucketName, namespaceName)
	bucketPath := GetTableBucketPath(bucketName)

	// Check if namespace exists and get metadata for permission check
	var metadata namespaceMetadata
	var bucketPolicy string
	var bucketTags map[string]string
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.readCatalogRecord(r.Context(), client, namespacePath)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &metadata); err != nil {
			return fmt.Errorf("failed to unmarshal metadata: %w", err)
		}

		// Fetch bucket policy if it exists
		policyData, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
		if err == nil {
			bucketPolicy = string(policyData)
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch bucket policy: %v", err)
		}
		bucketTags, err = h.readTags(r.Context(), client, bucketPath)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchNamespace, fmt.Sprintf("namespace %s not found", flattenNamespace(req.Namespace)))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to get namespace metadata: %v", err))
		}
		return err
	}

	bucketARN := h.generateTableBucketARN(metadata.OwnerAccountID, bucketName)
	principal := h.getAccountID(r)
	identityActions := getIdentityActions(r)
	if !CheckPermissionWithContext("DeleteNamespace", principal, metadata.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
		TrustedIssuers:   h.trustedIssuers(r),
		IsLocalAccountID: h.localAccountIDs,
		TableBucketName:  bucketName,
		Namespace:        namespaceName,
		TableBucketTags:  bucketTags,
		IdentityActions:  identityActions,
		DefaultAllow:     h.defaultAllowFor(r),
	}) {
		h.writeError(w, http.StatusNotFound, ErrCodeNoSuchNamespace, fmt.Sprintf("namespace %s not found", flattenNamespace(req.Namespace)))
		return ErrAccessDenied
	}

	// Check if namespace is empty
	var contents namespaceContents
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		var scanErr error
		contents, scanErr = h.scanNamespace(r.Context(), client, namespacePath)
		return scanErr
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchNamespace, fmt.Sprintf("namespace %s not found", flattenNamespace(req.Namespace)))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to list namespace entries: %v", err))
		}
		return err
	}

	if contents.hasChildren {
		h.writeError(w, http.StatusConflict, ErrCodeNamespaceNotEmpty, "namespace is not empty")
		return fmt.Errorf("namespace not empty")
	}

	// Delete the namespace. Files kept by a table drop that did not purge
	// outlive it: only the namespace's catalog record goes, and the directory
	// stays to hold them until the namespace is created again.
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		if contents.retainsFiles {
			return h.removeExtendedAttributes(r.Context(), client, namespacePath, s3tablesExtendedKeys...)
		}
		return h.deleteDirectory(r.Context(), client, namespacePath)
	})

	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to delete namespace")
		return err
	}

	h.writeJSON(w, http.StatusOK, nil)
	return nil
}

// namespaceContents is what DeleteNamespace finds under a namespace.
type namespaceContents struct {
	// hasChildren is set when a table, a view, or a file sits directly in the
	// namespace; any of them blocks the delete.
	hasChildren bool
	// retainsFiles is set when a directory without a catalog record still
	// holds files: a table dropped without purge left them for a later
	// register_table, so they must survive the namespace.
	retainsFiles bool
}

// scanNamespace reads every entry under a namespace, paging past the first
// listing page so a table cannot hide behind leftover directories. Tables,
// views and files are children that block the delete. A directory without a
// catalog record is what a rename or a dropped table leaves behind: an empty
// one goes with the namespace, one that still holds files is retained.
func (h *S3TablesHandler) scanNamespace(ctx context.Context, client filer_pb.SeaweedFilerClient, namespacePath string) (namespaceContents, error) {
	var contents namespaceContents
	err := h.forEachChild(ctx, client, namespacePath, func(entry *filer_pb.Entry) (bool, error) {
		if strings.HasPrefix(entry.Name, ".") {
			return true, nil
		}
		if _, hasMetadata := entry.Extended[ExtendedKeyMetadata]; hasMetadata || !entry.IsDirectory {
			contents.hasChildren = true
			return false, nil
		}
		if !contents.retainsFiles {
			held, err := h.holdsFiles(ctx, client, path.Join(namespacePath, entry.Name))
			if err != nil {
				return false, err
			}
			contents.retainsFiles = held
		}
		return true, nil
	})
	return contents, err
}
