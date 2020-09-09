var crypto = require("crypto");
var stream = require("stream");
var fileType = require("file-type");
var isSvg = require("is-svg");
var parallel = require("run-parallel");

function staticValue(value) {
  return function (req, file, cb) {
    cb(null, value);
  };
}

var defaultAcl = staticValue("private");
var defaultContentType = staticValue("application/octet-stream");

var defaultMetadata = staticValue(null);
var defaultCacheControl = staticValue(null);
var defaultContentDisposition = staticValue(null);
var defaultStorageClass = staticValue("STANDARD");
var defaultSSE = staticValue(null);
var defaultSSEKMS = staticValue(null);

function defaultKey(req, file, cb) {
  crypto.randomBytes(16, function (err, raw) {
    cb(err, err ? undefined : raw.toString("hex"));
  });
}

function autoContentType(req, file, cb) {
  file.stream.once("data", function (firstChunk) {
    var type = fileType(firstChunk);
    var mime;

    if (type) {
      mime = type.mime;
    } else if (isSvg(firstChunk)) {
      mime = "image/svg+xml";
    } else {
      mime = "application/octet-stream";
    }

    var outStream = new stream.PassThrough();

    outStream.write(firstChunk);
    file.stream.pipe(outStream);

    cb(null, mime, outStream);
  });
}

function collect(storage, req, file, cb) {
  parallel(
    [
      storage.getBucket.bind(storage, req, file),
      storage.getKey.bind(storage, req, file),
      storage.getAcl.bind(storage, req, file),
      storage.getMetadata.bind(storage, req, file),
      storage.getCacheControl.bind(storage, req, file),
      storage.getContentDisposition.bind(storage, req, file),
      storage.getStorageClass.bind(storage, req, file),
      storage.getSSE.bind(storage, req, file),
      storage.getSSEKMS.bind(storage, req, file),
    ],
    function (err, values) {
      if (err) return cb(err);

      storage.getContentType(req, file, function (
        err,
        contentType,
        replacementStream
      ) {
        if (err) return cb(err);

        cb.call(storage, null, {
          bucket: values[0],
          key: values[1],
          acl: values[2],
          metadata: values[3],
          cacheControl: values[4],
          contentDisposition: values[5],
          storageClass: values[6],
          contentType: contentType,
          replacementStream: replacementStream,
          serverSideEncryption: values[7],
          sseKmsKeyId: values[8],
        });
      });
    }
  );
}

function S3Storage(opts) {
  switch (typeof opts.s3) {
    case "object":
      this.s3 = opts.s3;
      break;
    default:
      throw new TypeError("Expected opts.s3 to be object");
  }

  switch (typeof opts.bucket) {
    case "function":
      this.getBucket = opts.bucket;
      break;
    case "string":
      this.getBucket = staticValue(opts.bucket);
      break;
    case "undefined":
      throw new Error("bucket is required");
    default:
      throw new TypeError(
        "Expected opts.bucket to be undefined, string or function"
      );
  }

  switch (typeof opts.key) {
    case "function":
      this.getKey = opts.key;
      break;
    case "undefined":
      this.getKey = defaultKey;
      break;
    default:
      throw new TypeError("Expected opts.key to be undefined or function");
  }

  switch (typeof opts.acl) {
    case "function":
      this.getAcl = opts.acl;
      break;
    case "string":
      this.getAcl = staticValue(opts.acl);
      break;
    case "undefined":
      this.getAcl = defaultAcl;
      break;
    default:
      throw new TypeError(
        "Expected opts.acl to be undefined, string or function"
      );
  }

  switch (typeof opts.contentType) {
    case "function":
      this.getContentType = opts.contentType;
      break;
    case "undefined":
      this.getContentType = defaultContentType;
      break;
    default:
      throw new TypeError(
        "Expected opts.contentType to be undefined or function"
      );
  }

  switch (typeof opts.metadata) {
    case "function":
      this.getMetadata = opts.metadata;
      break;
    case "undefined":
      this.getMetadata = defaultMetadata;
      break;
    default:
      throw new TypeError("Expected opts.metadata to be undefined or function");
  }

  switch (typeof opts.cacheControl) {
    case "function":
      this.getCacheControl = opts.cacheControl;
      break;
    case "string":
      this.getCacheControl = staticValue(opts.cacheControl);
      break;
    case "undefined":
      this.getCacheControl = defaultCacheControl;
      break;
    default:
      throw new TypeError(
        "Expected opts.cacheControl to be undefined, string or function"
      );
  }

  switch (typeof opts.contentDisposition) {
    case "function":
      this.getContentDisposition = opts.contentDisposition;
      break;
    case "string":
      this.getContentDisposition = staticValue(opts.contentDisposition);
      break;
    case "undefined":
      this.getContentDisposition = defaultContentDisposition;
      break;
    default:
      throw new TypeError(
        "Expected opts.contentDisposition to be undefined, string or function"
      );
  }

  switch (typeof opts.storageClass) {
    case "function":
      this.getStorageClass = opts.storageClass;
      break;
    case "string":
      this.getStorageClass = staticValue(opts.storageClass);
      break;
    case "undefined":
      this.getStorageClass = defaultStorageClass;
      break;
    default:
      throw new TypeError(
        "Expected opts.storageClass to be undefined, string or function"
      );
  }

  switch (typeof opts.serverSideEncryption) {
    case "function":
      this.getSSE = opts.serverSideEncryption;
      break;
    case "string":
      this.getSSE = staticValue(opts.serverSideEncryption);
      break;
    case "undefined":
      this.getSSE = defaultSSE;
      break;
    default:
      throw new TypeError(
        "Expected opts.serverSideEncryption to be undefined, string or function"
      );
  }

  switch (typeof opts.sseKmsKeyId) {
    case "function":
      this.getSSEKMS = opts.sseKmsKeyId;
      break;
    case "string":
      this.getSSEKMS = staticValue(opts.sseKmsKeyId);
      break;
    case "undefined":
      this.getSSEKMS = defaultSSEKMS;
      break;
    default:
      throw new TypeError(
        "Expected opts.sseKmsKeyId to be undefined, string, or function"
      );
  }

  this.transforms = opts.transforms;
}

S3Storage.prototype._handleFile = function (req, file, cb) {
  collect(this, req, file, function (err, opts) {
    if (err) return cb(err);

    if (this.transforms) {
      // Transform image

      // All transformer keys
			const transformerKeys = Object.keys(this.transforms)

			// Default sizes to 0 for all transformers
			const currentSizes = transformerKeys.reduce((acc, cur) => ({ ...acc, [cur]: 0 }), {})

			// Transformers functions
			const transformFunctions = transformerKeys.reduce(
				(acc, cur) => ({ ...acc, [cur]: this.transforms[cur]() }),
				{}
			)

			const fileStream = opts.replacementStream || file.stream
			console.log(file.stream)

			// Transformers pipes for all keys
			const transformPipes = transformerKeys.reduce(
				(acc, cur) => ({ ...acc, [cur]: fileStream.pipe(transformFunctions[cur]) }),
				{}
      )
      
      // Create params for S3
			const transformParams = transformerKeys.reduce(
				(acc, cur) => ({
					...acc,
					[cur]: {
						Bucket: opts.bucket,
						Key: `${cur}-${opts.key}`,
						ACL: opts.acl,
						CacheControl: opts.cacheControl,
						ContentType: opts.contentType,
						Metadata: opts.metadata,
						StorageClass: opts.storageClass,
						ServerSideEncryption: opts.serverSideEncryption,
						SSEKMSKeyId: opts.sseKmsKeyId,
						Body: transformPipes[cur]
					}
				}),
				{}
      )
      
      // Add contentDisposition
			if (opts.contentDisposition) {
				transformerKeys.forEach(key => {
					transformParams[key].contentDisposition = opts.contentDisposition
				})
      }
      
      // Uploads objects
			const uploads = transformerKeys.reduce(
				(acc, cur) => ({ ...acc, [cur]: this.s3.upload(transformParams[cur]) }),
				{}
      )
      
      // Update size value
      transformerKeys.forEach(key => {
				uploads[key].on('httpUploadProgress', function(ev) {
					if (ev.total) currentSizes[key] = ev.total
				})
			})

      // Upload and call calback
      try {
				const responses = await Promise.all(transformerKeys.map(key => uploads[key].promise()))
				return cb(
					null,
					transformerKeys.reduce(
						(acc, cur, index) => ({
							...acc,
							[cur]: {
								size: currentSizes[cur],
								bucket: opts.bucket,
								key: responses[index].key,
								acl: opts.acl,
								contentType: opts.contentType,
								contentDisposition: opts.contentDisposition,
								storageClass: opts.storageClass,
								serverSideEncryption: opts.serverSideEncryption,
								metadata: opts.metadata,
								location: responses[index].Location,
								etag: responses[index].ETag,
								versionId: responses[index].VersionId
							}
						}),
						{}
					)
				)
			} catch (e) {
				return cb(e)
      }
    } else {
      // No transformers set
      let currentSize = 0
			const fileStream = opts.replacementStream || file.stream
			const params = {
				Bucket: opts.bucket,
				Key: opts.key,
				ACL: opts.acl,
				CacheControl: opts.cacheControl,
				ContentType: opts.contentType,
				Metadata: opts.metadata,
				StorageClass: opts.storageClass,
				ServerSideEncryption: opts.serverSideEncryption,
				SSEKMSKeyId: opts.sseKmsKeyId,
				Body: fileStream
			}

			// Add contentDisposition
			if (opts.contentDisposition) {
				;(params as any).contentDisposition = opts.contentDisposition
			}
			const upload = this.s3.upload(params)

			upload.on('httpUploadProgress', function(ev) {
				if (ev.total) currentSize = ev.total
			})
			try {
				const response = await upload.promise()
				return cb(null, {
					size: currentSize,
					bucket: opts.bucket,
					key: response.key,
					acl: opts.acl,
					contentType: opts.contentType,
					contentDisposition: opts.contentDisposition,
					storageClass: opts.storageClass,
					serverSideEncryption: opts.serverSideEncryption,
					metadata: opts.metadata,
					location: response.Location,
					etag: response.ETag,
					versionId: response.VersionId
				})
			} catch (e) {
				return cb(e)
			}
    }
  });
};

S3Storage.prototype._removeFile = function (req, file, cb) {
  this.s3.deleteObject({ Bucket: file.bucket, Key: file.key }, cb);
};

module.exports = function (opts) {
  return new S3Storage(opts);
};

module.exports.AUTO_CONTENT_TYPE = autoContentType;
module.exports.DEFAULT_CONTENT_TYPE = defaultContentType;
