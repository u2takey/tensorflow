/* Copyright 2015 The TensorFlow Authors. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
==============================================================================*/
#include "tensorflow/core/platform/cos/cos_file_system.h"
#include "tensorflow/core/lib/io/path.h"
#include "tensorflow/core/lib/strings/str_util.h"
#include "tensorflow/core/platform/mutex.h"
#include "tensorflow/core/platform/file_system_helper.h"

#include <cos_api.h>
#include <cos_config.h>
#include <cos_sys_config.h>
#include <cos_defines.h>
#include <util/codec_util.h>


#include <cstdlib>
#include <sstream>
#include <iomanip>
#include <random>

namespace tensorflow {

namespace {
static const char* kCosFileSystemAllocationTag = "CosFileSystemAllocation";
static const size_t kCosReadAppendableFileBufferSize = 1024 * 1024;
static const int kCosGetChildrenMaxKeys = 1000;

qcloud_cos::CosConfig& GetDefaultClientConfig() {
  static mutex cfg_lock(LINKER_INITIALIZED);
  static bool init(false);
  static qcloud_cos::CosConfig cfg;

  std::lock_guard<mutex> lock(cfg_lock);

  if (!init) {
    const char* config_file_env = getenv("COS_CONFIG_FILE");
    if (config_file_env) {
      cfg = qcloud_cos::CosConfig(std::string(config_file_env));
    }else{
      const char* cos_appid = getenv("COS_APPID");
      if (cos_appid) {
        int64 appid;
        if (strings::safe_strto64(cos_appid, &appid)) {
          cfg.SetAppId(appid);
        }
      }
      const char* region = getenv("COS_REGION");
      if (region) {
        cfg.SetRegion(std::string(region));
      }
      const char* access_key = getenv("COS_ACCESS_KEY");
      if (access_key) {
        cfg.SetAccessKey(std::string(access_key));
      }

      const char* secret_key = getenv("COS_SECRET_KEY");
      if (secret_key) {
        cfg.SetSecretKey(std::string(secret_key));
      }

      const char* debug = getenv("COS_DEBUG");
      if (debug) {
        qcloud_cos::CosSysConfig::SetLogLevel(qcloud_cos::COS_LOG_DBG);
      }else{
        qcloud_cos::CosSysConfig::SetLogLevel(qcloud_cos::COS_LOG_ERR);
      }
    }

    init = true;
  }

  return cfg;
};

void ShutdownClient(qcloud_cos::CosAPI* cos_client) {
  if (cos_client != nullptr) {
    delete cos_client;
  }
}

Status ParseCosPath(const string& fname, bool empty_object_ok, string* bucket,
                   string* object) {
  if (!bucket || !object) {
    return errors::Internal("bucket and object cannot be null.");
  }
  StringPiece scheme, bucketp, objectp;
  io::ParseURI(fname, &scheme, &bucketp, &objectp);
  if (scheme != "cos") {
    return errors::InvalidArgument("cos path doesn't start with 'cos://': ",
                                   fname);
  }
  *bucket = bucketp.ToString();
  if (bucket->empty() || *bucket == ".") {
    return errors::InvalidArgument("cos path doesn't contain a bucket name: ",
                                   fname);
  }
  objectp.Consume("/");
  *object = objectp.ToString();
  if (!empty_object_ok && object->empty()) {
    return errors::InvalidArgument("cos path doesn't contain an object name: ",
                                   fname);
  }
  return Status::OK();
}

class CosRandomAccessFile : public RandomAccessFile {
 public:
  CosRandomAccessFile(const string& bucket, const string& object,
                     std::shared_ptr<qcloud_cos::CosAPI> cos_client)
      : bucket_(bucket), object_(object), cos_client_(cos_client) {}

  Status Read(uint64 offset, size_t n, StringPiece* result,
              char* scratch) const override {

    std::stringstream ss;
    qcloud_cos::GetObjectByStreamReq getObjectRequest(bucket_, object_, ss);
    qcloud_cos::GetObjectByStreamResp getObjectResponse;

    string range = strings::StrCat("bytes=", offset, "-", offset + n - 1);
    getObjectRequest.AddHeader("Range", range);

    auto outcome = this->cos_client_->GetObject(getObjectRequest, &getObjectResponse);
    if (!outcome.IsSucc()) {
      n = 0;
      *result = StringPiece(scratch, n);
      return Status(error::OUT_OF_RANGE, "Read less bytes than requested");
    }
    n = getObjectResponse.GetContentLength();
    ss.read(scratch, n);

    *result = StringPiece(scratch, n);
    return Status::OK();
  }

 private:
  string bucket_;
  string object_;
  std::shared_ptr<qcloud_cos::CosAPI> cos_client_;
};

std::string random_string()
{
     std::string str("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz");

     std::random_device rd;
     std::mt19937 generator(rd());

     std::shuffle(str.begin(), str.end(), generator);

     return str.substr(0, 10);
}

class CosWritableFile : public WritableFile {
 public:
  CosWritableFile(const string& bucket, const string& object,
                 std::shared_ptr<qcloud_cos::CosAPI> cos_client)
      : bucket_(bucket),
        object_(object),
        cos_client_(cos_client),
        sync_needed_(true){
    tmp_content_filename_ = "/tmp/cos_filesystem_" + random_string();
    outfile_.open(tmp_content_filename_, std::ios_base::binary | std::ios_base::trunc | std::ios_base::in | std::ios_base::out);

                }

  Status Append(const StringPiece& data) override {
    if (!outfile_) {
      return errors::FailedPrecondition(
          "The internal temporary file is not writable.");
    }
    sync_needed_ = true;
    outfile_.write(data.data(), data.size());
    if (!outfile_.good()) {
      return errors::Internal(
          "Could not append to the internal temporary file.");
    }
    return Status::OK();
  }

  Status Close() override {
    if (outfile_) {
      TF_RETURN_IF_ERROR(Sync());
      outfile_.close();
      std::remove(tmp_content_filename_.c_str());
    }
    return Status::OK();
  }

  Status Flush() override { return Sync(); }

  Status Sync() override {
    if (!outfile_) {
      return errors::FailedPrecondition(
          "The internal temporary file is not writable.");
    }
    if (!sync_needed_) {
      return Status::OK();
    }
    //outfile_.flush();
    if (!outfile_.good()) {
      return errors::Internal(
          "Could not write to the internal temporary file.");
    }
    qcloud_cos::PutObjectByStreamReq putObjectRequest(bucket_, object_, outfile_);

    qcloud_cos::PutObjectByStreamResp putObjectResponse;
    long offset = outfile_.tellp();
    outfile_.seekg(0);
    // std::string str((std::istreambuf_iterator<char>(outfile_)), std::istreambuf_iterator<char>());
    // std::cout << offset << "readsss." << str << std::endl;
    auto putObjectOutcome = this->cos_client_->PutObject(putObjectRequest, &putObjectResponse);
    outfile_.clear();
    outfile_.seekp(offset);
    if (!putObjectOutcome.IsSucc()) {
      string error = putObjectOutcome.GetErrorInfo() + ": " + putObjectOutcome.GetErrorMsg();
      return errors::Internal(error);
    }
    return Status::OK();
  }

 private:
  string bucket_;
  string object_;
  std::shared_ptr<qcloud_cos::CosAPI> cos_client_;
  string tmp_content_filename_;
  bool sync_needed_;
  std::fstream outfile_;
};

class CosReadOnlyMemoryRegion : public ReadOnlyMemoryRegion {
 public:
  CosReadOnlyMemoryRegion(std::unique_ptr<char[]> data, uint64 length)
      : data_(std::move(data)), length_(length) {}
  const void* data() override { return reinterpret_cast<void*>(data_.get()); }
  uint64 length() override { return length_; }

 private:
  std::unique_ptr<char[]> data_;
  uint64 length_;
};

}  // namespace

CosFileSystem::CosFileSystem()
    : cos_client_(nullptr, ShutdownClient), client_lock_() {}

CosFileSystem::~CosFileSystem() {}

// Initializes s3_client_, if needed, and returns it.
std::shared_ptr<qcloud_cos::CosAPI> CosFileSystem::GetCosClient() {
  std::lock_guard<mutex> lock(this->client_lock_);
  if (this->cos_client_.get() == nullptr) {
    auto api = new qcloud_cos::CosAPI(GetDefaultClientConfig());
    this->cos_client_ = std::shared_ptr<qcloud_cos::CosAPI>(api);
  }
  return this->cos_client_;
}

Status CosFileSystem::NewRandomAccessFile(
    const string& fname, std::unique_ptr<RandomAccessFile>* result) {
  string bucket, object;
  TF_RETURN_IF_ERROR(ParseCosPath(fname, false, &bucket, &object));
  result->reset(new CosRandomAccessFile(bucket, object, this->GetCosClient()));
  return Status::OK();
}

Status CosFileSystem::NewWritableFile(const string& fname,
                                     std::unique_ptr<WritableFile>* result) {
  string bucket, object;
  TF_RETURN_IF_ERROR(ParseCosPath(fname, false, &bucket, &object));
  this->GetCosClient();
  result->reset(new CosWritableFile(bucket, object, this->GetCosClient()));
  // auto a = new CosWritableFile(bucket, object, this->GetCosClient());
  return Status::OK();
}

Status CosFileSystem::NewAppendableFile(const string& fname,
                                       std::unique_ptr<WritableFile>* result) {
  std::unique_ptr<RandomAccessFile> reader;
  TF_RETURN_IF_ERROR(NewRandomAccessFile(fname, &reader));
  std::unique_ptr<char[]> buffer(new char[kCosReadAppendableFileBufferSize]);
  Status status;
  uint64 offset = 0;
  StringPiece read_chunk;

  string bucket, object;
  TF_RETURN_IF_ERROR(ParseCosPath(fname, false, &bucket, &object));
  result->reset(new CosWritableFile(bucket, object, this->GetCosClient()));

  while (true) {
    status = reader->Read(offset, kCosReadAppendableFileBufferSize, &read_chunk,
                          buffer.get());
    if (status.ok()) {
      (*result)->Append(read_chunk);
      offset += kCosReadAppendableFileBufferSize;
    } else if (status.code() == error::OUT_OF_RANGE) {
      (*result)->Append(read_chunk);
      break;
    } else {
      (*result).reset();
      return status;
    }
  }

  return Status::OK();
}

Status CosFileSystem::NewReadOnlyMemoryRegionFromFile(
    const string& fname, std::unique_ptr<ReadOnlyMemoryRegion>* result) {
  uint64 size;
  TF_RETURN_IF_ERROR(GetFileSize(fname, &size));
  std::unique_ptr<char[]> data(new char[size]);

  std::unique_ptr<RandomAccessFile> file;
  TF_RETURN_IF_ERROR(NewRandomAccessFile(fname, &file));

  StringPiece piece;
  TF_RETURN_IF_ERROR(file->Read(0, size, &piece, data.get()));

  result->reset(new CosReadOnlyMemoryRegion(std::move(data), size));
  return Status::OK();
}

Status CosFileSystem::FileExists(const string& fname) {
  FileStatistics stats;
  TF_RETURN_IF_ERROR(this->Stat(fname, &stats));
  return Status::OK();
}

Status CosFileSystem::GetChildren(const string& dir,
                                 std::vector<string>* result) {
  string bucket, prefix;
  TF_RETURN_IF_ERROR(ParseCosPath(dir, false, &bucket, &prefix));

  if (prefix.back() != '/') {
    prefix.push_back('/');
  }

  qcloud_cos::GetBucketReq listObjectsRequest(bucket);
  qcloud_cos::GetBucketResp *listObjectsResponse;
  listObjectsRequest.SetPrefix(prefix);
  listObjectsRequest.SetMaxKeys(kCosGetChildrenMaxKeys);
  listObjectsRequest.SetDelimiter("/");


  do {
    listObjectsResponse = new qcloud_cos::GetBucketResp();
    auto listObjectsOutcome =
        this->GetCosClient()->GetBucket(listObjectsRequest, listObjectsResponse);
    if (!listObjectsOutcome.IsSucc()) {
      std::string error = listObjectsOutcome.GetErrorInfo() + ": " + listObjectsOutcome.GetErrorMsg();
      return errors::Internal(error);
    }

    for (const auto& object : listObjectsResponse->GetCommonPrefixes()) {
      //std::cout << "CommonPrefixes. " << object << std::endl;
      std::string entry = object.substr(strlen(prefix.c_str()));
      if (entry.length() > 0) {
        entry.erase(entry.length() - 1);
        result->push_back(entry.c_str());
      }
    }
    for (const auto& object : listObjectsResponse->GetContents()) {
      //std::cout << "Contents. " << object.m_key << std::endl;
      std::string s = object.m_key;
      std::string entry = s.substr(strlen(prefix.c_str()));
      if (entry.length() > 0) {
        result->push_back(entry.c_str());
      }
    }
    listObjectsRequest.SetMarker(listObjectsResponse->GetNextMarker());
  } while (listObjectsResponse->IsTruncated());

  return Status::OK();
}

Status CosFileSystem::Stat(const string& fname, FileStatistics* stats) {
  string bucket, object;
  TF_RETURN_IF_ERROR(ParseCosPath(fname, true, &bucket, &object));

  if (object.empty()) {

    auto exsit = this->GetCosClient()->IsBucketExist(bucket);
    if (!exsit) {
      qcloud_cos::CosConfig cfg = GetDefaultClientConfig();
      return errors::NotFound("The bucket ", bucket, " was not found." + cfg.GetRegion() + cfg.GetAccessKey() );
    }
    stats->length = 0;
    stats->is_directory = 1;
    return Status::OK();
  }

  bool found = false;

  qcloud_cos::HeadObjectReq headObjectRequest(bucket, object);
  qcloud_cos::HeadObjectResp headObjectResponse;
  auto headObjectOutcome = this->GetCosClient()->HeadObject(headObjectRequest, &headObjectResponse);
  if (headObjectOutcome.IsSucc()) {
    stats->length = headObjectResponse.GetContentLength();
    stats->is_directory = 0;

    std::tm t = {};
    char buf[1024];
    strncpy(buf, headObjectResponse.GetLastModified().c_str(), sizeof(buf));
    buf[sizeof(buf) - 1] = 0;
    if ( strptime(buf, "%a, %d %b %Y %H:%M:%S GMT", &t) != NULL ){
        time_t t1 = mktime(&t);
        stats->mtime_nsec = int64(t1);
    }
    // std::tm t = {};
    // std::istringstream ss(headObjectResponse.GetLastModified());
    // ss >> std::get_time(&t, "%a, %d %b %Y %H:%M:%S GMT");
    // if (!ss.fail()) {
    //   time_t t1 = mktime(&t);
    //   stats->mtime_nsec = int64(t1);
    // }

    found = true;
    // s3 实现 为什么不return????
    return Status::OK();
  }
  string prefix = object;
  if (prefix.back() != '/') {
    prefix.push_back('/');
  }
  qcloud_cos::GetBucketReq listObjectsRequest(bucket);
  qcloud_cos::GetBucketResp listObjectsResponse;
  listObjectsRequest.SetMaxKeys(2);
  listObjectsRequest.SetPrefix(object);
  auto listObjectsOutcome =
      this->GetCosClient()->GetBucket(listObjectsRequest, &listObjectsResponse);
  if (listObjectsOutcome.IsSucc()) {
    if (listObjectsResponse.GetContents().size() > 0) {
      stats->length = 0;
      stats->is_directory = 1;
      found = true;
    }
  }
  if (!found) {
    return errors::NotFound("Object ", fname, " does not exist");
  }
  return Status::OK();
}

Status CosFileSystem::DeleteFile(const string& fname) {
  string bucket, object;
  TF_RETURN_IF_ERROR(ParseCosPath(fname, false, &bucket, &object));

  qcloud_cos::DeleteObjectReq deleteObjectRequest(bucket, object);
  qcloud_cos::DeleteObjectResp deleteObjectResponse;

  auto deleteObjectOutcome =
      this->GetCosClient()->DeleteObject(deleteObjectRequest, &deleteObjectResponse);
  if (!deleteObjectOutcome.IsSucc()) {
    std::string error = deleteObjectOutcome.GetErrorInfo() + ": " + deleteObjectOutcome.GetErrorMsg();
    return errors::Internal(error);
  }
  return Status::OK();
}

Status CosFileSystem::CreateDir(const string& dirname) {
  string bucket, object;
  TF_RETURN_IF_ERROR(ParseCosPath(dirname, true, &bucket, &object));

  if (object.empty()) {
    auto exsit = this->GetCosClient()->IsBucketExist(bucket);
    if (!exsit) {
      return errors::NotFound("The bucket ", bucket, " was not found.");
    }
    return Status::OK();
  }
  string filename = dirname;
  if (filename.back() != '/') {
    filename.push_back('/');
  }
  std::unique_ptr<WritableFile> file;
  TF_RETURN_IF_ERROR(NewWritableFile(filename, &file));
  TF_RETURN_IF_ERROR(file->Close());
  return Status::OK();
}

Status CosFileSystem::DeleteDir(const string& dirname) {
  string bucket, object;
  TF_RETURN_IF_ERROR(ParseCosPath(dirname, false, &bucket, &object));

  string prefix = object;
  if (prefix.back() != '/') {
    prefix.push_back('/');
  }

  qcloud_cos::GetBucketReq listObjectsRequest(bucket);
  qcloud_cos::GetBucketResp listObjectsResponse;
  listObjectsRequest.SetPrefix(prefix);
  listObjectsRequest.SetMaxKeys(2);
  auto listObjectsOutcome =
      this->GetCosClient()->GetBucket(listObjectsRequest, &listObjectsResponse);
  if (listObjectsOutcome.IsSucc()) {
    auto contents = listObjectsResponse.GetContents();
    if (contents.size() > 1 ||
        (contents.size() == 1 && contents[0].m_key != prefix)) {
      return errors::FailedPrecondition("Cannot delete a non-empty directory." + contents[0].m_key + "."+ prefix);
    }
    if (contents.size() == 1 && contents[0].m_key == prefix) {
      string filename = dirname;
      if (filename.back() != '/') {
        filename.push_back('/');
      }
      return DeleteFile(filename);
    }
  }
  return Status::OK();
}

Status CosFileSystem::GetFileSize(const string& fname, uint64* file_size) {
  FileStatistics stats;
  TF_RETURN_IF_ERROR(this->Stat(fname, &stats));
  *file_size = stats.length;
  return Status::OK();
}

Status CosFileSystem::RenameFile(const string& src, const string& target) {
  string src_bucket, src_object, target_bucket, target_object;
  TF_RETURN_IF_ERROR(ParseCosPath(src, false, &src_bucket, &src_object));
  TF_RETURN_IF_ERROR(
      ParseCosPath(target, false, &target_bucket, &target_object));
  if (src_object.back() == '/') {
    if (target_object.back() != '/') {
      target_object.push_back('/');
    }
  } else {
    if (target_object.back() == '/') {
      target_object.pop_back();
    }
  }

  qcloud_cos::GetBucketReq listObjectsRequest(src_bucket);
  qcloud_cos::GetBucketResp *listObjectsResponse;
  listObjectsRequest.SetPrefix(src_object);
  listObjectsRequest.SetMaxKeys(kCosGetChildrenMaxKeys);

  do {
    listObjectsResponse = new qcloud_cos::GetBucketResp();
    auto listObjectsOutcome =
        this->GetCosClient()->GetBucket(listObjectsRequest, listObjectsResponse);
    if (!listObjectsOutcome.IsSucc()) {
      string error = listObjectsOutcome.GetErrorInfo() + ": " + listObjectsOutcome.GetErrorMsg();
      return errors::Internal(error);
    }

    for (const auto& object : listObjectsResponse->GetContents()) {
      std::string src_key = object.m_key;
      std::string target_key = src_key;
      target_key.replace(0, src_object.length(), target_object.c_str());
      // x-cos-copy-source: <Bucketname>-<APPID>.cos.<Region>.myqcloud.com/filepath
      string source = src_bucket + ".cos." + GetDefaultClientConfig().GetRegion() + ".myqcloud.com/" + src_key;

      qcloud_cos::CopyReq copyObjectRequest(target_bucket, target_key);
      qcloud_cos::CopyResp copyObjectResponse;
      copyObjectRequest.SetXCosCopySource(source);

      auto copyObjectOutcome =
          this->GetCosClient()->Copy(copyObjectRequest, &copyObjectResponse);
      if (!copyObjectOutcome.IsSucc()) {
        string error = copyObjectOutcome.GetErrorInfo() + ": " + copyObjectOutcome.GetErrorMsg();
        return errors::Internal(error);
      }

      qcloud_cos::DeleteObjectReq deleteObjectRequest(src_bucket, src_key);
      qcloud_cos::DeleteObjectResp deleteObjectResponse;

      auto deleteObjectOutcome =
          this->GetCosClient()->DeleteObject(deleteObjectRequest, &deleteObjectResponse);
      if (!deleteObjectOutcome.IsSucc()) {
        string error = deleteObjectOutcome.GetErrorInfo() + ": " + deleteObjectOutcome.GetErrorMsg();
        return errors::Internal(error);
      }
    }
    listObjectsRequest.SetMarker(listObjectsResponse->GetNextMarker());
  } while (listObjectsResponse->IsTruncated());

  return Status::OK();
}

Status CosFileSystem::GetMatchingPaths(const string& pattern,
                                      std::vector<string>* results) {
  return internal::GetMatchingPaths(this, Env::Default(), pattern, results);
}

REGISTER_FILE_SYSTEM("cos", CosFileSystem);

}  // namespace tensorflow
