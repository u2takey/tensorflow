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

#include "tensorflow/core/lib/core/status_test_util.h"
#include "tensorflow/core/lib/gtl/stl_util.h"
#include "tensorflow/core/lib/io/path.h"
#include "tensorflow/core/platform/file_system.h"
#include "tensorflow/core/platform/test.h"

namespace tensorflow {

namespace {

class CosFileSystemTest : public ::testing::Test {
 protected:
  CosFileSystemTest() {}

  string TmpDir(const string& path) {
    char* test_dir = getenv("COS_TEST_TMPDIR");
    if (test_dir != nullptr) {
      return io::JoinPath(string(test_dir), path);
    } else {
      return "cos://" + io::JoinPath(testing::TmpDir(), path);
    }
  }

  Status WriteString(const string& fname, const string& content) {
    std::unique_ptr<WritableFile> writer;
    TF_RETURN_IF_ERROR(cosfs.NewWritableFile(fname, &writer));
    TF_RETURN_IF_ERROR(writer->Append(content));
    TF_RETURN_IF_ERROR(writer->Close());
    return Status::OK();
  }

  Status ReadAll(const string& fname, string* content) {
    std::unique_ptr<RandomAccessFile> reader;
    TF_RETURN_IF_ERROR(cosfs.NewRandomAccessFile(fname, &reader));

    uint64 file_size = 0;
    TF_RETURN_IF_ERROR(cosfs.GetFileSize(fname, &file_size));

    content->resize(file_size);
    StringPiece result;
    TF_RETURN_IF_ERROR(
        reader->Read(0, file_size, &result, gtl::string_as_array(content)));
    if (file_size != result.size()) {
      return errors::DataLoss("expected ", file_size, " got ", result.size(),
                              " bytes");
    }
    return Status::OK();
  }

  CosFileSystem cosfs;
};

TEST_F(CosFileSystemTest, NewRandomAccessFile) {
  const string fname = TmpDir("RandomAccessFile");
  const string content = "abcdefghijklmn";

  TF_ASSERT_OK(WriteString(fname, content));

  std::unique_ptr<RandomAccessFile> reader;
  TF_EXPECT_OK(cosfs.NewRandomAccessFile(fname, &reader));

  string got;
  got.resize(content.size());
  StringPiece result;
  TF_EXPECT_OK(
      reader->Read(0, content.size(), &result, gtl::string_as_array(&got)));
  EXPECT_EQ(content.size(), result.size());
  EXPECT_EQ(content, result);

  got.clear();
  got.resize(4);
  TF_EXPECT_OK(reader->Read(2, 4, &result, gtl::string_as_array(&got)));
  EXPECT_EQ(4, result.size());
  EXPECT_EQ(content.substr(2, 4), result);
}

TEST_F(CosFileSystemTest, NewWritableFile) {
  std::unique_ptr<WritableFile> writer1;
  const string fname = TmpDir("WritableFile");
  TF_EXPECT_OK(cosfs.NewWritableFile(fname, &writer1));
  // TF_EXPECT_OK(writer->Append("content1,"));
  // TF_EXPECT_OK(writer->Append("content2"));
  //TF_EXPECT_OK(writer->Flush());
  // TF_EXPECT_OK(writer->Sync());
  // TF_EXPECT_OK(writer->Close());

  // string content;
  // TF_EXPECT_OK(ReadAll(fname, &content));
  // EXPECT_EQ("content1,content2", content);
}

TEST_F(CosFileSystemTest, NewAppendableFile) {
  std::unique_ptr<WritableFile> writer;

  const string fname = TmpDir("AppendableFile");
  TF_ASSERT_OK(WriteString(fname, "test"));

  TF_EXPECT_OK(cosfs.NewAppendableFile(fname, &writer));
  TF_EXPECT_OK(writer->Append("content"));
  TF_EXPECT_OK(writer->Close());
}

TEST_F(CosFileSystemTest, NewReadOnlyMemoryRegionFromFile) {
  const string fname = TmpDir("MemoryFile");
  const string content = "content";
  TF_ASSERT_OK(WriteString(fname, content));
  std::unique_ptr<ReadOnlyMemoryRegion> region;
  TF_EXPECT_OK(cosfs.NewReadOnlyMemoryRegionFromFile(fname, &region));

  EXPECT_EQ(content, StringPiece(reinterpret_cast<const char*>(region->data()),
                                 region->length()));
}

TEST_F(CosFileSystemTest, FileExists) {
  const string fname = TmpDir("FileExists");
  // Ensure the file doesn't yet exist.
  TF_ASSERT_OK(cosfs.DeleteFile(fname));
  EXPECT_EQ(error::Code::NOT_FOUND, cosfs.FileExists(fname).code());
  TF_ASSERT_OK(WriteString(fname, "test"));
  TF_EXPECT_OK(cosfs.FileExists(fname));
}

TEST_F(CosFileSystemTest, GetChildren) {
  const string base = TmpDir("GetChildren");
  TF_EXPECT_OK(cosfs.CreateDir(base));

  const string file = io::JoinPath(base, "TestFile.csv");
  TF_EXPECT_OK(WriteString(file, "test"));

  const string subdir = io::JoinPath(base, "SubDir");
  TF_EXPECT_OK(cosfs.CreateDir(subdir));
  // cos object storage doesn't support empty directory, we create file in the
  // directory
  const string subfile = io::JoinPath(subdir, "TestSubFile.csv");
  TF_EXPECT_OK(WriteString(subfile, "test"));

  std::vector<string> children;
  TF_EXPECT_OK(cosfs.GetChildren(base, &children));
  std::sort(children.begin(), children.end());
  EXPECT_EQ(std::vector<string>({"SubDir", "TestFile.csv"}), children);
}

TEST_F(CosFileSystemTest, DeleteFile) {
  const string fname = TmpDir("DeleteFile");
  TF_ASSERT_OK(WriteString(fname, "test"));
  TF_EXPECT_OK(cosfs.DeleteFile(fname));
}

TEST_F(CosFileSystemTest, GetFileSize) {
  const string fname = TmpDir("GetFileSize");
  TF_ASSERT_OK(WriteString(fname, "test"));
  uint64 file_size = 0;
  TF_EXPECT_OK(cosfs.GetFileSize(fname, &file_size));
  EXPECT_EQ(4, file_size);
}

TEST_F(CosFileSystemTest, CreateDir) {
  // cos object storage doesn't support empty directory, we create file in the
  // directory
  const string dir = TmpDir("CreateDir");
  TF_EXPECT_OK(cosfs.CreateDir(dir));

  const string file = io::JoinPath(dir, "CreateDirFile.csv");
  TF_EXPECT_OK(WriteString(file, "test"));
  FileStatistics stat;
  TF_EXPECT_OK(cosfs.Stat(dir, &stat));
  EXPECT_TRUE(stat.is_directory);
}

TEST_F(CosFileSystemTest, DeleteDir) {
  // cos object storage doesn't support empty directory, we create file in the
  // directory
  const string dir = TmpDir("DeleteDir");
  const string file = io::JoinPath(dir, "DeleteDirFile.csv");
  TF_EXPECT_OK(WriteString(file, "test"));
  EXPECT_FALSE(cosfs.DeleteDir(dir).ok());

  TF_EXPECT_OK(cosfs.DeleteFile(file));
  TF_EXPECT_OK(cosfs.DeleteDir(dir));
  FileStatistics stat;
  EXPECT_FALSE(cosfs.Stat(dir, &stat).ok());
}

TEST_F(CosFileSystemTest, RenameFile) {
  const string fname1 = TmpDir("RenameFile1");
  const string fname2 = TmpDir("RenameFile2");
  TF_ASSERT_OK(WriteString(fname1, "test"));
  TF_EXPECT_OK(cosfs.RenameFile(fname1, fname2));
  string content;
  TF_EXPECT_OK(ReadAll(fname2, &content));
  EXPECT_EQ("test", content);
}

TEST_F(CosFileSystemTest, RenameFile_Overwrite) {
  const string fname1 = TmpDir("RenameFile1");
  const string fname2 = TmpDir("RenameFile2");

  TF_ASSERT_OK(WriteString(fname2, "test"));
  TF_EXPECT_OK(cosfs.FileExists(fname2));

  TF_ASSERT_OK(WriteString(fname1, "test"));
  TF_EXPECT_OK(cosfs.RenameFile(fname1, fname2));
  string content;
  TF_EXPECT_OK(ReadAll(fname2, &content));
  EXPECT_EQ("test", content);
}

TEST_F(CosFileSystemTest, StatFile) {
  const string fname = TmpDir("StatFile");
  TF_ASSERT_OK(WriteString(fname, "test"));
  FileStatistics stat;
  TF_EXPECT_OK(cosfs.Stat(fname, &stat));
  EXPECT_EQ(4, stat.length);
  EXPECT_FALSE(stat.is_directory);
}

}  // namespace
}  // namespace tensorflow
