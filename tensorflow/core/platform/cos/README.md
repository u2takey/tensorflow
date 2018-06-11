# 1. 编译

linux的编译都在 ubuntu:16.04 运行

```
apt-get update
apt-get install -y wget cmake g++ vim openssl libssl-dev git python
```
装tensorflow用来验证 

```
pip install tensorflow==1.6.0
TF_LIB=$(python -c 'import tensorflow as tf; print(tf.sysconfig.get_lib())')
```

注意
1. 所有.a 编译都要 -fPIC 或者 set(CMAKE_POSITION_INDEPENDENT_CODE ON)
2. 容器里面的tensorflow和pip官方地址的tensorflow包编译方式不同，目前采用pip官方地址的tensorflow包, 所以使用的时候容器里面要先uninstall, 再install, 本地包的编译都增加 cxxflags="-D_GLIBCXX_USE_CXX11_ABI=0" (1.8.0 不用，验证方式nm -A /usr/local/lib/python2.7/dist-packages/tensorflow/*.so | grep ZN10tensorflow10FileSystem10File)

## 1.1 编译boost

```
wget https://dl.bintray.com/boostorg/release/1.66.0/source/boost_1_66_0.tar.gz
tar zxvf boost_1_66_0.tar.gz
cd boost_1_66_0
./bootstrap.sh
./bjam cxxflags=-fPIC cxxflags="-D_GLIBCXX_USE_CXX11_ABI=0"  cflags=-fPIC -a link=static -j14

# 安装位置
/root/boost_1_66_0
/root/boost_1_66_0/stage/lib
```

## 1.2 编译poco
```
wget https://pocoproject.org/releases/poco-1.9.0/poco-1.9.0-all.tar.gz
tar zxvf poco-1.9.0-all.tar.gz
cd poco-1.9.0-all
// cmakelist 改static编译, 增加 D_GLIBCXX_USE_CXX11_ABI 参数
mkdir cmake_build
cd cmake_build
cmake ..
make
```

## 1.3 编译openssl

tensorflow默认使用boringssl, 但是由于和poco需要的ssl不兼容，所以需要增加openssl依赖
```
系统的ssl库可能没有fPIC，需要重新编译
./config no-shared CFLAGS=-fPIC CXXFLAGS=-fPIC CXXFLAGS="-D_GLIBCXX_USE_CXX11_ABI=0"  
```

## 1.4 编译cos-cpp-sdk-v5

```
git clone https://github.com/tencentyun/cos-cpp-sdk-v5.git
export bootpath=/data/src/github.com/boost_1_66_0/stage/lib
export pocopath=/data/src/github.com/poco-1.9.0-all/cmake_build/lib
cp $bootpath/libboost_system.a lib/
cp $bootpath/libboost_thread.a lib/
cp /usr/lib/x86_64-linux-gnu/libssl.a lib/
cp /usr/lib/x86_64-linux-gnu/libcrypto.a lib/
cp $pocopath/libPocoCrypto.a $pocopath/libPocoEncodings.a $pocopath/libPocoFoundation.a $pocopath/libPocoJSON.a $pocopath/libPocoNet.a $pocopath/libPocoNetSSL.a  $pocopath/libPocoUtil.a $pocopath/libPocoXML.a lib/

# cmakelist 
SET(BOOST_HEADER_DIR "/root/boost_1_66_0")
INCLUDE_DIRECTORIES(/root/boost_1_66_0)

INCLUDE_DIRECTORIES(/root/poco-1.9.0-all/Foundation/include)
INCLUDE_DIRECTORIES(/root/poco-1.9.0-all/Net/include)
INCLUDE_DIRECTORIES(/root/poco-1.9.0-all/NetSSL_OpenSSL/include)
INCLUDE_DIRECTORIES(/root/poco-1.9.0-all/Crypto/include)
INCLUDE_DIRECTORIES(/root/poco-1.9.0-all/Util/include)

vim /src/CMakelist
target_link_libraries(... dl)

mkdir -p build
cd build
cmake ..
make

```


## 1.5 cos_file_system.so

```
./configure
bazel build //tensorflow/core/platform/cos:cos_dep_linux --verbose_failures
bazel build -c opt --copt="-fPIC" --cxxopt="-D_GLIBCXX_USE_CXX11_ABI=0" //tensorflow/core/platform/cos:cos_file_system.so --verbose_failures

如果有报错
ar -dv libPocoFoundation-test.a adler32.c.o compress.c.o crc32.c.o deflate.c.o infback.c.o   inffast.c.o inflate.c.o inftrees.c.o trees.c.o zutil.c.o

```

## 1.6 编译可能问题
https://github.com/tensorflow/tensorflow/pull/17508/files

## 2. 测试

### 2.1 测试cos_file_system_test
```
测试cos_file_system_test
bazel build -c opt --cxxopt="-D_GLIBCXX_USE_CXX11_ABI=0"  //tensorflow/core/platform/cos:cos_file_system_test --verbose_failures

export COS_TEST_TMPDIR="cos://test-1255502019/test2"
export COS_REGION=ap-shanghai
export COS_ACCESS_KEY=xx
export COS_SECRET_KEY=yy
#export COS_DEBUG=true
bazel-bin/tensorflow/core/platform/cos/cos_file_system_test
```

### 2.2 在ipython中测试file_io

```
直接在ipython 中测试
import os
from tensorflow.python.framework import load_library
load_library.load_file_system_library("cos_file_system.so")
from tensorflow.python.lib.io import file_io
os.environ["COS_REGION"] = "ap-shanghai"
os.environ["COS_ACCESS_KEY"] = "x"
os.environ["COS_SECRET_KEY"] = "y"
#os.environ["COS_DEBUG"] = "true"


file_io.is_directory("cos://test-1255502019/estimator/trainer/model.py")
file_io.stat("cos://test-1255502019/estimator/trainer/model.py")
file_io.read_file_to_string("cos://test-1255502019/estimator/trainer/model.py")
file_io.atomic_write_string_to_file("cos://test-1255502019/estimator/test/1","cos://test-1255502019/estimator/test/1")
file_io.delete_recursively('cos://test-1255502019/estimator1')

export COS_REGION="ap-shanghai"
export COS_ACCESS_KEY="x"
export COS_SECRET_KEY="y"
export COS_TEST_TMPDIR="cos://test-1255502019/test2"
```

### 2.3 在训练脚本中测试

```
export MODEL_DIR="cos://test-1255502019/estimator-cos/output6"
export COS_REGION="ap-shanghai"
export COS_ACCESS_KEY="x"
export COS_SECRET_KEY="y"
export TRAIN_DATA=`pwd`/data/adult.data.csv
export EVAL_DATA=`pwd`/data/adult.test.csv
time python -m trainer.task --job-dir $MODEL_DIR --train-files $TRAIN_DATA --eval-files $EVAL_DATA --train-steps 1000 --eval-steps 100#


# 修改estimator task.py
from tensorflow.python.framework import load_library
load_library.load_file_system_library("/private/var/tmp/_bazel_leiwang/fcd2755e74e086048f6d1d7d6303955c/execroot/org_tensorflow/bazel-out/darwin-opt/bin/tensorflow/core/platform/cos/cos_file_system.so")


# 对比cosfs
time docker run --rm -i -t \
-e APPID="1255502019" \
-e BUCKET="test" \
-e LOCAL="/data" \
-e REMOTE="http://cos.ap-shanghai.myqcloud.com" \
-e DEBUGLEVEL="info" \
-e SECRETID="x" \
-e SECRETKEY="y" \
-e MODEL_DIR="/data/estimator-cos/output7" \
-e COS_REGION="ap-shanghai" \
-e COS_ACCESS_KEY="x" \
-e COS_SECRET_KEY="y" \
-e TRAIN_DATA=/estimator/data/adult.data.csv \
-e EVAL_DATA=/estimator/data/adult.test.csv \
-v `pwd`:"/estimator" \
-v "/private/var/tmp/":"/private/var/tmp/" \
-w "/estimator" \
--privileged \
ccr.ccs.tencentyun.com/leiiwang/tensorflow-with-cosfs:1.5.0 python -m trainer.task --job-dir /data/estimator-cos/output7 --train-files /estimator/data/adult.data.csv --eval-files /estimator/data/adult.test.csv --train-steps 1000 --eval-steps 100



# so
23.78s user 3.42s system 33% cpu 1:20.59 total
24.09s user 3.42s system 30% cpu 1:31.23 tota

# cosfs
0.03s user 0.02s system 0% cpu 3:24.59 total
0.03s user 0.03s system 0% cpu 2:57.80 total
0.04s user 0.07s system 0% cpu 5:05.02 total
```


