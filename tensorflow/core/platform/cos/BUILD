# Description:
# Cos support for TensorFlow.

package(default_visibility = ["//visibility:public"])

licenses(["notice"])  # Apache 2.0

exports_files(["LICENSE"])

load(
    "//tensorflow:tensorflow.bzl",
    "tf_cc_binary",
    "tf_cc_test",
    "tf_custom_op_library"
)

#--------------------------------------------------------------------
cc_import(
    name = "libcossdk",
    hdrs = glob([
        "include/**/*.h",
        "include/*.h",
    ]),
    static_library = "linux/libcossdk.a",
    alwayslink =1,
)

cc_import(
    name = "libPocoNetSSL",
    static_library = "linux/libPocoNetSSL.a",
    alwayslink =1,
)

cc_import(
    name = "libPocoXML",
    static_library = "linux/libPocoXML.a",
    alwayslink =1,
)

cc_import(
    name = "libboost_thread",
    static_library = "linux/libboost_thread.a",
    alwayslink =1,
)

cc_import(
    name = "libjsoncpp",
    static_library = "linux/libjsoncpp.a",
    alwayslink =1,
)

cc_import(
    name = "libssl",
    static_library = "linux/libssl.a",
    alwayslink =1,
)

cc_import(
    name = "libPocoEncodings",
    static_library = "linux/libPocoEncodings.a",
    alwayslink =1,
)

cc_import(
    name = "libPocoJSON",
    static_library = "linux/libPocoJSON.a",
    alwayslink =1,
)

cc_import(
    name = "libPocoCrypto",
    static_library = "linux/libPocoCrypto.a",
    alwayslink =1,
)

cc_import(
    name = "libPocoFoundation",
    static_library = "linux/libPocoFoundation.a",
    alwayslink =1,
)

cc_import(
    name = "libPocoNet",
    static_library = "linux/libPocoNet.a",
    alwayslink =1,
)

cc_import(
    name = "libPocoUtil",
    static_library = "linux/libPocoUtil.a",
    alwayslink =1,
)

cc_import(
    name = "libboost_system",
    static_library = "linux/libboost_system.a",
    alwayslink =1,
)

cc_import(
    name = "libcrypto",
    static_library = "linux/libcrypto.a",
    alwayslink =1,
)

#--------------------------------------------------------------------
cc_import(
    name = "libcossdk-darwin",
    hdrs = glob([
        "include/**/*.h",
        "include/*.h",
    ]),
    static_library = "darwin/libcossdk.a",
)

cc_import(
    name = "libPocoNetSSL-darwin",
    static_library = "darwin/libPocoNetSSL.a",
)

cc_import(
    name = "libPocoXML-darwin",
    static_library = "darwin/libPocoXML.a",
)

cc_import(
    name = "libboost_thread-darwin",
    static_library = "darwin/libboost_thread.a",
)

cc_import(
    name = "libjsoncpp-darwin",
    static_library = "darwin/libjsoncpp.a",
)

cc_import(
    name = "libssl-darwin",
    static_library = "darwin/libssl.a",
)

cc_import(
    name = "libPocoEncodings-darwin",
    static_library = "darwin/libPocoEncodings.a",
)

cc_import(
    name = "libPocoJSON-darwin",
    static_library = "darwin/libPocoJSON.a",
)

cc_import(
    name = "libPocoCrypto-darwin",
    static_library = "darwin/libPocoCrypto.a",
)

cc_import(
    name = "libPocoFoundation-darwin",
    static_library = "darwin/libPocoFoundation-test.a",
)

cc_import(
    name = "libPocoNet-darwin",
    static_library = "darwin/libPocoNet.a",
)

cc_import(
    name = "libPocoUtil-darwin",
    static_library = "darwin/libPocoUtil.a",
)

cc_import(
    name = "libboost_system-darwin",
    static_library = "darwin/libboost_system.a",
)

cc_import(
    name = "libcrypto-darwin",
    static_library = "darwin/libcrypto.a",
)

#--------------------------------------------------------------------


filegroup(
    name = "all_files",
    srcs = glob(
        ["**/*"],
        exclude = [
            "**/METADATA",
            "**/OWNERS",
        ],
    ),
    visibility = ["//tensorflow:__subpackages__"],
)


cc_library(
    name = "cos_lib",
    includes = ["include"],
    deps = [
        ":libcossdk",
        ":libPocoCrypto",
        ":libPocoFoundation",
        ":libPocoNet",
        ":libPocoUtil",
        ":libboost_system",
        ":libcrypto",
        ":libssl",
        #"@boringssl//:crypto",
        #"@boringssl//:ssl",
        ":libPocoEncodings",
        ":libPocoJSON",
        ":libPocoNetSSL",
        ":libPocoXML",
        ":libboost_thread",
        "@jsoncpp_git//:jsoncpp",
    ],
    linkstatic = 1,
    alwayslink = 1,
)

cc_library(
    name = "cos_lib-darwin",
    deps = [
        ":libcossdk-darwin",
        ":libPocoCrypto-darwin",
        ":libPocoFoundation-darwin",
        ":libPocoNet-darwin",
        ":libPocoUtil-darwin",
        ":libboost_system-darwin",
        ":libcrypto-darwin",
        ":libssl-darwin",
        ":libPocoEncodings-darwin",
        ":libPocoJSON-darwin",
        ":libPocoNetSSL-darwin",
        ":libPocoXML-darwin",
        ":libboost_thread-darwin",
        ":libjsoncpp-darwin",
    ],
    linkstatic = 1,
    alwayslink = 1,
)


cc_library(
    name = "cos_file_system",
    srcs = [
        "cos_file_system.h",
        "cos_file_system.cc",
    ],
    hdrs = [
        "cos_file_system.h",
    ],
    includes = ["include"],
    deps = [
        "//tensorflow/core:lib",
       "@protobuf_archive//:protobuf_headers",
    ] + select({
        "//tensorflow:darwin": [":cos_lib-darwin"],
        "//conditions:default": [":cos_lib"],
    }),
    linkstatic = 1,
    alwayslink = 1,
)

tf_custom_op_library(
    name = "cos_file_system.so",
    srcs = [
     "cos_file_system.h",
     "cos_file_system.cc",
     ],
    linkopts=select({
        "//conditions:default": [
            "-lpthread",
            "-lrt",
            "-lm"
        ]
    }),
    deps = [
       ":cos_lib",
      ] ,
)


tf_cc_test(
    name = "cos_file_system_test",
    size = "small",
    srcs = [
        "cos_file_system_test.cc",
    ],
    tags = [
        "manual",
    ],
    deps = [
        ":cos_file_system",
        "//tensorflow/core:lib",
        "//tensorflow/core:lib_internal",
        "//tensorflow/core:test",
        "//tensorflow/core:test_main",
	],
)


genrule(
    local = 1,
    name = "cos_dep_linux",
    srcs = [
      "cos-dep-linux"
    ],
    outs = [
      "linux"
    ],
    cmd = """
        LIB_PATH=$$(dirname $(location cos-dep-linux))
        wget --quiet 'http://data-sg-1255502019.cossgp.myqcloud.com/tool/cos-dep-linux.tar.gz?sign=+87zU0ancx5M4xXWT0kQNw9+Bb1hPTEyNTU1MDIwMTkmaz1BS0lEbU9tdnltd3E5RzlwR1NCQWcyN2U4WXMwdFhramlDY20mZT0xNTMwOTQ1NDc4JnQ9MTUyODM1MzQ3OCZyPTEzNzc4MjM3NzMmZj0vdG9vbC9jb3MtZGVwLWxpbnV4LnRhci5neiZiPWRhdGEtc2c=' -O dep.tar.gz
        tar zxvf dep.tar.gz
        echo $$LIB_PATH
        echo `pwd`
        cp -r linux $$LIB_PATH
        cp -r linux $(location linux)
    """,
)
