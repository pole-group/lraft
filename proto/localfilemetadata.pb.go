// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.23.0
// 	protoc        v3.13.0
// source: localfilemetadata.proto

package proto

import (
	proto "github.com/golang/protobuf/proto"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// This is a compile-time assertion that a sufficiently up-to-date version
// of the legacy proto package is being used.
const _ = proto.ProtoPackageIsVersion4

type FileSource int32

const (
	FileSource_FILE_SOURCE_LOCAL     FileSource = 0
	FileSource_FILE_SOURCE_REFERENCE FileSource = 1
)

// Enum value maps for FileSource.
var (
	FileSource_name = map[int32]string{
		0: "FILE_SOURCE_LOCAL",
		1: "FILE_SOURCE_REFERENCE",
	}
	FileSource_value = map[string]int32{
		"FILE_SOURCE_LOCAL":     0,
		"FILE_SOURCE_REFERENCE": 1,
	}
)

func (x FileSource) Enum() *FileSource {
	p := new(FileSource)
	*p = x
	return p
}

func (x FileSource) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (FileSource) Descriptor() protoreflect.EnumDescriptor {
	return file_localfilemetadata_proto_enumTypes[0].Descriptor()
}

func (FileSource) Type() protoreflect.EnumType {
	return &file_localfilemetadata_proto_enumTypes[0]
}

func (x FileSource) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use FileSource.Descriptor instead.
func (FileSource) EnumDescriptor() ([]byte, []int) {
	return file_localfilemetadata_proto_rawDescGZIP(), []int{0}
}

type LocalFileMeta struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	UserMeta []byte     `protobuf:"bytes,1,opt,name=userMeta,proto3" json:"userMeta,omitempty"`
	Source   FileSource `protobuf:"varint,2,opt,name=source,proto3,enum=proto.FileSource" json:"source,omitempty"`
	Checksum string     `protobuf:"bytes,3,opt,name=checksum,proto3" json:"checksum,omitempty"`
}

func (x *LocalFileMeta) Reset() {
	*x = LocalFileMeta{}
	if protoimpl.UnsafeEnabled {
		mi := &file_localfilemetadata_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *LocalFileMeta) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LocalFileMeta) ProtoMessage() {}

func (x *LocalFileMeta) ProtoReflect() protoreflect.Message {
	mi := &file_localfilemetadata_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LocalFileMeta.ProtoReflect.Descriptor instead.
func (*LocalFileMeta) Descriptor() ([]byte, []int) {
	return file_localfilemetadata_proto_rawDescGZIP(), []int{0}
}

func (x *LocalFileMeta) GetUserMeta() []byte {
	if x != nil {
		return x.UserMeta
	}
	return nil
}

func (x *LocalFileMeta) GetSource() FileSource {
	if x != nil {
		return x.Source
	}
	return FileSource_FILE_SOURCE_LOCAL
}

func (x *LocalFileMeta) GetChecksum() string {
	if x != nil {
		return x.Checksum
	}
	return ""
}

var File_localfilemetadata_proto protoreflect.FileDescriptor

var file_localfilemetadata_proto_rawDesc = []byte{
	0x0a, 0x17, 0x6c, 0x6f, 0x63, 0x61, 0x6c, 0x66, 0x69, 0x6c, 0x65, 0x6d, 0x65, 0x74, 0x61, 0x64,
	0x61, 0x74, 0x61, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x05, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x22, 0x72, 0x0a, 0x0d, 0x4c, 0x6f, 0x63, 0x61, 0x6c, 0x46, 0x69, 0x6c, 0x65, 0x4d, 0x65, 0x74,
	0x61, 0x12, 0x1a, 0x0a, 0x08, 0x75, 0x73, 0x65, 0x72, 0x4d, 0x65, 0x74, 0x61, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x0c, 0x52, 0x08, 0x75, 0x73, 0x65, 0x72, 0x4d, 0x65, 0x74, 0x61, 0x12, 0x29, 0x0a,
	0x06, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x11, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x46, 0x69, 0x6c, 0x65, 0x53, 0x6f, 0x75, 0x72, 0x63, 0x65,
	0x52, 0x06, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x12, 0x1a, 0x0a, 0x08, 0x63, 0x68, 0x65, 0x63,
	0x6b, 0x73, 0x75, 0x6d, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x63, 0x68, 0x65, 0x63,
	0x6b, 0x73, 0x75, 0x6d, 0x2a, 0x3e, 0x0a, 0x0a, 0x46, 0x69, 0x6c, 0x65, 0x53, 0x6f, 0x75, 0x72,
	0x63, 0x65, 0x12, 0x15, 0x0a, 0x11, 0x46, 0x49, 0x4c, 0x45, 0x5f, 0x53, 0x4f, 0x55, 0x52, 0x43,
	0x45, 0x5f, 0x4c, 0x4f, 0x43, 0x41, 0x4c, 0x10, 0x00, 0x12, 0x19, 0x0a, 0x15, 0x46, 0x49, 0x4c,
	0x45, 0x5f, 0x53, 0x4f, 0x55, 0x52, 0x43, 0x45, 0x5f, 0x52, 0x45, 0x46, 0x45, 0x52, 0x45, 0x4e,
	0x43, 0x45, 0x10, 0x01, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_localfilemetadata_proto_rawDescOnce sync.Once
	file_localfilemetadata_proto_rawDescData = file_localfilemetadata_proto_rawDesc
)

func file_localfilemetadata_proto_rawDescGZIP() []byte {
	file_localfilemetadata_proto_rawDescOnce.Do(func() {
		file_localfilemetadata_proto_rawDescData = protoimpl.X.CompressGZIP(file_localfilemetadata_proto_rawDescData)
	})
	return file_localfilemetadata_proto_rawDescData
}

var file_localfilemetadata_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_localfilemetadata_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_localfilemetadata_proto_goTypes = []interface{}{
	(FileSource)(0),       // 0: proto.FileSource
	(*LocalFileMeta)(nil), // 1: proto.LocalFileMeta
}
var file_localfilemetadata_proto_depIdxs = []int32{
	0, // 0: proto.LocalFileMeta.source:type_name -> proto.FileSource
	1, // [1:1] is the sub-list for method output_type
	1, // [1:1] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_localfilemetadata_proto_init() }
func file_localfilemetadata_proto_init() {
	if File_localfilemetadata_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_localfilemetadata_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*LocalFileMeta); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_localfilemetadata_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_localfilemetadata_proto_goTypes,
		DependencyIndexes: file_localfilemetadata_proto_depIdxs,
		EnumInfos:         file_localfilemetadata_proto_enumTypes,
		MessageInfos:      file_localfilemetadata_proto_msgTypes,
	}.Build()
	File_localfilemetadata_proto = out.File
	file_localfilemetadata_proto_rawDesc = nil
	file_localfilemetadata_proto_goTypes = nil
	file_localfilemetadata_proto_depIdxs = nil
}
