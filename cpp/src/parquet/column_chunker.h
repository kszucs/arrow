// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <utility>

#include <cmath>
#include <fstream>
#include <iostream>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include "arrow/array.h"
#include "arrow/type_traits.h"

// Constants
const uint64_t GEAR_HASH_TABLE[] = {
    0xb088d3a9e840f559, 0x5652c7f739ed20d6, 0x45b28969898972ab, 0x6b0a89d5b68ec777,
    0x368f573e8b7a31b7, 0x1dc636dce936d94b, 0x207a4c4e5554d5b6, 0xa474b34628239acb,
    0x3b06a83e1ca3b912, 0x90e78d6c2f02baf7, 0xe1c92df7150d9a8a, 0x8e95053a1086d3ad,
    0x5a2ef4f1b83a0722, 0xa50fac949f807fae, 0x0e7303eb80d8d681, 0x99b07edc1570ad0f,
    0x689d2fb555fd3076, 0x00005082119ea468, 0xc4b08306a88fcc28, 0x3eb0678af6374afd,
    0xf19f87ab86ad7436, 0xf2129fbfbe6bc736, 0x481149575c98a4ed, 0x0000010695477bc5,
    0x1fba37801a9ceacc, 0x3bf06fd663a49b6d, 0x99687e9782e3874b, 0x79a10673aa50d8e3,
    0xe4accf9e6211f420, 0x2520e71f87579071, 0x2bd5d3fd781a8a9b, 0x00de4dcddd11c873,
    0xeaa9311c5a87392f, 0xdb748eb617bc40ff, 0xaf579a8df620bf6f, 0x86a6e5da1b09c2b1,
    0xcc2fc30ac322a12e, 0x355e2afec1f74267, 0x2d99c8f4c021a47b, 0xbade4b4a9404cfc3,
    0xf7b518721d707d69, 0x3286b6587bf32c20, 0x0000b68886af270c, 0xa115d6e4db8a9079,
    0x484f7e9c97b2e199, 0xccca7bb75713e301, 0xbf2584a62bb0f160, 0xade7e813625dbcc8,
    0x000070940d87955a, 0x8ae69108139e626f, 0xbd776ad72fde38a2, 0xfb6b001fc2fcc0cf,
    0xc7a474b8e67bc427, 0xbaf6f11610eb5d58, 0x09cb1f5b6de770d1, 0xb0b219e6977d4c47,
    0x00ccbc386ea7ad4a, 0xcc849d0adf973f01, 0x73a3ef7d016af770, 0xc807d2d386bdbdfe,
    0x7f2ac9966c791730, 0xd037a86bc6c504da, 0xf3f17c661eaa609d, 0xaca626b04daae687,
    0x755a99374f4a5b07, 0x90837ee65b2caede, 0x6ee8ad93fd560785, 0x0000d9e11053edd8,
    0x9e063bb2d21cdbd7, 0x07ab77f12a01d2b2, 0xec550255e6641b44, 0x78fb94a8449c14c6,
    0xc7510e1bc6c0f5f5, 0x0000320b36e4cae3, 0x827c33262c8b1a2d, 0x14675f0b48ea4144,
    0x267bd3a6498deceb, 0xf1916ff982f5035e, 0x86221b7ff434fb88, 0x9dbecee7386f49d8,
    0xea58f8cac80f8f4a, 0x008d198692fc64d8, 0x6d38704fbabf9a36, 0xe032cb07d1e7be4c,
    0x228d21f6ad450890, 0x635cb1bfc02589a5, 0x4620a1739ca2ce71, 0xa7e7dfe3aae5fb58,
    0x0c10ca932b3c0deb, 0x2727fee884afed7b, 0xa2df1c6df9e2ab1f, 0x4dcdd1ac0774f523,
    0x000070ffad33e24e, 0xa2ace87bc5977816, 0x9892275ab4286049, 0xc2861181ddf18959,
    0xbb9972a042483e19, 0xef70cd3766513078, 0x00000513abfc9864, 0xc058b61858c94083,
    0x09e850859725e0de, 0x9197fb3bf83e7d94, 0x7e1e626d12b64bce, 0x520c54507f7b57d1,
    0xbee1797174e22416, 0x6fd9ac3222e95587, 0x0023957c9adfbf3e, 0xa01c7d7e234bbe15,
    0xaba2c758b8a38cbb, 0x0d1fa0ceec3e2b30, 0x0bb6a58b7e60b991, 0x4333dd5b9fa26635,
    0xc2fd3b7d4001c1a3, 0xfb41802454731127, 0x65a56185a50d18cb, 0xf67a02bd8784b54f,
    0x696f11dd67e65063, 0x00002022fca814ab, 0x8cd6be912db9d852, 0x695189b6e9ae8a57,
    0xee9453b50ada0c28, 0xd8fc5ea91a78845e, 0xab86bf191a4aa767, 0x0000c6b5c86415e5,
    0x267310178e08a22e, 0xed2d101b078bca25, 0x3b41ed84b226a8fb, 0x13e622120f28dc06,
    0xa315f5ebfb706d26, 0x8816c34e3301bace, 0xe9395b9cbb71fdae, 0x002ce9202e721648,
    0x4283db1d2bb3c91c, 0xd77d461ad2b1a6a5, 0xe2ec17e46eeb866b, 0xb8e0be4039fbc47c,
    0xdea160c4d5299d04, 0x7eec86c8d28c3634, 0x2119ad129f98a399, 0xa6ccf46b61a283ef,
    0x2c52cedef658c617, 0x2db4871169acdd83, 0x0000f0d6f39ecbe9, 0x3dd5d8c98d2f9489,
    0x8a1872a22b01f584, 0xf282a4c40e7b3cf2, 0x8020ec2ccb1ba196, 0x6693b6e09e59e313,
    0x0000ce19cc7c83eb, 0x20cb5735f6479c3b, 0x762ebf3759d75a5b, 0x207bfe823d693975,
    0xd77dc112339cd9d5, 0x9ba7834284627d03, 0x217dc513e95f51e9, 0xb27b1a29fc5e7816,
    0x00d5cd9831bb662d, 0x71e39b806d75734c, 0x7e572af006fb1a23, 0xa2734f2f6ae91f85,
    0xbf82c6b5022cddf2, 0x5c3beac60761a0de, 0xcdc893bb47416998, 0x6d1085615c187e01,
    0x77f8ae30ac277c5d, 0x917c6b81122a2c91, 0x5b75b699add16967, 0x0000cf6ae79a069b,
    0xf3c40afa60de1104, 0x2063127aa59167c3, 0x621de62269d1894d, 0xd188ac1de62b4726,
    0x107036e2154b673c, 0x0000b85f28553a1d, 0xf2ef4e4c18236f3d, 0xd9d6de6611b9f602,
    0xa1fc7955fb47911c, 0xeb85fd032f298dbd, 0xbe27502fb3befae1, 0xe3034251c4cd661e,
    0x441364d354071836, 0x0082b36c75f2983e, 0xb145910316fa66f0, 0x021c069c9847caf7,
    0x2910dfc75a4b5221, 0x735b353e1c57a8b5, 0xce44312ce98ed96c, 0xbc942e4506bdfa65,
    0xf05086a71257941b, 0xfec3b215d351cead, 0x00ae1055e0144202, 0xf54b40846f42e454,
    0x00007fd9c8bcbcc8, 0xbfbd9ef317de9bfe, 0xa804302ff2854e12, 0x39ce4957a5e5d8d4,
    0xffb9e2a45637ba84, 0x55b9ad1d9ea0818b, 0x00008acbf319178a, 0x48e2bfc8d0fbfb38,
    0x8be39841e848b5e8, 0x0e2712160696a08b, 0xd51096e84b44242a, 0x1101ba176792e13a,
    0xc22e770f4531689d, 0x1689eff272bbc56c, 0x00a92a197f5650ec, 0xbc765990bda1784e,
    0xc61441e392fcb8ae, 0x07e13a2ced31e4a0, 0x92cbe984234e9d4d, 0x8f4ff572bb7d8ac5,
    0x0b9670c00b963bd0, 0x62955a581a03eb01, 0x645f83e5ea000254, 0x41fce516cd88f299,
    0xbbda9748da7a98cf, 0x0000aab2fe4845fa, 0x19761b069bf56555, 0x8b8f5e8343b6ad56,
    0x3e5d1cfd144821d9, 0xec5c1e2ca2b0cd8f, 0xfaf7e0fea7fbb57f, 0x000000d3ba12961b,
    0xda3f90178401b18e, 0x70ff906de33a5feb, 0x0527d5a7c06970e7, 0x22d8e773607c13e9,
    0xc9ab70df643c3bac, 0xeda4c6dc8abe12e3, 0xecef1f410033e78a, 0x0024c2b274ac72cb,
    0x06740d954fa900b4, 0x1d7a299b323d6304, 0xb3c37cb298cbead5, 0xc986e3c76178739b,
    0x9fabea364b46f58a, 0x6da214c5af85cc56, 0x17a43ed8b7a38f84, 0x6eccec511d9adbeb,
    0xf9cab30913335afb, 0x4a5e60c5f415eed2, 0x00006967503672b4, 0x9da51d121454bb87,
    0x84321e13b9bbc816, 0xfb3d6fb6ab2fdd8d, 0x60305eed8e160a8d, 0xcbbf4b14e9946ce8,
    0x00004f63381b10c3, 0x07d5b7816fcc4e10, 0xe5a536726a6a8155, 0x57afb23447a07fdd,
    0x18f346f7abc9d394, 0x636dc655d61ad33d, 0xcc8bab4939f7f3f6, 0x63c7a906c1dd187b};
// const uint64_t MASK = 0xffff00000000000;
const uint64_t MASK = 0xffff00000000000;
// const uint64_t MASK = 0xfff0000000000000;
// const int MIN_LEN = 65536 / 8;
// const int MAX_LEN = 65536 * 2;

const int MIN_LEN = 512 * 1024;
const int MAX_LEN = 2 * 1024 * 1024;

class GearHash {
 public:
  GearHash() : hash_(0), mask_(MASK) {}

  void Reset() {
    hash_ = 0;
    mask_ = MASK;
    chunk_size_ = 0;
  }

  // int64_t GetHash() const {
  //     return hash_;
  // }

  // void SetHash(int64_t hash) {
  //     hash_ = hash;
  // }

  // bool Update(const )

  // template <typename T>
  // bool Update(const ::arrow::NumericArray<T>& array, uint64_t index) {
  //     using byte_width = typename ::arrow::TypeTraits<typename
  //     T::TypeClass>::bytes_required();
  //     // get the byte represetnation of the value
  //     auto buf = reinterpret_cast<const uint8_t*>(array.Value(index));
  //     return Update<byte_width>(buf);
  // }

  bool IsBoundary(int16_t def, int16_t rep, std::string str) {
    bool match = false;
    // update with both bytes of the def
    hash_ = (hash_ << 1) + GEAR_HASH_TABLE[def & 0xff];
    match |= (hash_ & mask_) == 0;
    hash_ = (hash_ << 1) + GEAR_HASH_TABLE[(def >> 8) & 0xff];
    match |= (hash_ & mask_) == 0;
    // update with both bytes of the rep
    hash_ = (hash_ << 1) + GEAR_HASH_TABLE[rep & 0xff];
    match |= (hash_ & mask_) == 0;
    hash_ = (hash_ << 1) + GEAR_HASH_TABLE[(rep >> 8) & 0xff];
    match |= (hash_ & mask_) == 0;
    chunk_size_ += 4;
    match |= Update(reinterpret_cast<const uint8_t*>(str.c_str()), str.size());
    // return match;
    if ((match && (chunk_size_ >= MIN_LEN)) || (chunk_size_ >= MAX_LEN)) {
      // std::cout << "chunk size: " << chunk_size_ << std::endl;
      // std::cout << "hash: " << hash_ << std::endl;
      chunk_size_ = 0;
      return true;
    } else {
      return false;
    }
  }

  // template <size_t N>
  bool inline Update(const uint8_t* buf, size_t N) {
    bool match = false;
    for (size_t i = 0; i < N; ++i) {
      hash_ = (hash_ << 1) + GEAR_HASH_TABLE[buf[i]];

      if ((hash_ & mask_) == 0) {
        match = true;
      }
    }
    chunk_size_ += N;
    return match;
  }

 private:
  uint64_t hash_;
  uint64_t mask_;
  uint64_t chunk_size_ = 0;
};
