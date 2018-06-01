// Copyright 2018 and onwards Noel Martin, Qwant Research.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Qwant Research nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#include "snappy.h"

#ifdef ENVIRONMENT64

namespace snappy {

size_t MaxCompressedLength64(size_t source_bytes) {
  int num_blocks = source_bytes/maxBytesPerBlock + (source_bytes%maxBytesPerBlock == 0 ? 0 : 1);
  return MaxCompressedLength(source_bytes) + num_blocks*sizeof(size_t);
}

void RawCompress64(const char* input,
                 size_t input_length,
                 char* compressed,
                 size_t* compressed_length) {

  uint64_t input_pos = 0;
  uint64_t compressed_pos = 0;

  int num_blocks = input_length/maxBytesPerBlock + (input_length%maxBytesPerBlock == 0 ? 0 : 1);
    
  std::memcpy (compressed + compressed_pos, &num_blocks, sizeof(num_blocks));
  compressed_pos += sizeof(num_blocks);

  size_t input_block_size;
  size_t compressed_block_size;

  for (int block_ref = 0; block_ref < num_blocks; block_ref++) {
    input_block_size = std::min(maxBytesPerBlock, input_length - input_pos);

    RawCompress (input + input_pos, input_block_size, 
                 compressed + compressed_pos + sizeof(size_t), &compressed_block_size);

    std::memcpy (compressed + compressed_pos, &compressed_block_size, sizeof(size_t));

    input_pos += input_block_size;
    compressed_pos += compressed_block_size + sizeof(size_t);

    if (compressed_pos > *compressed_length) {
      throw std::runtime_error ("Snappy RawCompress64 failed, not enought memory.");
    }
  }

  *compressed_length = compressed_pos;
}

bool RawUncompress64(const char* compressed, size_t compressed_length,
                     char* uncompressed) {
  uint64_t compressed_pos = 0;
  uint64_t uncompressed_pos = 0;

  int num_blocks;

  std::memcpy (&num_blocks, compressed, sizeof(num_blocks));
  compressed_pos += sizeof(num_blocks);

  size_t compressed_block_size;
  size_t uncompressed_block_size;

  bool uncompress_succeed = false;

  for (int block_ref = 0; block_ref < num_blocks; block_ref++) {
    std::memcpy (&compressed_block_size, compressed + compressed_pos, sizeof(size_t));
    compressed_pos += sizeof(size_t);

    GetUncompressedLength (compressed + compressed_pos, compressed_block_size,
                           &uncompressed_block_size);

    uncompress_succeed = RawUncompress (compressed + compressed_pos, compressed_block_size, 
                                        uncompressed + uncompressed_pos);
    if (!uncompress_succeed) {
      return uncompress_succeed;
    }

    compressed_pos += compressed_block_size;
    uncompressed_pos += uncompressed_block_size;
  }

  return uncompress_succeed;
}

} // namespace snappy

#endif // ENVIRONMENT64
