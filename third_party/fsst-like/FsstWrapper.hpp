#pragma once
// -------------------------------------------------------------------------------------
// Lightweight non-owning wrapper around DuckDB's duckdb_fsst_decoder_t.
// DuckDB manages the decoder's lifetime and tracks the symbol table size.
// -------------------------------------------------------------------------------------
#include <cassert>
#include <iostream>
#include <string>
#include <vector>
#include "Utility.hpp"
#include "fsst.h"
// -------------------------------------------------------------------------------------
#define FSST_CORRUPT 32774747032022883 /* 7-byte number in little endian containing "corrupt" */
// -------------------------------------------------------------------------------------
class FsstDecoder {
public:
   FsstDecoder(const duckdb_fsst_decoder_t* decoder, uint32_t symbol_table_size)
       : decoder(decoder), symbol_table_size(symbol_table_size) {}

   uint32_t GetSymbolTableSize() const { return symbol_table_size; }
   const duckdb_fsst_decoder_t& GetSymbolTable() const { return *decoder; }

   // How much memory is required so that FSST can fast decompress into it.
   uint32_t GetIdealBufferSize(uint32_t compressed_size) const { return compressed_size * 8 + 32; }

   std::string SymbolToStr(unsigned code_index) const;
   void PrintSymbolTable(std::ostream& os) const;

   // Used in the state machines.
   std::vector<std::string> ExtractFsstTable() const;

   // Iterate a FSST-encoded string, dispatching each decoded byte or symbol code
   // to the provided callbacks. Returns true when the string is exhausted without
   // either callback signalling completion (returning false).
   template<typename ConsumeCode, typename ConsumeChar>
   inline bool Iterate(
      size_t lenIn,
      const unsigned char *strIn,
      size_t size,
      ConsumeChar&& consume_char,
      ConsumeCode&& consume_code
   ) const {
      unsigned char*__restrict__ len = (unsigned char* __restrict__) decoder->len;
      unsigned long long*__restrict__ symbol = (unsigned long long* __restrict__) decoder->symbol;
      size_t code, posOut = 0, posIn = 0;

#ifndef FSST_MUST_ALIGN
#define FSST_UNALIGNED_STORE(dst,src) memcpy((unsigned long long*) (dst), &(src), sizeof(unsigned long long))
#if defined(__BYTE_ORDER__) && defined(__ORDER_LITTLE_ENDIAN__) && (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)

      while (posOut+32 <= size && posIn+4 <= lenIn) {
         unsigned int nextBlock, escapeMask;
         memcpy(&nextBlock, strIn+posIn, sizeof(unsigned int));
         escapeMask = (nextBlock&0x80808080u)&((((~nextBlock)&0x7F7F7F7Fu)+0x7F7F7F7Fu)^0x80808080u);
         if (escapeMask == 0) {
            code = strIn[posIn++]; if (!consume_code(code)) return true; posOut += len[code];
            code = strIn[posIn++]; if (!consume_code(code)) return true; posOut += len[code];
            code = strIn[posIn++]; if (!consume_code(code)) return true; posOut += len[code];
            code = strIn[posIn++]; if (!consume_code(code)) return true; posOut += len[code];
         } else {
            unsigned long firstEscapePos=__builtin_ctzl((unsigned long long) escapeMask)>>3;
            switch(firstEscapePos) { /* Duff's device */
            case 3: code = strIn[posIn++]; if (!consume_code(code)) return true; posOut += len[code];
                  // fall through
            case 2: code = strIn[posIn++]; if (!consume_code(code)) return true; posOut += len[code];
                  // fall through
            case 1: code = strIn[posIn++]; if (!consume_code(code)) return true; posOut += len[code];
                  // fall through
            case 0: posIn+=2; if (!consume_char(strIn[posIn - 1])) return true; posOut++;
            }
         }
      }
      if (posOut+32 <= size) {
         if (posIn+2 <= lenIn) {
            if (strIn[posIn] != FSST_ESC) {
               code = strIn[posIn++]; if (!consume_code(code)) return true; posOut += len[code];
               if (strIn[posIn] != FSST_ESC) {
                  code = strIn[posIn++]; if (!consume_code(code)) return true; posOut += len[code];
               } else {
                  posIn += 2; if (!consume_char(strIn[posIn - 1])) return true; posOut++;
               }
            } else {
               if (!consume_char(strIn[posIn + 1])) return true;
               posIn += 2; posOut++;
            }
         }
         if (posIn < lenIn) {
            code = strIn[posIn++]; if (!consume_code(code)) return true; posOut += len[code];
         }
      }
#else
      while (posOut+8 <= size && posIn < lenIn)
         if ((code = strIn[posIn++]) < FSST_ESC) {
            FSST_UNALIGNED_STORE(nullptr, symbol[code]); // suppress unused warning
            posOut += len[code];
            if (!consume_code(code)) return true;
         } else {
            if (!consume_char(strIn[posIn])) return true;
            posIn++; posOut++;
         }
#endif
#endif
      while (posIn < lenIn)
         if ((code = strIn[posIn++]) < FSST_ESC) {
            size_t posWrite = posOut, endWrite = posOut + len[code];
            unsigned char* __restrict__ symbolPointer = ((unsigned char* __restrict__) &symbol[code]) - posWrite;
            if ((posOut = endWrite) > size) endWrite = size;
            for(; posWrite < endWrite; posWrite++) {
               if (!consume_char(symbolPointer[posWrite])) return true;
            }
         } else {
            if (posOut < size) {
               if (!consume_char(strIn[posIn])) return true;
            }
            posIn++; posOut++;
         }
      if (posOut >= size && (decoder->zeroTerminated&1)) {
         assert(0);
      }

      return false;
   }

private:
   const duckdb_fsst_decoder_t* decoder;
   uint32_t symbol_table_size;
};
// -------------------------------------------------------------------------------------
