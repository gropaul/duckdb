#include "FsstWrapper.hpp"
// -------------------------------------------------------------------------------------
using namespace std;
// -------------------------------------------------------------------------------------
static_assert(sizeof(uint64_t) == sizeof(size_t)); // FSST uses size_t everywhere.
// -------------------------------------------------------------------------------------
std::string FsstDecoder::SymbolToStr(unsigned code_index) const
{
   std::string ret;
   for (uint32_t jdx = 0; jdx < decoder->len[code_index]; jdx++) {
      ret += static_cast<char>(decoder->symbol[code_index] >> (jdx * 8));
   }
   return ret;
}
// -------------------------------------------------------------------------------------
void FsstDecoder::PrintSymbolTable(ostream& os) const
{
   for (uint32_t idx = 0; idx < symbol_table_size; idx++) {
      os << "idx: " << idx << ", len: " << static_cast<int>(decoder->len[idx]) << ", symbol: ";
      os << SymbolToStr(idx) << endl;
   }
}
// -------------------------------------------------------------------------------------
std::vector<std::string> FsstDecoder::ExtractFsstTable() const
{
   std::vector<std::string> fsst_symbols(GetSymbolTableSize());

   for (unsigned index = 0, limit = GetSymbolTableSize(); index != limit; ++index) {
      auto raw_symbol = GetSymbolTable().symbol[index];

      // Skip corrupt (invalid) symbols; they are placed at the end of the table.
      if (raw_symbol == FSST_CORRUPT) {
         continue;
      }

      fsst_symbols[index] = SymbolToStr(index);
   }

   return fsst_symbols;
}
// -------------------------------------------------------------------------------------
