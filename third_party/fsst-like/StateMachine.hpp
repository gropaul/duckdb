#pragma once
// -------------------------------------------------------------------------------------
#include <string>
#include "FsstWrapper.hpp"

class MetaStateMachine;

template <int IGNORED>
class StateMachineImpl {
public:
   StateMachineImpl(std::string_view pattern)
       : P(pattern), m(pattern.size())
   {
      build_pi();
   }

   StateMachineImpl(const std::string& pattern)
       : P(pattern), m(pattern.size())
   {
      build_pi();
   }

   bool has_zerokmp_property() const
   {
      for (unsigned index = 0; index != m; ++index) {
         if (pi[index])
            return false;
      }
      return true;
   }

   void init(const FsstDecoder& fsstDecoder)
   {
      // Take the FSST symbols.
      fsst_symbols = fsstDecoder.ExtractFsstTable();

      // Init the fsst_size.
      fsst_size = fsst_symbols.size();
   }

   void precompute()
   {
      // Init the lookup table.
      lookup_table.resize(P.size() * fsst_size);

      for (unsigned index = 0; index != m; ++index) {
         for (unsigned code = 0; code != fsst_size; ++code) {
            // Init the state.
            init_state(index);

            // The default stop-position is the length of the string.
            unsigned stop_pos = fsst_symbols[code].size();
            accept_symbol(code, &stop_pos);
            assert(stop_pos <= fsst_symbols[code].size());

            // Save the state we arrived at.
            lookup_table[index * fsst_size + code] = {curr_state, stop_pos};
         }
      }
   }

   unsigned size() const {
      return m;
   }

   unsigned get_state() const {
      return curr_state;
   }

   inline void accept(char c)
   {
      // TODO: Maybe optimize this when `curr_state` is anyway always 0.
      // TODO: Like we should optimize for the last if.
      while ((curr_state > 0) && (P[curr_state] != c)) {
         curr_state = pi[curr_state - 1];
      }

      if (P[curr_state] == c)
         ++curr_state;
   }

   inline void zerokmp_accept(char c)
   {
      // Does it already match? That is, the while-loop is invalid.
      if (P[curr_state] == c) {
         ++curr_state;
      }
      else {
         // Then, we can set the state to 0.
         curr_state = 0;

         // And make another check.
         if (P[curr_state] == c)
            ++curr_state;
      }
   }

   inline void branchless_zerokmp_accept(char c)
   {
      unsigned match1 = (P[curr_state] == c);
      unsigned mask1 = -match1;

      curr_state = curr_state & mask1;

      unsigned match2 = (P[curr_state] == c);
      curr_state += match2;
   }

   inline void accept_symbol(size_t code, unsigned* stop_pos = nullptr)
   {
      for (unsigned index = 0, limit = fsst_symbols[code].size(); index != limit; ++index) {
         auto c = fsst_symbols[code][index];

         // Accept.
         accept(c);

         // Reached the final state?
         if (curr_state == m) {
            // If we have a `stop_pos`, store it.
            if (!!stop_pos)
               (*stop_pos) = index + 1;
            return;
         }
      }
   }

   inline void zerokmp_accept_symbol(size_t code)
   {
      for (auto c : fsst_symbols[code]) {
         zerokmp_accept(c);

         if (curr_state == m)
            return;
      }
   }

   inline unsigned accept_symbol_with_lookup(size_t code)
   {
      // First, save `table_index`, since `curr_state` will change.
      auto table_index = curr_state * fsst_size + code;

      // Use the state lookup table.
      curr_state = lookup_table[table_index].first;

      // Return the stop position. This is helpful for the meta-machine.
      return lookup_table[table_index].second;
   }

   // Another implementation of KMP on uncompressed data.
   bool raw_kmp_match(const std::string& text) { return raw_kmp_match(text.data(), text.size()); }

   // Implementation of KMP on uncompressed data.
   bool raw_kmp_match(const char* ptr, unsigned len)
   {
      // Init.
      init_state();

      // Iterate.
      for (unsigned index = 0, limit = len; index != limit; ++index) {
         accept(ptr[index]);

         // TODO: Here, we can have two paths.
         // TODO: If `curr_state` is very far from the end, don't have this `if` here.
         // TODO: Otherwise, have it.
         // TODO: But maybe only when necessary (like traverse until some point and then switch).
         if (curr_state == m)
            return true;
      }
      return false;
   }

   // Implementation of ZeroKMP on uncompressed data.
   bool raw_zerokmp_match(const char* ptr, unsigned len)
   {
      // Init.
      init_state();

      // Iterate.
      for (unsigned index = 0, limit = len; index != limit; ++index) {
         zerokmp_accept(ptr[index]);

         // TODO: Here, we can have two paths.
         // TODO: If `curr_state` is very far from the end, don't have this `if` here.
         // TODO: Otherwise, have it.
         // TODO: But maybe only when necessary (like traverse until some point and then switch).
         if (curr_state == m)
            return true;
      }
      return false;
   }

   // Implementation of KMP on FSST-encoded data.
   bool fsst_kmp_match(const FsstDecoder& fsstDecoder, size_t lenIn, const unsigned char* strIn, size_t size)
   {
      // Init.
      init_state();

      auto consume_char = [this](unsigned char c) {
         accept(c);
         return curr_state != m;
      };

      auto consume_code = [this](size_t code) {
         accept_symbol(code);
         return curr_state != m;
      };

      return fsstDecoder.Iterate(lenIn, strIn, size, consume_char, consume_code);
   }

   // Implementation of ZeroKMP on FSST-encoded data.
   bool fsst_zerokmp_match(const FsstDecoder& fsstDecoder, size_t lenIn, const unsigned char* strIn, size_t size)
   {
      // Init.
      init_state();

      auto consume_char = [this](unsigned char c) {
         zerokmp_accept(c);
         return curr_state != m;
      };

      auto consume_code = [this](size_t code) {
         // TODO: Remove this assert at the end.
         assert(code < fsst_size);
         zerokmp_accept_symbol(code);
         return curr_state != m;
      };

      return fsstDecoder.Iterate(lenIn, strIn, size, consume_char, consume_code);
   }

   // Implementation of LookupKMP on FSST-encoded data.
   bool fsst_lookup_kmp_match(const FsstDecoder& fsstDecoder, size_t lenIn, const unsigned char* strIn, size_t size /*, bool verbose = false */)
   {
      // Init.
      init_state();

      auto consume_char = [this](unsigned char c) {
         accept(c);
         return curr_state != m;
      };

      auto consume_code = [this](size_t code) {
         accept_symbol_with_lookup(code);
         return curr_state != m;
      };

      return fsstDecoder.Iterate(lenIn, strIn, size, consume_char, consume_code);
   }

   // Implementation of LookupZeroKMP on FSST-encoded data.
   bool fsst_lookup_zerokmp_match(const FsstDecoder& fsstDecoder, size_t lenIn, const unsigned char* strIn, size_t size)
   {
      // Init.
      init_state();

      auto consume_char = [this](unsigned char c) {
         zerokmp_accept(c);
         return curr_state != m;
      };

      auto consume_code = [this](size_t code) {
         accept_symbol_with_lookup(code);
         return curr_state != m;
      };

      return fsstDecoder.Iterate(lenIn, strIn, size, consume_char, consume_code);
   }

   // Implementation of a branchless LookupZeroKMP on FSST-encoded data.
   bool fsst_lookup_branchless_zerokmp_match(const FsstDecoder& fsstDecoder, size_t lenIn, const unsigned char* strIn, size_t size)
   {
      // Init.
      init_state();

      auto consume_char = [this](unsigned char c) {
         branchless_zerokmp_accept(c);
         return curr_state != m;
      };

      auto consume_code = [this](size_t code) {
         // TODO: Remove this assert at the end.
         assert(code < fsst_size);
         accept_symbol_with_lookup(code);
         return curr_state != m;
      };

      return fsstDecoder.Iterate(lenIn, strIn, size, consume_char, consume_code);
   }

private:
   std::string P;
   unsigned m;
   unsigned curr_state;
   std::vector<unsigned> pi;
   unsigned fsst_size;
   std::vector<std::string> fsst_symbols;
   std::vector<std::pair<unsigned, unsigned>> lookup_table;

   friend class MetaStateMachine;

   inline void init_state(unsigned pos = 0)
   {
      curr_state = pos;
   }

   void build_pi()
   {
      pi.assign(P.size(), 0);

      pi[0] = 0;

      unsigned k = 0;
      for (unsigned q = 1, limit = P.size(); q != limit; ++q) {
         while ((k > 0) && (P[k] != P[q])) {
            k = pi[k - 1];
         }

         // Match? Then advance.
         if (P[k] == P[q])
            ++k;

         // Store the state.
         pi[q] = k;
      }

      // std::cerr << "pi:" << std::endl;
      // for (unsigned index = 0; index != m; ++index) {
      //   std::cerr << pi[index] << " ";
      // }
      // std::cerr << std::endl;
   }
};

using StateMachine = StateMachineImpl<0>;
using StateMachine2 = StateMachineImpl<1>;
using StateMachineView = StateMachineImpl<2>;
